"""Crunchyroll homepage service: two rankers, one feature store.

    VERTICAL   which rails, in what order  (crunchyroll-rail-ranker)
    HORIZONTAL which titles, in what order (crunchyroll-watch-next-ranker)

FastAPI serves the JSON API and the built React app from one process, so the browser
makes one round trip per homepage and the service fans out behind it.
"""
import asyncio
import json
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, ORJSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import settings as S
from .clients import http, keyed_read, pool, stream_chat, w
from .fallback import breakers, simulate
from .homepage import Clock, contexts, forget_retrieval, homepage, rank_titles, request_context
from .ops import ops
from .snapshot import snapshot

DIST = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend", "dist")


@asynccontextmanager
async def lifespan(_app):
    await pool.open(wait=False)
    refresher = asyncio.create_task(snapshot.run())
    warm = asyncio.create_task(_warm())
    yield
    refresher.cancel()
    warm.cancel()
    await pool.close()
    await http.aclose()


async def _warm():
    """One throwaway homepage at startup: TLS, HTTP/2, the OAuth token and two Postgres
    connections are all paid here instead of by the first visitor."""
    try:
        await asyncio.wait_for(snapshot.ready.wait(), 180)
        await homepage("v0001", "post_play", request_context("tv", "en-US", 21, True))
    except Exception:
        pass


app = FastAPI(title="Crunchyroll homepage service", lifespan=lifespan,
              default_response_class=ORJSONResponse)
app.add_middleware(GZipMiddleware, minimum_size=1024)


class Ctx(BaseModel):
    viewer_id: str = Field("v0001", pattern=r"^[A-Za-z0-9_]{1,32}$")
    surface: str = "post_play"
    device: str = "tv"
    locale: str = "en-US"
    hour: int | None = Field(21, ge=0, le=23)
    frozen: bool = True


async def _ready():
    try:
        await asyncio.wait_for(snapshot.ready.wait(), 60)
    except asyncio.TimeoutError:
        raise HTTPException(503, f"reference snapshot not loaded yet: {snapshot.error or 'loading'}")


def _server_timing(body: dict) -> str:
    parts = [f'{s["stage"]};dur={s["ms"]}' for s in body.get("timings", [])]
    return ", ".join(parts + [f'total;dur={body.get("total_ms", 0)}'])


# ----------------------------------------------------------------------- api
@app.get("/api/config")
async def config():
    return {"catalog": S.CATALOG, "schema": S.SCHEMA,
            "endpoints": {"rail_ranker": S.RAIL_RANKER_ENDPOINT, "ranker": S.RANKER_ENDPOINT,
                          "retriever": S.RETRIEVER_ENDPOINT, "llm": S.LLM_ENDPOINT},
            "budgets_ms": {"rails": S.RAIL_TIMEOUT_MS, "titles": S.RANKER_TIMEOUT_MS,
                           "retriever": S.RETRIEVER_TIMEOUT_MS},
            "snapshot": {"ready": snapshot.ready.is_set(), "error": snapshot.error,
                         "age_s": round(time.time() - snapshot.loaded_at, 1) if snapshot.loaded_at else None,
                         "viewers": sorted(snapshot.entitled)[:400]},
            "burst_enabled": bool(S.BURST_JOB_ID)}


@app.post("/api/homepage")
async def api_homepage(c: Ctx):
    await _ready()
    body = await homepage(c.viewer_id, c.surface, request_context(c.device, c.locale, c.hour, c.frozen))
    body["breakers"] = {k: b.snapshot() for k, b in breakers.items()}
    body["simulate"] = simulate["mode"]
    return ORJSONResponse(body, headers={"Server-Timing": _server_timing(body)})


@app.post("/api/contexts")
async def api_contexts(c: Ctx):
    await _ready()
    return await contexts(c.viewer_id, c.locale)


class Simulate(BaseModel):
    mode: str = Field(pattern=r"^(off|timeout|open)$")


@app.post("/api/fallback/simulate")
async def api_simulate(s: Simulate):
    simulate["mode"] = s.mode
    if s.mode == "off":
        for b in breakers.values():
            b.ok()
    return {"mode": simulate["mode"], "breakers": {k: b.snapshot() for k, b in breakers.items()}}


@app.get("/api/ops")
async def api_ops():
    return await ops()


# ------------------------------------------------------------------- explain
class Explain(Ctx):
    title_id: str = Field(pattern=r"^[A-Za-z0-9_]{1,32}$")
    score: float | None = None
    rank: int | None = None


SYSTEM = (
    "You explain one recommendation on a streaming homepage to a ranking engineer. "
    "Use ONLY the feature values provided; they are the rows the model's feature store "
    "served for this request. Name the 2-3 features that most plausibly drove the score, "
    "with their values. If the data cannot support a claim, say so. No preamble, "
    "at most 90 words, plain sentences.")


@app.post("/api/explain")
async def api_explain(e: Explain):
    ctx = request_context(e.device, e.locale, e.hour, e.frozen)
    reads = await asyncio.gather(
        keyed_read("online_viewer_features", {"viewer_id": e.viewer_id}),
        keyed_read("online_recent_behavior", {"viewer_id": e.viewer_id}),
        keyed_read("online_title_features", {"title_id": e.title_id}),
        return_exceptions=True)
    rows = {name: (r["row"] if isinstance(r, dict) else None)
            for name, r in zip(("viewer", "recent_behavior", "title"), reads)}
    title = snapshot.titles.get(e.title_id, {})
    facts = {"title": {"id": e.title_id, "name": title.get("title_name"),
                       "genre": title.get("primary_genre")},
             "request": {**ctx, "surface": e.surface},
             "model_score_play_start_probability": e.score, "rank_in_row": e.rank, **rows}
    messages = [{"role": "system", "content": SYSTEM},
                {"role": "user", "content": json.dumps(facts, default=str)}]

    async def gen():
        t0 = time.perf_counter()
        first = None
        try:
            async for chunk in stream_chat(S.LLM_ENDPOINT, messages):
                if first is None:
                    first = (time.perf_counter() - t0) * 1000
                    yield _sse("meta", {"first_token_ms": round(first)})
                yield _sse("token", {"text": chunk})
        except Exception as ex:
            yield _sse("error", {"message": f"{type(ex).__name__}: {str(ex)[:200]}"})
        yield _sse("done", {"total_ms": round((time.perf_counter() - t0) * 1000),
                            "endpoint": S.LLM_ENDPOINT})

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


# --------------------------------------------------------------------- burst
class Burst(Ctx):
    n_events: int = Field(3, ge=1, le=20)


@app.post("/api/burst")
async def api_burst(b: Burst):
    """Fire the burst job, then stream the Lakebase row until it moves, then re-rank.

    The number is measured, not asserted: polled every 250 ms against Postgres. This is
    the TRIGGERED path (append events, recompute, refresh sync), so expect minutes;
    `make streaming` is the seconds-scale CONTINUOUS one.
    """
    if not S.BURST_JOB_ID:
        raise HTTPException(400, "BURST_JOB_ID is not set; redeploy with `make deploy`.")
    watch = "minutes_watched_24h"

    async def gen():
        before = (await keyed_read("online_recent_behavior", {"viewer_id": b.viewer_id}))["row"] or {}
        before_val = before.get(watch)
        yield _sse("before", {"column": watch, "value": before_val})
        run = await asyncio.to_thread(lambda: w.jobs.run_now(
            job_id=int(S.BURST_JOB_ID),
            notebook_params={"viewer_id": b.viewer_id, "mode": "burst", "n_events": str(b.n_events)}))
        yield _sse("run", {"run_id": run.run_id})
        t0, next_tick, after_val = time.time(), 0.0, before_val
        while time.time() - t0 < S.BURST_WAIT_S:
            await asyncio.sleep(0.25)
            try:
                row = (await keyed_read("online_recent_behavior", {"viewer_id": b.viewer_id}))["row"] or {}
                after_val = row.get(watch)
            except Exception:
                continue
            elapsed = round(time.time() - t0, 2)
            if after_val is not None and (before_val is None or float(after_val) != float(before_val)):
                yield _sse("changed", {"after_s": elapsed, "before": before_val, "after": after_val})
                break
            if elapsed >= next_tick:  # every 5 s: elapsed plus the job's own state
                next_tick = elapsed + 5
                st = await asyncio.to_thread(lambda: w.jobs.get_run(run.run_id).state)
                yield _sse("tick", {"elapsed_s": elapsed, "job_state": str(
                    getattr(st.life_cycle_state, "value", st.life_cycle_state))})
        else:
            yield _sse("timeout", {"elapsed_s": round(time.time() - t0, 1)})
        await snapshot.refresh_viewer(b.viewer_id)
        forget_retrieval(b.viewer_id)
        ctx = request_context(b.device, b.locale, b.hour, b.frozen)
        titles = await rank_titles(b.viewer_id, b.surface, ctx, Clock())
        yield _sse("reranked", titles)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# -------------------------------------------------------------------- static
if os.path.isdir(os.path.join(DIST, "assets")):
    class _Immutable(StaticFiles):
        async def get_response(self, path, scope):
            r = await super().get_response(path, scope)
            r.headers["Cache-Control"] = "public, max-age=31536000, immutable"
            return r

    app.mount("/assets", _Immutable(directory=os.path.join(DIST, "assets")), name="assets")


@app.get("/{path:path}", include_in_schema=False)
async def spa(path: str, request: Request):
    index = os.path.join(DIST, "index.html")
    if path.startswith("api/") or not os.path.exists(index):
        raise HTTPException(404, "frontend not built: run `npm run build` in app/")
    return FileResponse(index, headers={"Cache-Control": "no-cache"})
