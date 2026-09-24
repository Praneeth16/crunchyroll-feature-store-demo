"""One homepage request: both rankers and the raw online rows, fanned out in parallel.

    t=0 ─┬─ rail ranker (vertical) ──────────────► online_viewer_rail row for the top rail
         ├─ retriever ─► entitled/unseen filter ─► watch-next ranker (horizontal)
         └─ online_viewer_features ∥ online_recent_behavior

The endpoints do their own feature lookups against the same Lakebase store; the direct
reads here exist to put the rows on screen, not to feed the models.
"""
import asyncio
import datetime as dt
import json
import time

from . import settings as S
from .clients import invoke, keyed_read
from .fallback import fallback_rails, guarded, last_good
from .snapshot import snapshot

RETRIEVE_K = 60
RANK_K = 25


def request_context(device: str, locale: str, hour: int | None, frozen: bool) -> dict:
    # UTC, so the frozen clock is the same instant on a laptop and in the app container.
    if frozen:
        at = dt.datetime(2026, 8, 31, hour if hour is not None else 21, tzinfo=dt.timezone.utc)
    else:
        at = dt.datetime.now(dt.timezone.utc)
    return {"device": device, "locale": locale, "hour_of_day": at.hour,
            "day_of_week": at.weekday(), "request_epoch_s": int(at.timestamp())}


class Clock:
    def __init__(self):
        self.t0 = time.perf_counter()
        self.stages = []

    def now(self):
        return (time.perf_counter() - self.t0) * 1000.0

    async def time(self, stage: str, coro):
        start = self.now()
        try:
            return await coro
        finally:
            self.stages.append({"stage": stage, "start_ms": round(start, 1),
                                "ms": round(self.now() - start, 1)})


# ----------------------------------------------------------------- vertical
async def rank_rails(viewer: str, eligible: list[dict], ctx: dict, remember: bool = True):
    records = [{"viewer_id": viewer, "rail_id": r["rail_id"], "device": ctx["device"],
                "locale": ctx["locale"], "hour_of_day": ctx["hour_of_day"],
                "day_of_week": ctx["day_of_week"], "request_epoch_s": ctx["request_epoch_s"]}
               for r in eligible]
    by_id = {r["rail_id"]: r for r in eligible}
    resp, call = await guarded("rails", S.RAIL_TIMEOUT_MS, lambda: invoke(
        S.RAIL_RANKER_ENDPOINT, {"dataframe_records": records}))

    if resp is not None:
        preds = sorted(resp.get("predictions") or [], key=lambda p: p["rail_rank"])
        order = [p["rail_id"] for p in preds]
        scores = {p["rail_id"]: float(p["engagement_probability"]) for p in preds}
        source, age = "model", None
        if remember:
            last_good.put(viewer, order)
    else:
        order, source, age = fallback_rails(viewer, eligible)
        scores = {}

    # editorial_rank is catalog-wide 1..16; the model's rank is dense over the ELIGIBLE
    # set. Dense-rank the incumbent within the same set, or every rail gets a free
    # "gain" for each ineligible rail above it and a model that reproduced the old
    # homepage exactly would still show rails moving up.
    incumbent = {r["rail_id"]: i + 1 for i, r in
                 enumerate(sorted(eligible, key=lambda r: r["editorial_rank"]))}
    items = [{**by_id[rid], "rank": i + 1, "score": scores.get(rid),
              "incumbent_rank": incumbent[rid], "moved": incumbent[rid] - (i + 1)}
             for i, rid in enumerate(order) if rid in by_id]
    return {"items": items, "source": source, "cached_age_s": age, "call": call,
            "request_example": records[0] if records else None}


# --------------------------------------------------------------- horizontal
# Retrieval depends only on the viewer's embedding, which is recomputed by the pipeline,
# not by the request -- so a context change (device, hour) never changes it. Caching it
# takes the retriever off the serial retrieve -> rank path for every repeat view
# (measured in region: retriever p50 61 ms of a 152 ms homepage).
RETRIEVAL_TTL_S = 300
_retrieval_cache: dict = {}


def forget_retrieval(viewer: str):
    _retrieval_cache.pop(viewer, None)


async def _retrieve(viewer: str):
    hit = _retrieval_cache.get(viewer)
    if hit and time.time() - hit[0] < RETRIEVAL_TTL_S:
        return hit[1], {"status": "ok", "ms": 0.0, "cached": True}
    resp, call = await guarded("retriever", S.RETRIEVER_TIMEOUT_MS, lambda: invoke(
        S.RETRIEVER_ENDPOINT, {"dataframe_records": [{"viewer_id": viewer, "top_k": RETRIEVE_K}]}))
    if resp is not None:
        _retrieval_cache[viewer] = (time.time(), resp)
        if len(_retrieval_cache) > 10_000:
            _retrieval_cache.pop(next(iter(_retrieval_cache)))
    return resp, call


async def rank_titles(viewer: str, surface: str, ctx: dict, clock: Clock):
    retrieved, rcall = await clock.time("retriever", _retrieve(viewer))
    if retrieved is not None:
        cands = json.loads(retrieved["predictions"][0]["candidates"])
        retrieval = {c["title_id"]: float(c["retrieval_score"]) for c in cands}
        rsource = "model"
    else:
        retrieval = {t: None for t in snapshot.popularity[:RETRIEVE_K]}
        rsource = "popularity"

    eligible = snapshot.entitled_unseen(viewer, list(retrieval))
    records = [{"viewer_id": viewer, "title_id": t, "surface": surface,
                "device": ctx["device"], "locale": ctx["locale"],
                "hour_of_day": ctx["hour_of_day"], "request_epoch_s": ctx["request_epoch_s"]}
               for t in eligible]

    scores, tcall = {}, {"status": "skipped", "ms": 0.0}
    if records:
        resp, tcall = await clock.time("watch_next_ranker", guarded(
            "titles", S.RANKER_TIMEOUT_MS,
            lambda: invoke(S.RANKER_ENDPOINT, {"dataframe_records": records})))
        if resp is not None:
            scores = dict(zip(eligible, (float(p) for p in resp["predictions"])))

    order = sorted(eligible, key=lambda t: -scores[t]) if scores else eligible
    source = "model" if scores else ("retrieval" if rsource == "model" else "popularity")
    items = [{**_title(t), "score": scores.get(t), "retrieval_score": retrieval.get(t)}
             for t in order[:RANK_K]]
    return {"items": items, "source": source, "retrieval_source": rsource,
            "calls": {"retriever": rcall, "ranker": tcall},
            "funnel": {"catalog": len(snapshot.titles), "retrieved": len(retrieval),
                       "entitled_unseen": len(eligible), "ranked": len(items)}}


def _title(t: str) -> dict:
    x = snapshot.titles.get(t, {})
    return {"title_id": t, "title_name": x.get("title_name", t),
            "genre": x.get("primary_genre"), "maturity": x.get("maturity_rating"),
            "is_simulcast": x.get("is_simulcast"), "release_year": x.get("release_year")}


async def _read(table: str, keys: dict):
    try:
        return await keyed_read(table, keys)
    except Exception as e:
        return {"row": None, "error": f"{type(e).__name__}: {str(e)[:160]}"}


# ------------------------------------------------------------------ homepage
async def homepage(viewer: str, surface: str, ctx: dict) -> dict:
    clock = Clock()
    eligible = snapshot.eligible_rails(viewer)

    async def vertical():
        rails = await clock.time("rail_ranker", rank_rails(viewer, eligible, ctx))
        top = rails["items"][0]["rail_id"] if rails["items"] else None
        row = await clock.time("lakebase_viewer_rail", _read(
            "online_viewer_rail", {"viewer_id": viewer, "rail_id": top})) if top else None
        return rails, row

    (rails, vr_row), titles, vf_row, rb_row = await asyncio.gather(
        vertical(), rank_titles(viewer, surface, ctx, clock),
        clock.time("lakebase_viewer", _read("online_viewer_features", {"viewer_id": viewer})),
        clock.time("lakebase_recent", _read("online_recent_behavior", {"viewer_id": viewer})))

    return {
        "viewer_id": viewer, "known_viewer": snapshot.known(viewer),
        "context": {**ctx, "surface": surface},
        "rails": {**rails, "catalog": len(snapshot.rails), "eligible": len(eligible)},
        "titles": titles,
        "online": {"viewer_features": vf_row, "recent_behavior": rb_row, "viewer_rail": vr_row},
        "timings": sorted(clock.stages, key=lambda s: s["start_ms"]),
        "total_ms": round(clock.now(), 1),
    }


CONTEXTS = [("21:00 TV", "tv", 21), ("09:00 TV", "tv", 9),
            ("21:00 mobile", "mobile", 21), ("09:00 mobile", "mobile", 9)]


async def contexts(viewer: str, locale: str) -> dict:
    """Same viewer, same feature store, four requests. Only the request changes."""
    eligible = snapshot.eligible_rails(viewer)
    results = await asyncio.gather(*[
        rank_rails(viewer, eligible, request_context(dev, locale, hr, True), remember=False)
        for _, dev, hr in CONTEXTS])
    cols = []
    for (label, _, _), r in zip(CONTEXTS, results):
        cols.append({"label": label, "source": r["source"], "ms": r["call"]["ms"],
                     "ranks": {i["rail_id"]: i["rank"] for i in r["items"]}})
    ref = next((c for c in cols if c["source"] == "model"), None)
    for c in cols:
        c["moved_vs_ref"] = (None if ref is None or c["source"] != "model" else
                             sum(1 for k, v in c["ranks"].items() if ref["ranks"].get(k) != v))
    names = {r["rail_id"]: r["rail_name"] for r in eligible}
    order = sorted(names, key=lambda k: (ref or cols[0])["ranks"].get(k, 99))
    return {"reference": ref["label"] if ref else None, "columns": cols,
            "rails": [{"rail_id": k, "rail_name": names[k]} for k in order]}
