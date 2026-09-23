"""The three things the request path talks to: serving endpoints, Lakebase, a warehouse.

Serving goes over one shared HTTP/2 client straight to /invocations. The SDK's
serving_endpoints.query is synchronous and re-resolves auth per call; in a service whose
whole budget is ~100 ms, a pooled keep-alive connection is the difference.

Lakebase is an async pool. The logic mirrors src/crfs/online.py (and the Streamlit app's
old lib/lakebase.py): OAuth token from /api/2.0/postgres/credentials as the password,
the DIRECT host rather than the pooler -- verified 2026-09-07, the pooled host rejects
OAuth with "SASL authentication failed" -- and identifiers validated before they are
spliced, because psycopg cannot parameterise them.

The warehouse is off the request path entirely: only snapshot.py and ops.py use it.
"""
import asyncio
import re
import time
from decimal import Decimal

import httpx
import psycopg
from psycopg_pool import AsyncConnectionPool
from databricks.sdk import WorkspaceClient

from . import settings as S

w = WorkspaceClient()
HOST = w.config.host.rstrip("/")

_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class EndpointError(Exception):
    def __init__(self, status: int, detail: str):
        super().__init__(f"HTTP {status}: {detail[:200]}")
        self.status = status


# ------------------------------------------------------------------------ auth
_auth = {"headers": None, "at": 0.0}


async def auth_headers() -> dict:
    # authenticate() can block on a token refresh, so it runs off the event loop and
    # its result is reused for 10 minutes -- the OAuth token itself lives an hour.
    if _auth["headers"] is None or time.time() - _auth["at"] > 600:
        _auth["headers"] = await asyncio.to_thread(w.config.authenticate)
        _auth["at"] = time.time()
    return _auth["headers"]


# --------------------------------------------------------------------- serving
http = httpx.AsyncClient(
    http2=True,
    timeout=httpx.Timeout(30.0, connect=5.0),
    # Measured 2026-09-23: after ~100 s idle the serving front end has closed the pooled
    # connection, and the next call fails in 3 ms with "Server disconnected" -- which the
    # breaker would count as an endpoint failure. Expire idle connections first.
    limits=httpx.Limits(max_connections=64, max_keepalive_connections=32,
                        keepalive_expiry=30),
)


async def invoke(endpoint: str, payload: dict, served_model: str = None) -> dict:
    path = (f"/serving-endpoints/{endpoint}/served-models/{served_model}/invocations"
            if served_model else f"/serving-endpoints/{endpoint}/invocations")
    try:
        r = await http.post(HOST + path, json=payload, headers=await auth_headers())
    except (httpx.RemoteProtocolError, httpx.ConnectError, httpx.ReadError):
        # A connection the server dropped between requests, not a slow endpoint: one
        # retry on a fresh connection, still inside the caller's budget.
        r = await http.post(HOST + path, json=payload, headers=await auth_headers())
    if r.status_code != 200:
        raise EndpointError(r.status_code, r.text)
    return r.json()


async def stream_chat(endpoint: str, messages: list, max_tokens: int = 400):
    """Yield text deltas from an OpenAI-compatible chat endpoint."""
    import json
    body = {"messages": messages, "max_tokens": max_tokens, "stream": True}
    async with http.stream("POST", f"{HOST}/serving-endpoints/{endpoint}/invocations",
                           json=body, headers=await auth_headers()) as r:
        if r.status_code != 200:
            raise EndpointError(r.status_code, (await r.aread()).decode(errors="replace"))
        async for line in r.aiter_lines():
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if data == "[DONE]":
                return
            try:
                delta = json.loads(data)["choices"][0].get("delta") or {}
            except (ValueError, KeyError, IndexError):
                continue
            if delta.get("content"):
                yield delta["content"]


# -------------------------------------------------------------------- lakebase
_pg = {"token": None, "at": 0.0, "host": None, "user": None}


async def _pg_credentials():
    if _pg["token"] is None or time.time() - _pg["at"] > 50 * 60:
        def mint():
            ep = w.api_client.do("GET", f"/api/2.0/postgres/{S.ENDPOINT_PATH}") or {}
            host = ((ep.get("status") or {}).get("hosts") or {}).get("host")
            tok = w.api_client.do("POST", "/api/2.0/postgres/credentials",
                                  body={"endpoint": S.ENDPOINT_PATH})["token"]
            return host, tok, w.current_user.me().user_name
        _pg["host"], _pg["token"], _pg["user"] = await asyncio.to_thread(mint)
        _pg["at"] = time.time()
    return _pg


class _TokenConnection(psycopg.AsyncConnection):
    """Every NEW connection gets a fresh token; an open one outlives its token fine,
    because Postgres only authenticates at connect time."""

    @classmethod
    async def connect(cls, conninfo: str = "", **kwargs):
        c = await _pg_credentials()
        kwargs.update(host=c["host"], user=c["user"], password=c["token"])
        return await super().connect(conninfo, **kwargs)


pool = AsyncConnectionPool(
    conninfo=f"port=5432 dbname={S.CATALOG} sslmode=require connect_timeout=10",
    connection_class=_TokenConnection,
    kwargs={"autocommit": True},
    min_size=2, max_size=10,
    max_lifetime=40 * 60,
    open=False,
)


async def keyed_read(table: str, keys: dict, cols: str = "*"):
    """One keyed lookup, the shape an application issues.
    Returns {row, sql, ms}; row is None when the key is absent."""
    for name in [table, *keys, *([] if cols == "*" else [c.strip() for c in cols.split(",")])]:
        if not _IDENT.fullmatch(name):
            raise ValueError(f"not a valid identifier: {name!r}")
    where = " AND ".join(f"{k} = %s" for k in keys)
    sql = f'SELECT {cols} FROM "{S.SCHEMA}"."{table}" WHERE {where}'
    t0 = time.perf_counter()
    async with pool.connection() as conn:
        cur = await conn.execute(sql, tuple(keys.values()))
        row = await cur.fetchone()
        names = [d.name for d in cur.description]
    ms = (time.perf_counter() - t0) * 1000.0
    shown = sql.replace("%s", "'{}'").format(*keys.values())
    # NUMERIC columns arrive as Decimal, which orjson refuses to serialise.
    row = {k: float(v) if isinstance(v, Decimal) else v for k, v in zip(names, row)} if row else None
    return {"row": row, "sql": shown, "ms": round(ms, 2)}


# ------------------------------------------------------------------- warehouse
def sql_rows(statement: str) -> list[dict]:
    """Run a statement to completion and return rows as dicts, or raise.

    Polls while RUNNING and raises on anything but SUCCEEDED: an empty result used to
    stand in for PERMISSION_DENIED and read as "run the pipeline" (verification_log).
    States are compared by `.value` -- str(StatementState.SUCCEEDED) is the repr.
    """
    def state(r):
        raw = getattr(getattr(r, "status", None), "state", None)
        return str(getattr(raw, "value", raw) or "")

    res = w.statement_execution.execute_statement(
        statement=statement, warehouse_id=S.WAREHOUSE_ID, wait_timeout="30s")
    deadline = time.time() + 120
    while state(res) in ("PENDING", "RUNNING") and time.time() < deadline:
        time.sleep(1)
        res = w.statement_execution.get_statement(res.statement_id)
    if state(res) != "SUCCEEDED":
        err = getattr(getattr(res, "status", None), "error", None)
        raise RuntimeError(f"{state(res) or 'NO_STATE'}: {getattr(err, 'message', '')}")
    cols = [c.name for c in res.manifest.schema.columns] if res.manifest else []
    rows = (res.result.data_array if res.result else None) or []
    return [dict(zip(cols, r)) for r in rows]
