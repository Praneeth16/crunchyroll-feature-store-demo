"""Direct Postgres access to Lakebase-backed Online Feature Store.

Duplication note: This logic mirrors src/crfs/online.py. The app cannot import src/
because apps don't share the notebook path. If online.py changes, sync those changes here.

Why this exists: the serving endpoint's own feature lookup is the fast path we
care about, but to *prove* freshness and keyed-read latency we need to read the
online store the way an application would. spark.sql() against FOREIGN tables
measures serverless SQL planning plus federated read (~1s), not the serving path.

Auth is OAuth: POST /api/2.0/postgres/credentials mints a short-lived token used
as the Postgres password. Tokens last about an hour, so the connection is cached
with a 50-minute expiry and reopened on OperationalError.

Connect to the *direct* endpoint host, not the pooler. Verified 2026-09-07: the
read_write_pooled_host rejects OAuth credentials with "SASL authentication
failed", while the same token authenticates fine against status.hosts.host.
"""
import time
import datetime as dt
import streamlit as st

CRED_API = "/api/2.0/postgres/credentials"
TOKEN_TTL_S = 50 * 60


def mint_token(w, endpoint_path: str) -> str:
    resp = w.api_client.do("POST", CRED_API, body={"endpoint": endpoint_path})
    return resp["token"]


def endpoint_hosts(w, endpoint_path: str) -> dict:
    st_resp = (w.api_client.do("GET", f"/api/2.0/postgres/{endpoint_path}") or {}).get("status") or {}
    hosts = st_resp.get("hosts") or {}
    return {"host": hosts.get("host"), "pooled": hosts.get("read_write_pooled_host")}


class OnlineStore:
    """Thin psycopg wrapper with token refresh and timed reads.

    pg_database defaults to the UC catalog name and pg_schema to the UC schema,
    which is how the feature store lays published tables out in Postgres.
    """

    def __init__(self, w, endpoint_path: str, pg_database: str, pg_schema: str,
                 user: str = None, pooled: bool = False, connect_timeout: int = 10):
        self.w = w
        self.endpoint_path = endpoint_path
        self.pg_database = pg_database
        self.pg_schema = pg_schema
        self.pooled = pooled
        self.connect_timeout = connect_timeout
        self.user = user or w.current_user.me().user_name
        self._conn = None
        self._token_at = 0.0

    def _dsn(self) -> str:
        hosts = endpoint_hosts(self.w, self.endpoint_path)
        host = hosts["pooled"] if self.pooled and hosts.get("pooled") else hosts["host"]
        token = mint_token(self.w, self.endpoint_path)
        self._token_at = time.time()
        return (f"host={host} port=5432 dbname={self.pg_database} user={self.user} "
                f"password={token} sslmode=require connect_timeout={self.connect_timeout}")

    def conn(self):
        import psycopg
        stale = self._conn is None or (time.time() - self._token_at) > TOKEN_TTL_S
        if not stale:
            try:
                if self._conn.closed:
                    stale = True
            except Exception:
                stale = True
        if stale:
            self.close()
            self._conn = psycopg.connect(self._dsn(), autocommit=True)
        return self._conn

    def close(self):
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def query(self, sql: str, params=None, retry: bool = True):
        """Run SQL, returning (rows, columns). Reconnects once on a dropped
        connection -- the endpoint can recycle, and a stale token looks the same."""
        import psycopg
        try:
            with self.conn().cursor() as cur:
                cur.execute(sql, params)
                # DDL/DML produce no result set; fetchall() would raise.
                if cur.description is None:
                    return [], []
                return cur.fetchall(), [d.name for d in cur.description]
        except psycopg.OperationalError:
            if not retry:
                raise
            self.close()
            return self.query(sql, params, retry=False)

    def keyed_read(self, table: str, key_col: str, key_val, cols: str = "*"):
        """One keyed lookup, the shape an application actually issues.
        Returns (row_or_None, columns, elapsed_ms)."""
        sql = f'SELECT {cols} FROM "{self.pg_schema}"."{table}" WHERE {key_col} = %s'
        t0 = time.perf_counter()
        rows, columns = self.query(sql, (key_val,))
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        return (rows[0] if rows else None), columns, elapsed_ms

    def keyed_read_composite(self, table: str, keys: dict, cols: str = "*"):
        """A multi-column keyed lookup -- the shape viewer x rail features need.

        Column names are interpolated because psycopg cannot parameterise
        identifiers, so they are validated against a strict identifier pattern
        first: these come from application code, but a lookup helper that will
        splice anything into a WHERE clause is a bad helper.
        Returns (row_or_None, columns, elapsed_ms).
        """
        import re
        ident = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
        for k in keys:
            if not ident.fullmatch(k):
                raise ValueError(f"not a valid column name: {k!r}")
        # Mirrors src/crfs/online.py. `table` and `cols` were spliced unvalidated while
        # the docstring above claimed they were checked; kept identical in both copies
        # so the app and the pipeline cannot drift on a security-relevant guard.
        if not ident.fullmatch(table):
            raise ValueError(f"not a valid table name: {table!r}")
        if cols != "*":
            for c in cols.split(","):
                if not ident.fullmatch(c.strip()):
                    raise ValueError(f"not a valid column list: {cols!r}")
        where = " AND ".join(f"{k} = %s" for k in keys)
        sql = f'SELECT {cols} FROM "{self.pg_schema}"."{table}" WHERE {where}'
        t0 = time.perf_counter()
        rows, columns = self.query(sql, tuple(keys.values()))
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        return (rows[0] if rows else None), columns, elapsed_ms

    def keyed_read_latency(self, table: str, key_col: str, key_vals, warmup: int = 2):
        """p50/p95 over a list of keys. Warmup reads are excluded."""
        for k in list(key_vals)[:warmup]:
            try:
                self.keyed_read(table, key_col, k)
            except Exception:
                pass
        lat = []
        for k in key_vals:
            try:
                _, _, ms = self.keyed_read(table, key_col, k)
                lat.append(ms)
            except Exception:
                pass
        lat = sorted(lat)
        n = len(lat)
        if not n:
            return {}
        pick = lambda q: lat[min(n - 1, int(q * n))]
        return {"n": n, "p50_ms": round(pick(0.50), 1), "p95_ms": round(pick(0.95), 1),
                "min_ms": round(lat[0], 1), "max_ms": round(lat[-1], 1)}

    def wait_for_value(self, table: str, key_col: str, key_val: str, watch_col: str,
                       at_least: float, timeout_s: float = 120.0, poll_s: float = 0.25,
                       on_poll=None):
        """Poll a keyed row until watch_col reaches at_least.

        Used for the headline freshness number. Returns dict with:
        {reached, polls, poll_interval_ms, observed, latency_ms, elapsed_s}
        """
        t0 = time.perf_counter()
        polls = 0
        while (time.perf_counter() - t0) < timeout_s:
            try:
                row, cols, _ = self.keyed_read(table, key_col, key_val, cols=f"{watch_col}")
                polls += 1
                seen = row[0] if row else None
                if on_poll:
                    on_poll(polls, seen)
                if seen is not None and float(seen) >= float(at_least):
                    wall_ms = time.time() * 1000.0
                    return {"reached": True, "polls": polls,
                            "poll_interval_ms": poll_s * 1000,
                            "observed": float(seen),
                            "latency_ms": round(wall_ms - float(at_least), 1),
                            "elapsed_s": round(time.perf_counter() - t0, 2)}
            except Exception as e:
                if on_poll:
                    on_poll(polls, None, str(e))
                pass
            time.sleep(poll_s)
        return {"reached": False, "polls": polls, "poll_interval_ms": poll_s * 1000,
                "elapsed_s": round(time.perf_counter() - t0, 2)}


def from_config(w, catalog: str, schema: str, endpoint_path: str, **kw) -> OnlineStore:
    """Build an OnlineStore from config parameters."""
    return OnlineStore(w, endpoint_path, pg_database=catalog, pg_schema=schema, **kw)
