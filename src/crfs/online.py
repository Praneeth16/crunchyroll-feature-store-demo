"""Direct Postgres access to the Lakebase-backed Online Feature Store.

Why this exists: the serving endpoint's own feature lookup is the fast path we
care about, but to *prove* freshness and keyed-read latency we need to read the
online store the way an application would. spark.sql() against the FOREIGN table
does not measure that -- it measures serverless SQL planning plus a federated
read (~1s) and has nothing to do with the serving path.

Auth is OAuth: POST /api/2.0/postgres/credentials mints a short-lived token used
as the Postgres password. Tokens last about an hour, so the connection is cached
with a 50-minute expiry and reopened on OperationalError.

Connect to the *direct* endpoint host, not the pooler. Verified 2026-09-07: the
read_write_pooled_host rejects OAuth credentials with "SASL authentication
failed", while the same token authenticates fine against status.hosts.host. Pass
pooled=True only if that changes.

The published online tables are read-only. Never write to them -- the sync
pipeline owns them and a manual write breaks it.
"""
import time

CRED_API = "/api/2.0/postgres/credentials"
TOKEN_TTL_S = 50 * 60


def mint_token(w, endpoint_path: str) -> str:
    resp = w.api_client.do("POST", CRED_API, body={"endpoint": endpoint_path})
    return resp["token"]


def endpoint_hosts(w, endpoint_path: str) -> dict:
    st = (w.api_client.do("GET", f"/api/2.0/postgres/{endpoint_path}") or {}).get("status") or {}
    hosts = st.get("hosts") or {}
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

    # ---------------------------------------------------------------- connect
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

    # ------------------------------------------------------------------ reads
    def query(self, sql: str, params=None, retry: bool = True):
        """Run SQL, returning (rows, columns). Reconnects once on a dropped
        connection -- the endpoint can recycle, and a stale token looks the same."""
        import psycopg
        try:
            with self.conn().cursor() as cur:
                cur.execute(sql, params)
                # DDL and DML produce no result set; fetchall() would raise
                # "the last operation didn't produce records".
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
        # `table` and `cols` were spliced in unvalidated while the docstring claimed
        # otherwise. No caller passes anything but literals today, so this closes a
        # latent hazard rather than a live one -- but a helper that will splice anything
        # into a FROM clause is exactly what the docstring says it must not be.
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
        """p50/p95 over a list of keys. Warmup reads are excluded so connection
        setup and first-plan cost do not pollute the percentiles."""
        for k in list(key_vals)[:warmup]:
            self.keyed_read(table, key_col, k)
        lat = sorted(self.keyed_read(table, key_col, k)[2] for k in key_vals)
        n = len(lat)
        if not n:
            return {}
        pick = lambda q: lat[min(n - 1, int(q * n))]
        return {"n": n, "p50_ms": round(pick(0.50), 1), "p95_ms": round(pick(0.95), 1),
                "min_ms": round(lat[0], 1), "max_ms": round(lat[-1], 1)}

    # ------------------------------------------------- freshness measurement
    def wait_for_value(self, table: str, key_col: str, key_val, watch_col: str,
                       at_least, timeout_s: float = 120.0, poll_s: float = 0.25,
                       verbose: bool = True):
        """Poll a keyed row until watch_col reaches at_least.

        Used for the headline freshness number. watch_col carries the producer's
        own produced_epoch_ms, so the reported latency subtracts two readings of
        the *same* clock -- no clock-skew argument. The poll interval is returned
        because it quantizes the answer and the audience deserves to see it.
        """
        t0 = time.perf_counter()
        polls = 0
        while (time.perf_counter() - t0) < timeout_s:
            row, cols, _ = self.keyed_read(table, key_col, key_val,
                                           cols=f"{watch_col}")
            polls += 1
            seen = row[0] if row else None
            if seen is not None and float(seen) >= float(at_least):
                wall_ms = time.time() * 1000.0
                return {"reached": True, "polls": polls,
                        "poll_interval_ms": poll_s * 1000,
                        "observed": float(seen),
                        "latency_ms": round(wall_ms - float(at_least), 1),
                        "elapsed_s": round(time.perf_counter() - t0, 2)}
            time.sleep(poll_s)
        return {"reached": False, "polls": polls, "poll_interval_ms": poll_s * 1000,
                "elapsed_s": round(time.perf_counter() - t0, 2)}


def from_config(w, cfg, **kw) -> OnlineStore:
    """Build an OnlineStore from a crfs Config."""
    return OnlineStore(w, cfg.endpoint_path, pg_database=cfg.catalog,
                       pg_schema=cfg.schema, **kw)
