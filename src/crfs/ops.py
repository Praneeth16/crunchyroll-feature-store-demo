"""Operational reads against the Online Feature Store and its sync pipelines.

Every wait in this repo goes through wait_for_sync(). The demo used to sleep 60
and 90 seconds and hope; these calls ask the platform what actually happened.

Sync state lives at GET /api/2.0/database/synced_tables/{full_name}. The
published online tables show up in UC as FOREIGN tables and each one is backed
by its own pipeline plus an event_log_<pipeline_id> table in the same schema.
"""
import time
import datetime as dt

SYNC_API = "/api/2.0/database/synced_tables"
STORE_API = "/api/2.0/feature-store/online-stores"

ONLINE_OK = "SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE"


def _get(w, path):
    return w.api_client.do("GET", path)


# ---------------------------------------------------------------- sync status
def sync_status(w, full_name: str) -> dict:
    """Raw synced-table record. full_name is the three-level online table name."""
    return _get(w, f"{SYNC_API}/{full_name}")


def _sync_fields(rec: dict) -> dict:
    """Flatten the parts we actually use. Shape has moved before, so read
    defensively rather than indexing blind."""
    st = rec.get("data_synchronization_status") or {}
    last = st.get("last_sync") or {}
    delta = last.get("delta_table_sync_info") or {}
    triggered = st.get("triggered_update_status") or {}
    t_delta = (triggered.get("delta_table_sync_info") or {})
    return {
        "detailed_state": st.get("detailed_state") or rec.get("detailed_state"),
        "pipeline_id": st.get("pipeline_id") or rec.get("pipeline_id"),
        "message": st.get("message"),
        "sync_start": last.get("sync_start_timestamp"),
        "sync_end": last.get("sync_end_timestamp"),
        "delta_commit_version": delta.get("delta_commit_version"),
        "delta_commit_timestamp": delta.get("delta_commit_timestamp"),
        "last_processed_commit_version": (
            triggered.get("last_processed_commit_version")
            or t_delta.get("delta_commit_version")
            or delta.get("delta_commit_version")
        ),
    }


def sync_summary(w, full_name: str) -> dict:
    out = _sync_fields(sync_status(w, full_name))
    out["name"] = full_name
    return out


# Delta operations that change no data. A sync pipeline never reports "processing"
# one of these, because there is nothing new to move -- so they must not be used as
# the commit version an online table is waited on.
#
# This is an exclusion list, not an allow-list, on purpose: an operation nobody
# anticipated is treated as data-changing, so the wait is too strict rather than too
# loose. Being too loose means reading stale online features and calling them fresh,
# which is the failure this whole module exists to prevent.
MAINTENANCE_OPS = (
    "OPTIMIZE", "VACUUM START", "VACUUM END", "ANALYZE TABLE", "COMPUTE STATISTICS",
    "SET TBLPROPERTIES", "UNSET TBLPROPERTIES", "REORG TABLE", "CLUSTER BY",
    "ADD CONSTRAINT", "DROP CONSTRAINT", "REFRESH TABLE",
)


def source_commit_version(spark, source_full_name: str):
    """The latest commit that actually changed data.

    Not simply `DESCRIBE HISTORY LIMIT 1`. Managed UC tables get predictive
    optimization, so the newest commit is frequently an `OPTIMIZE` -- and a synced
    table's `last_processed_commit_version` will never reach it, because there is no
    data in it to process. Anchoring the wait on that version makes
    `refresh_and_wait` time out on a table that is genuinely current.

    Measured: `viewer_rail_features_ts` at commit 9 (OPTIMIZE) with the sync correctly
    holding at commit 8 (MERGE) and reporting NO_PENDING_UPDATE. Waiting for 9 could
    only ever end in the 900s timeout that failed the vertical job.

    Returns None only when the table has no history at all.
    """
    rows = spark.sql(f"DESCRIBE HISTORY {source_full_name}").select("version", "operation").collect()
    if not rows:
        return None
    data_rows = [r for r in rows if (r["operation"] or "").upper() not in MAINTENANCE_OPS]
    if not data_rows:
        # Nothing but maintenance in the whole history: fall back to the newest commit
        # rather than returning None, which would silently disable the check.
        return int(max(int(r["version"]) for r in rows))
    return int(max(int(r["version"]) for r in data_rows))


def wait_for_sync(w, full_name: str, min_commit_version=None, timeout_s=300,
                  poll_s=3, verbose=True):
    """Block until the online table has caught up.

    Caught up means: state is ONLINE with no pending update and, when
    min_commit_version is given, the pipeline has processed at least that source
    Delta commit. Returns the final summary; raises TimeoutError.
    """
    t0 = time.time()
    last = None
    while time.time() - t0 < timeout_s:
        try:
            last = _sync_fields(sync_status(w, full_name))
        except Exception as e:                      # transient 404 right after publish
            if verbose:
                print(f"  [{time.time()-t0:5.1f}s] {full_name}: not queryable yet ({str(e)[:80]})")
            time.sleep(poll_s)
            continue
        processed = last.get("last_processed_commit_version")
        state = last.get("detailed_state")
        caught_up = state == ONLINE_OK and (
            min_commit_version is None
            or (processed is not None and int(processed) >= int(min_commit_version))
        )
        if verbose:
            print(f"  [{time.time()-t0:5.1f}s] {full_name}: {state} "
                  f"processed_commit={processed} want>={min_commit_version}")
        if caught_up:
            last["name"] = full_name
            return last
        if state and "FAILED" in state:
            raise RuntimeError(f"sync failed for {full_name}: {state} {last.get('message')}")
        time.sleep(poll_s)
    raise TimeoutError(f"{full_name} did not catch up in {timeout_s}s; last={last}")


def sync_lag_seconds(w, full_name: str):
    """Wall-clock seconds since the last completed sync ended."""
    end = _sync_fields(sync_status(w, full_name)).get("sync_end")
    if not end:
        return None
    ts = dt.datetime.fromisoformat(end.replace("Z", "+00:00"))
    return (dt.datetime.now(dt.timezone.utc) - ts).total_seconds()


def refresh_sync(w, full_name: str, verbose=True):
    """Kick a TRIGGERED synced table to re-read its source.

    publish_table() is a create, not an upsert: calling it on a table that is
    already published fails with
        AlreadyExists: Failing setup of Delta sync table: Destination table
        <name> already exists in schema <schema>
    So a refresh means starting an update on the pipeline the original publish
    created. Returns the pipeline id, or None if the table is not published.
    """
    try:
        pid = _sync_fields(sync_status(w, full_name)).get("pipeline_id")
    except Exception:
        return None
    if not pid:
        return None
    w.pipelines.start_update(pipeline_id=pid)
    if verbose:
        print(f"triggered refresh of {full_name} (pipeline {pid})")
    return pid


def refresh_and_wait(w, full_name: str, min_commit_version=None, timeout_s: int = 300,
                     poll_s: int = 3, trigger: bool = True, verbose: bool = True):
    """Trigger a refresh and wait for a *new* sync to finish.

    Why this exists instead of refresh_sync() + wait_for_sync(): waiting on
    `last_processed_commit_version >= source_version` can be satisfied by the
    PREVIOUS completed sync, so the wait returns immediately and the caller reads
    stale online values. Observed symptom: the freshness demo re-ranked with
    identical features and reported a delta of exactly 0.0.

    Anchoring on `sync_end_timestamp` removes the race -- we record it before
    triggering and require it to advance.

    Two ways to be satisfied, and both are needed:
      * a new sync finished (sync_end advanced), or
      * the pipeline is ONLINE and has already processed min_commit_version.

    The second case is not a loophole -- when the source data is byte-identical
    (this demo regenerates deterministically from SEED=42, so `title_features`
    often is), the merge produces nothing to sync and `sync_end` never moves. Demanding
    that it move made a correct, already-current table look like a 600s timeout.

    Pass trigger=False when a refresh has already been started (publish_or_refresh
    does that) so this only waits instead of kicking a second update.
    """
    before = _sync_fields(sync_status(w, full_name)).get("sync_end")
    if verbose:
        print(f"  {full_name}: sync_end before refresh = {before}")
    if trigger:
        refresh_sync(w, full_name, verbose=verbose)

    t0 = time.time()
    while time.time() - t0 < timeout_s:
        cur = _sync_fields(sync_status(w, full_name))
        state, end = cur.get("detailed_state"), cur.get("sync_end")
        if verbose:
            print(f"  [{time.time()-t0:5.1f}s] {full_name}: {state} sync_end={end}")
        if state and "FAILED" in state:
            raise RuntimeError(f"sync failed for {full_name}: {state} {cur.get('message')}")
        processed = cur.get("last_processed_commit_version")
        already_current = (
            min_commit_version is not None
            and processed is not None
            and int(processed) >= int(min_commit_version))
        if state == ONLINE_OK and (end != before or already_current):
            cur["name"] = full_name
            cur["sync_seconds"] = round(time.time() - t0, 1)
            cur["new_sync"] = end != before
            if verbose and not cur["new_sync"]:
                print(f"  {full_name}: nothing to sync, already at commit {processed}")
            return cur
        time.sleep(poll_s)
    raise TimeoutError(f"{full_name}: no new sync completed within {timeout_s}s")


def publish_or_refresh(w, fe, online_store_name: str, source_full: str, online_full: str,
                       publish_mode: str = "TRIGGERED", online_store=None, verbose=True):
    """Publish on first call, refresh on every call after that.

    Idempotency matters here: the spine job re-runs, and a demo gets rebuilt
    repeatedly. Returns "published" or "refreshed".
    """
    try:
        sync_status(w, online_full)
        exists = True
    except Exception:
        exists = False

    # A UC-less Postgres leftover from an earlier delete would make publish fail
    # with AlreadyExists, so clear it before trying.
    if not exists and online_store is not None:
        table = online_full.split(".")[-1]
        rows, _ = online_store.query(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
            (online_store.pg_schema, table))
        if rows:
            if verbose:
                print(f"{online_full}: orphaned postgres table found, dropping before publish")
            online_store.query(
                f'DROP TABLE IF EXISTS "{online_store.pg_schema}"."{table}" CASCADE')

    if exists:
        if publish_mode == "CONTINUOUS":
            # A continuous pipeline is already streaming; nothing to trigger.
            if verbose:
                print(f"{online_full}: already published CONTINUOUS, leaving it running")
            return "streaming"
        refresh_sync(w, online_full, verbose=verbose)
        return "refreshed"

    fe.publish_table(
        online_store=fe.get_online_store(name=online_store_name),
        source_table_name=source_full,
        online_table_name=online_full,
        publish_mode=publish_mode,
    )
    if verbose:
        print(f"published {source_full} -> {online_full} ({publish_mode})")
    return "published"


def drop_synced_if_exists(w, full_name: str, online_store=None, verbose=True) -> bool:
    """Fully unpublish an online table, using the documented API.

    Per the Online Feature Store docs, `w.feature_store.delete_online_table()` is the
    ONLY recommended method:

        "It removes the table from both Unity Catalog and the database. Other methods
         such as the Databricks SQL command DROP TABLE or the Python SDK command to
         delete a synced table do not delete the table from underlying database
         storage."

    That is exactly the trap this repo fell into first: deleting the *synced table*
    (DELETE /api/2.0/database/synced_tables/{name}) removed the UC entry and the
    pipeline but left the Postgres table behind, so the next publish_table failed with

        AlreadyExists: Failing setup of Delta sync table: Destination table <table>
        already exists in schema <schema>

    while Unity Catalog showed nothing at all. `online_store` is still accepted so a
    caller can clean up a Postgres leftover from that earlier era, but it should not be
    needed going forward.

    Warning from the same docs, worth honouring: deleting a published table "can lead
    to unexpected failures in downstream dependencies" -- make sure no model serving or
    feature serving endpoint still looks features up from it.
    """
    dropped = False
    try:
        w.feature_store.delete_online_table(online_table_name=full_name)
        dropped = True
        if verbose:
            print(f"deleted online table {full_name} (UC + database)")
    except Exception as e:
        msg = str(e)
        if verbose:
            print(f"delete_online_table({full_name}): {msg[:140]}")
        # Fall back to the synced-table API for tables published before this path
        # existed, then clear the Postgres leftover it leaves behind.
        try:
            sync_status(w, full_name)
            w.api_client.do("DELETE", f"{SYNC_API}/{full_name}")
            dropped = True
            if verbose:
                print(f"  fell back to the synced-table API for {full_name}")
        except Exception:
            pass

    if online_store is not None:
        table = full_name.split(".")[-1]
        try:
            rows, _ = online_store.query(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = %s",
                (online_store.pg_schema, table))
            if rows:
                online_store.query(
                    f'DROP TABLE IF EXISTS "{online_store.pg_schema}"."{table}" CASCADE')
                dropped = True
                if verbose:
                    print(f"  cleared postgres leftover {online_store.pg_schema}.{table}")
        except Exception as e:
            if verbose:
                print(f"  could not check/clear postgres table {table}: {str(e)[:120]}")
    return dropped


# ------------------------------------------------------------ pipeline health
def pipeline_health(w, pipeline_id: str) -> dict:
    if not pipeline_id:
        return {}
    p = w.pipelines.get(pipeline_id)
    latest = (p.latest_updates or [None])[0]
    return {
        "pipeline_id": pipeline_id,
        "name": p.name,
        "state": getattr(p.state, "value", str(p.state)),
        "last_update_state": getattr(getattr(latest, "state", None), "value", None),
        "last_update_created": getattr(latest, "creation_time", None),
    }


# ------------------------------------------------------- online store & compute
def online_store_status(w, name: str) -> dict:
    return _get(w, f"{STORE_API}/{name}")


def lakebase_endpoint(w, endpoint_path: str) -> dict:
    rec = _get(w, f"/api/2.0/postgres/{endpoint_path}")
    st = rec.get("status") or {}
    return {
        "uid": rec.get("uid"),
        "state": st.get("current_state"),
        "min_cu": st.get("autoscaling_limit_min_cu"),
        "max_cu": st.get("autoscaling_limit_max_cu"),
        "host": (st.get("hosts") or {}).get("host"),
        "pooled_host": (st.get("hosts") or {}).get("read_write_pooled_host"),
        "last_active": st.get("last_active_time"),
    }


# ------------------------------------------------------------------- cost
COST_SQL = """
SELECT u.usage_date,
       u.sku_name,
       ROUND(SUM(u.usage_quantity), 2)                          AS dbu,
       ROUND(SUM(u.usage_quantity * p.pricing.default), 2)      AS usd_list
FROM system.billing.usage u
JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
 AND u.usage_end_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
WHERE {predicate}
  AND u.usage_date >= current_date() - {days}
GROUP BY u.usage_date, u.sku_name
ORDER BY u.usage_date DESC, usd_list DESC
"""


def cost_sql(endpoint_uid: str = None, endpoint_names=None, days: int = 14) -> str:
    """Daily DBU and list USD. Filter by the Lakebase endpoint uid, by serving
    endpoint names, or both."""
    preds = []
    if endpoint_uid:
        preds.append(f"u.usage_metadata.endpoint_id = '{endpoint_uid}'")
    if endpoint_names:
        quoted = ", ".join(f"'{n}'" for n in endpoint_names)
        preds.append(f"u.usage_metadata.endpoint_name IN ({quoted})")
    predicate = "(" + " OR ".join(preds) + ")" if preds else "TRUE"
    return COST_SQL.format(predicate=predicate, days=days)


def daily_cost(spark, endpoint_uid: str = None, endpoint_names=None, days: int = 14):
    return spark.sql(cost_sql(endpoint_uid, endpoint_names, days))
