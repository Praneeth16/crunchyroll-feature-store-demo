"""Ops footer: every number read live, none typed in. Cached, never on the hot path."""
import asyncio
import datetime as dt
import time

from . import settings as S
from .clients import sql_rows, w

SYNC_TABLES = ["online_viewer_features", "online_recent_behavior", "online_title_features",
               "online_session_features", "online_viewer_rail", "online_rail_features"]

_cache: dict = {}


async def cached(key: str, ttl: float, fn):
    hit = _cache.get(key)
    if hit and time.time() - hit[0] < ttl:
        return hit[1]
    val = await asyncio.to_thread(fn)
    _cache[key] = (time.time(), val)
    return val


def _store():
    store = w.api_client.do("GET", f"/api/2.0/feature-store/online-stores/{S.ONLINE_STORE}") or {}
    ep = w.api_client.do("GET", f"/api/2.0/postgres/{S.ENDPOINT_PATH}") or {}
    st = ep.get("status") or {}
    return {"capacity": store.get("capacity"), "state": store.get("state"),
            "read_replicas": store.get("read_replica_count"),
            "endpoint_uid": ep.get("uid"), "endpoint_state": st.get("current_state"),
            "min_cu": st.get("autoscaling_limit_min_cu"), "max_cu": st.get("autoscaling_limit_max_cu")}


def _sync_one(t: str) -> dict:
    try:
        rec = w.api_client.do("GET", f"/api/2.0/database/synced_tables/{S.FQ}.{t}") or {}
        status = rec.get("data_synchronization_status") or {}
        end = (status.get("last_sync") or {}).get("sync_end_timestamp")
        lag = ((dt.datetime.now(dt.timezone.utc) - dt.datetime.fromisoformat(end.replace("Z", "+00:00")))
               .total_seconds() if end else None)
        return {"table": t, "state": status.get("detailed_state"),
                "lag_s": None if lag is None else round(lag, 1)}
    except Exception as e:
        return {"table": t, "state": f"unavailable ({str(e)[:120]})", "lag_s": None}


def _sync_lag():
    # Six independent REST reads; serially they were most of this panel's wait.
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(len(SYNC_TABLES)) as ex:
        return list(ex.map(_sync_one, SYNC_TABLES))


def _cost(endpoint_uid: str):
    return sql_rows(f"""
    SELECT u.usage_date, u.sku_name,
           ROUND(SUM(u.usage_quantity), 2) AS dbu,
           ROUND(SUM(u.usage_quantity * p.pricing.default), 2) AS usd_list
    FROM system.billing.usage u
    JOIN system.billing.list_prices p
      ON u.sku_name = p.sku_name
     AND u.usage_end_time >= p.price_start_time
     AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
    WHERE (u.usage_metadata.endpoint_id = '{endpoint_uid or ""}'
        OR u.usage_metadata.endpoint_name LIKE 'crunchyroll-%')
      AND u.usage_date >= current_date() - 3
    GROUP BY u.usage_date, u.sku_name
    ORDER BY u.usage_date DESC, usd_list DESC""")


async def ops() -> dict:
    store, sync = await asyncio.gather(cached("store", 60, _store), cached("sync", 60, _sync_lag),
                                       return_exceptions=True)
    out = {"store": store if not isinstance(store, Exception) else {"error": str(store)[:200]},
           "sync": sync if not isinstance(sync, Exception) else
           [{"table": "-", "state": str(sync)[:200], "lag_s": None}]}
    try:
        # The Lakebase endpoint bills under its uid, not a crunchyroll-* name.
        uid = out["store"].get("endpoint_uid")
        out["cost"] = await cached("cost", 300, lambda: _cost(uid))
    except Exception as e:
        out["cost_error"] = str(e)[:200]
    return out
