# Databricks notebook source
# MAGIC %md
# MAGIC # 13 · Operating the online store — sync health, capacity, cost
# MAGIC
# MAGIC The question a platform team asks after the demo lands: what does it take
# MAGIC to run this, and what does it cost? Everything below is read from the
# MAGIC platform's own APIs and billing system tables — nothing is estimated.
# MAGIC
# MAGIC Say:
# MAGIC > "One always-on cost in this architecture: the online store. Lakebase
# MAGIC > online stores cannot scale to zero, because a store that sleeps cannot
# MAGIC > answer a keyed read in single-digit milliseconds. Everything else here
# MAGIC > scales to zero."
# COMMAND ----------
# MAGIC %pip install databricks-sdk --quiet
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import os, sys
_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _root not in sys.path:
    sys.path.insert(0, _root)

from src.crfs.config import Config
from src.crfs import ops

cfg = Config.from_widgets(dbutils)
spark.sql(f"USE {cfg.fq}")

import json
import datetime as dt
import pandas as pd
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
print(cfg.describe())
# COMMAND ----------
# MAGIC %md
# MAGIC ## Which online tables exist, and are they caught up?
# MAGIC
# MAGIC A published online table is a FOREIGN table in Unity Catalog backed by its
# MAGIC own sync pipeline. `detailed_state` is the operator's single most useful
# MAGIC field; the commit versions tell you whether the pipeline has actually
# MAGIC consumed what the source last wrote.
# COMMAND ----------
ONLINE_PAIRS = [
    ("viewer_features_current", "online_viewer_features"),
    ("title_features", "online_title_features"),
    ("recent_behavior_current", "online_recent_behavior"),
    ("viewer_embedding_current", "online_viewer_embedding"),
    ("session_features_current", "online_session_features"),
]

rows = []
for src, dst in ONLINE_PAIRS:
    src_full, dst_full = cfg.t(src), cfg.t(dst)
    if not spark.catalog.tableExists(dst_full):
        print(f"skip {dst}: not published yet")
        continue
    try:
        s = ops.sync_summary(w, dst_full)
    except Exception as e:
        print(f"skip {dst}: {str(e)[:120]}")
        continue
    src_version = ops.source_commit_version(spark, src_full) if spark.catalog.tableExists(src_full) else None
    lag = ops.sync_lag_seconds(w, dst_full)
    health = ops.pipeline_health(w, s.get("pipeline_id"))
    rows.append({
        "online_table": dst,
        "detailed_state": s.get("detailed_state"),
        "source_commit": src_version,
        "processed_commit": s.get("last_processed_commit_version"),
        "behind_by": (None if src_version is None or s.get("last_processed_commit_version") is None
                      else src_version - int(s["last_processed_commit_version"])),
        "sync_end": s.get("sync_end"),
        "lag_seconds": None if lag is None else round(lag, 1),
        "pipeline_state": health.get("state"),
        "last_update_state": health.get("last_update_state"),
        "pipeline_id": s.get("pipeline_id"),
        "online_rows": spark.table(dst_full).count(),
        "offline_rows": spark.table(src_full).count() if src_version is not None else None,
    })

sync_pdf = pd.DataFrame(rows)
display(spark.createDataFrame(sync_pdf) if len(sync_pdf) else spark.createDataFrame([], "empty STRING"))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Persist sync state so a dashboard can see it
# MAGIC
# MAGIC AI/BI dashboards run SQL; they cannot call a REST API. So this notebook
# MAGIC writes the API's own numbers to a Delta table each run, and the dashboard
# MAGIC reads that. It is the only honest way to chart sync lag.
# COMMAND ----------
LOG = cfg.t("crfs_ops_sync_log")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {LOG} (
  captured_at TIMESTAMP,
  online_table STRING,
  detailed_state STRING,
  sync_end TIMESTAMP,
  lag_seconds DOUBLE,
  delta_commit_version LONG,
  last_processed_commit_version LONG
)
""")

if len(sync_pdf):
    log_pdf = pd.DataFrame({
        "captured_at": pd.Timestamp.utcnow().tz_localize(None),
        "online_table": sync_pdf["online_table"],
        "detailed_state": sync_pdf["detailed_state"],
        "sync_end": pd.to_datetime(sync_pdf["sync_end"], errors="coerce", utc=True).dt.tz_localize(None),
        "lag_seconds": sync_pdf["lag_seconds"].astype(float),
        "delta_commit_version": sync_pdf["source_commit"].astype("Int64").astype("float"),
        "last_processed_commit_version": pd.to_numeric(
            sync_pdf["processed_commit"], errors="coerce"),
    })
    (spark.createDataFrame(log_pdf).write.mode("append").saveAsTable(LOG))
    print(f"appended {len(log_pdf)} rows to {LOG}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## The online store and its Lakebase compute
# MAGIC
# MAGIC The capacity class governs the backing endpoint's compute floor: `CU_1`
# MAGIC produces a 4–8 CU endpoint, `CU_2` an 8–16 CU one. Verified by resizing this
# MAGIC store from CU_2 to CU_1 and watching the endpoint bounds move.
# COMMAND ----------
store = ops.online_store_status(w, cfg.online_store)
print("online store:", json.dumps(store, indent=2))

endpoint = ops.lakebase_endpoint(w, cfg.endpoint_path)
print("\nLakebase endpoint:", json.dumps(endpoint, indent=2))
print("\nscale-to-zero: not supported for online stores - this compute is always on")
# COMMAND ----------
# MAGIC %md
# MAGIC ## What it actually costs
# MAGIC
# MAGIC Straight from `system.billing.usage` joined to `system.billing.list_prices`,
# MAGIC filtered to this store's Lakebase endpoint and to the serving endpoints.
# MAGIC List prices, so a customer with a committed contract should read these as an
# MAGIC upper bound.
# COMMAND ----------
uid = endpoint.get("uid")
serving = [cfg.ranker_endpoint, cfg.retriever_endpoint, cfg.feature_endpoint, cfg.agent_endpoint]

cost = ops.daily_cost(spark, endpoint_uid=uid, endpoint_names=serving, days=14)
display(cost)

totals = cost.groupBy("sku_name").sum("dbu", "usd_list").toPandas()
print("\n14-day totals by SKU:")
for _, r in totals.iterrows():
    print(f"  {r['sku_name']:<62} {r['sum(dbu)']:>9.2f} DBU  ${r['sum(usd_list)']:>8.2f}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## What you would change, with the measured numbers attached
# COMMAND ----------
lakebase_rows = cost.filter("sku_name LIKE '%DATABASE_SERVERLESS%'").toPandas()
per_day = (lakebase_rows.groupby("usage_date")["usd_list"].sum().sort_index()
           if len(lakebase_rows) else pd.Series(dtype=float))
recent = per_day.tail(3).mean() if len(per_day) else float("nan")

print(f"online store, recent daily list cost: ${recent:,.2f}/day  ~ ${recent * 30:,.0f}/month")
print(f"current capacity: {store.get('capacity')}  endpoint {endpoint.get('min_cu')}-{endpoint.get('max_cu')} CU")
print("""
Levers, in the order a platform team should reach for them:
  1. Capacity class. CU_1 is the floor; it halves the endpoint's CU bounds versus
     CU_2. Right-size to the working set, not to the peak QPS you hope for.
  2. Read replicas. read_replica_count > 0 buys failover and read throughput, and
     multiplies the always-on cost. Zero is correct until a keyed read is on a
     latency-critical path with real traffic.
  3. Publish mode per table. TRIGGERED costs a pipeline run per refresh;
     CONTINUOUS holds a streaming pipeline open. Reserve CONTINUOUS for features
     that must change a decision inside the session.
  4. Which features go online at all. Only serving-critical current values need
     to be there. History belongs offline.
  5. Serving endpoints scale to zero. Leave that on except on demo day, where a
     cold start on stage costs more than the compute.
""")
# COMMAND ----------
dbutils.notebook.exit(json.dumps({
    "online_store": {k: store.get(k) for k in ("name", "capacity", "state", "read_replica_count")},
    "lakebase_endpoint": endpoint,
    "online_tables": rows,
    "recent_daily_usd": None if pd.isna(recent) else round(float(recent), 2),
    "sync_log_table": LOG,
}, default=str))
