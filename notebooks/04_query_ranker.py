# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · One governed API: keys + context in, ranked titles out
# MAGIC
# MAGIC The application sends what only it knows — `viewer_id`, candidate
# MAGIC `title_id`s, surface, device, locale, hour. The endpoint retrieves the
# MAGIC current viewer and title features from the Lakebase online store,
# MAGIC scores every candidate in one pass, and returns play-start probabilities.
# MAGIC
# MAGIC This notebook also measures keyed-read latency against the online store
# MAGIC and reads back the inference table the endpoint captured.
# COMMAND ----------
# MAGIC %pip install databricks-sdk "psycopg[binary]" --quiet
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import os, sys
_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _root not in sys.path:
    sys.path.insert(0, _root)
from src.crfs.config import Config
from src.crfs import candidates as C

cfg = Config.from_widgets(dbutils)
CATALOG, SCHEMA = cfg.catalog, cfg.schema
ENDPOINT = cfg.ranker_endpoint
spark.sql(f"USE {cfg.fq}")

import time, json
import pandas as pd
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

viewer = spark.sql(f"""
  SELECT v.viewer_id, v.tier, v.age_bracket
  FROM {CATALOG}.{SCHEMA}.viewers v
  WHERE v.age_bracket <> '13-17'
  ORDER BY v.activity_level DESC LIMIT 1
""").first()
VID = viewer["viewer_id"]
print("demo viewer:", VID, "| tier:", viewer["tier"], "| age:", viewer["age_bracket"])

candidates = spark.sql(f"""
  SELECT e.title_id, t.title_name, t.primary_genre
  FROM {CATALOG}.{SCHEMA}.entitlements e
  JOIN {CATALOG}.{SCHEMA}.titles t ON e.title_id = t.title_id
  WHERE e.viewer_id = '{VID}' AND e.allowed
  ORDER BY t.intrinsic_popularity DESC LIMIT 25
""").toPandas()
print("eligible candidates:", len(candidates))

# Built by src/crfs/candidates.py so this notebook, notebook 05 and the app all send
# the identical payload. request_epoch_s matters: without it the v2 ranker's
# cr_session_decay UDF receives None, guards to 0.0, and the feature is silently dead.
REQUEST_EPOCH = int(time.time())
records = C.request_records(VID, candidates["title_id"], surface="post_play",
                            device="tv", locale="en-US", hour_of_day=21,
                            request_epoch_s=REQUEST_EPOCH)

t0 = time.time()
resp = w.serving_endpoints.query(name=ENDPOINT, dataframe_records=records)
query_ms = (time.time() - t0) * 1000
scores = [float(p) for p in resp.predictions]
print(f"endpoint returned {len(scores)} scores in {query_ms:.0f} ms")

ranked = candidates.copy()
ranked["play_start_probability"] = scores
ranked = ranked.sort_values("play_start_probability", ascending=False).reset_index(drop=True)
ranked.index = ranked.index + 1
display(ranked[["title_name", "primary_genre", "play_start_probability"]].head(12))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Keyed-read latency against the Lakebase online store
# MAGIC
# MAGIC Measured the way an application reads: one Postgres connection, one keyed
# MAGIC `SELECT ... WHERE viewer_id = %s` per call.
# MAGIC
# MAGIC The first version of this notebook timed `spark.sql()` against the FOREIGN
# MAGIC table instead and reported ~1 s as "online keyed read latency". That number
# MAGIC was serverless SQL planning plus a federated read — it never touched the
# MAGIC serving path. Both numbers are printed below, clearly labelled, because the
# MAGIC gap between them is itself worth explaining.
# COMMAND ----------
from src.crfs import online

viewer_ids = [r.viewer_id for r in spark.sql(
    f"SELECT viewer_id FROM {cfg.t('viewers')} ORDER BY rand() LIMIT 30").collect()]

store = online.from_config(w, cfg)
keyed = store.keyed_read_latency("online_viewer_features", "viewer_id", viewer_ids)
print("Lakebase keyed reads via Postgres, from the notebook driver (in-region):")
print("   ", keyed)

row, cols, one_ms = store.keyed_read("online_viewer_features", "viewer_id", viewer_ids[0])
print(f"\nexample row ({one_ms:.1f} ms):")
print("   ", dict(list(zip(cols, row))[:6]) if row else None)

sql_lat = []
for vid in viewer_ids[:5]:
    t0 = time.time()
    spark.sql(f"SELECT * FROM {cfg.t('online_viewer_features')} WHERE viewer_id = '{vid}'").collect()
    sql_lat.append((time.time() - t0) * 1000)
sql_p50 = sorted(sql_lat)[len(sql_lat) // 2]
print(f"\nSame rows via spark.sql on the FOREIGN table: p50={sql_p50:.0f} ms")
print("    ^ SQL planning + federated read. Not the serving path. Do not quote this")
print("      as online-store latency.")
store.close()
# COMMAND ----------
# MAGIC %md
# MAGIC ## What the endpoint saw and decided — inference table
# COMMAND ----------
tables = [r.tableName for r in spark.sql(
    f"SHOW TABLES IN {CATALOG}.{SCHEMA} LIKE 'cr_ranker_inference*'").collect()]
print("inference tables:", tables)
payload_tbl = next((t for t in tables if t.endswith("_payload")), tables[0] if tables else None)
if payload_tbl:
    captured = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.{payload_tbl} LIMIT 3")
    display(captured)
    print("payload table:", payload_tbl, "| columns:", captured.columns, "| rows:", captured.count())

dbutils.notebook.exit(json.dumps({
    "endpoint": ENDPOINT,
    "request_epoch_s": REQUEST_EPOCH,
    "endpoint_query_ms": round(query_ms),
    "candidates_scored": len(records),
    "keyed_read_postgres": keyed,
    "sql_foreign_table_p50_ms": round(sql_p50),
    "top_pick": str(ranked.iloc[0]["title_name"]),
    "top_score": round(float(ranked.iloc[0]["play_start_probability"]), 4),
}))
