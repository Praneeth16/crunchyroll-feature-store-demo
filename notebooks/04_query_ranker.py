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
# MAGIC %pip install databricks-sdk --quiet
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
dbutils.widgets.text("catalog", "serverless_lakebase_praneeth_catalog")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = "crunchyroll_demo"
ENDPOINT = "crunchyroll-watch-next-ranker"
spark.sql(f"USE {CATALOG}.{SCHEMA}")

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

records = [{"viewer_id": VID, "title_id": r.title_id, "surface": "post_play",
            "device": "tv", "locale": "en-US", "hour_of_day": 21}
           for r in candidates.itertuples()]

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
# COMMAND ----------
viewer_ids = [r.viewer_id for r in spark.sql(
    f"SELECT viewer_id FROM {CATALOG}.{SCHEMA}.viewers ORDER BY rand() LIMIT 30").collect()]
lat = []
for vid in viewer_ids:
    t0 = time.time()
    spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.online_viewer_features WHERE viewer_id = '{vid}'").collect()
    lat.append((time.time() - t0) * 1000)
lat = sorted(lat)
p50 = lat[len(lat) // 2]
p95 = lat[int(len(lat) * 0.95)]
print(f"online keyed reads (n={len(lat)}): p50={p50:.0f} ms, p95={p95:.0f} ms, min={lat[0]:.0f} ms")
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
    "endpoint_query_ms": round(query_ms),
    "online_read_p50_ms": round(p50), "online_read_p95_ms": round(p95),
    "top_pick": str(ranked.iloc[0]["title_name"]),
    "top_score": round(float(ranked.iloc[0]["play_start_probability"]), 4),
}))
