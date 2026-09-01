# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Freshness loop — a skip should change the next ranking
# MAGIC
# MAGIC The deck's Phase 3 story, shrunk to minutes:
# MAGIC
# MAGIC 1. Reset the viewer to a calm baseline (repeatable), query the ranker.
# MAGIC 2. Simulate an in-session burst: the viewer binges sci-fi episodes right now.
# MAGIC 3. Recompute `recent_behavior_current`, re-publish to Lakebase (TRIGGERED).
# MAGIC 4. Query again — every candidate re-scores on the fresh features.
# MAGIC
# MAGIC Production swaps steps 2–3 for Kafka → Spark Real-Time Mode → Lakebase
# MAGIC (200 ms p99 published benchmark). The demo shows the same contract end
# MAGIC to end: event → feature → online store → different score.
# COMMAND ----------
# MAGIC %pip install databricks-sdk --quiet
# COMMAND ----------
# MAGIC %pip install databricks-feature-engineering --quiet
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
dbutils.widgets.text("catalog", "serverless_lakebase_praneeth_catalog")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = "crunchyroll_demo"
ONLINE_STORE = "crunchyroll-online-store"
ENDPOINT = "crunchyroll-watch-next-ranker"
spark.sql(f"USE {CATALOG}.{SCHEMA}")

import time, json
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.feature_engineering import FeatureEngineeringClient
w = WorkspaceClient()
fe = FeatureEngineeringClient()

VID = "v0001"

# Reset the demo viewer to a calm baseline so the loop is repeatable:
# yesterday's v0001 was watching slice-of-life, not sci-fi.
calm = pd.DataFrame([{
    "viewer_id": VID,
    "minutes_watched_24h": 38.5,
    "skips_24h": 0,
    "active_titles_24h": 2,
    "last_primary_genre": "slice_of_life",
}])
fe.write_table(name=f"{CATALOG}.{SCHEMA}.recent_behavior_current",
               df=spark.createDataFrame(calm), mode="merge")
fe.publish_table(
    online_store=fe.get_online_store(name=ONLINE_STORE),
    source_table_name=f"{CATALOG}.{SCHEMA}.recent_behavior_current",
    online_table_name=f"{CATALOG}.{SCHEMA}.online_recent_behavior",
    publish_mode="TRIGGERED",
)
print("baseline reset published; waiting for sync...")
time.sleep(90)

def query_ranker(records):
    t0 = time.time()
    resp = w.serving_endpoints.query(name=ENDPOINT, dataframe_records=records)
    return [float(p) for p in resp.predictions], (time.time() - t0) * 1000

candidates = spark.sql(f"""
  SELECT e.title_id, t.title_name, t.primary_genre
  FROM {CATALOG}.{SCHEMA}.entitlements e
  JOIN {CATALOG}.{SCHEMA}.titles t ON e.title_id = t.title_id
  WHERE e.viewer_id = '{VID}' AND e.allowed
  ORDER BY t.intrinsic_popularity DESC LIMIT 25
""").toPandas()
records = [{"viewer_id": VID, "title_id": r.title_id, "surface": "post_play",
            "device": "tv", "locale": "en-US", "hour_of_day": 21}
           for r in candidates.itertuples()]

before, ms1 = query_ranker(records)
candidates["score_before"] = before
print(f"baseline query: {ms1:.0f} ms")
print("online recent_behavior BEFORE:")
print(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.online_recent_behavior WHERE viewer_id='{VID}'").toPandas().to_string(index=False))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Simulate the in-session burst
# COMMAND ----------
from datetime import datetime, timedelta
now = datetime.now()
sci_fi_titles = spark.sql(f"""
  SELECT title_id FROM {CATALOG}.{SCHEMA}.titles
  WHERE primary_genre = 'sci_fi' AND is_simulcast ORDER BY intrinsic_popularity DESC LIMIT 3
""").toPandas()["title_id"].tolist()

burst = []
for i, tid in enumerate(sci_fi_titles):
    burst.append({
        "event_id": f"burst-{VID}-{i}",
        "viewer_id": VID, "title_id": tid,
        "event_ts": now - timedelta(minutes=(3 - i) * 24),
        "event_type": "complete",
        "session_id": f"burst-{VID}-s1",
        "surface": "post_play", "device": "tv", "locale": "en-US",
        "hour_of_day": 21, "position": None, "played": None,
        "watch_seconds": 1420,
    })
burst_sdf = spark.createDataFrame(pd.DataFrame(burst))
burst_sdf.write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.engagement_events")
print("appended", len(burst), "complete events for", VID, "on sci-fi titles:", sci_fi_titles)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Recompute the recent-behavior feature and re-publish
# COMMAND ----------
ev = spark.table(f"{CATALOG}.{SCHEMA}.engagement_events").toPandas()
title_genre = spark.table(f"{CATALOG}.{SCHEMA}.titles").toPandas().set_index("title_id")["primary_genre"].to_dict()

last24 = ev[pd.to_datetime(ev["event_ts"]) > pd.Timestamp(now) - pd.Timedelta(hours=24)]
g = last24[last24["viewer_id"] == VID]
g_w = g[g["watch_seconds"].fillna(0) > 0]
last_genre = "none"
if len(g_w):
    last_genre = title_genre.get(g_w.sort_values("event_ts").iloc[-1]["title_id"], "none")

new_row = pd.DataFrame([{
    "viewer_id": VID,
    "minutes_watched_24h": round(float(g_w["watch_seconds"].sum() / 60.0), 2),
    "skips_24h": int((g["event_type"] == "skip").sum()),
    "active_titles_24h": int(g_w["title_id"].nunique()),
    "last_primary_genre": last_genre,
}])
print("new recent_behavior row:", new_row.to_string(index=False))

fe.write_table(name=f"{CATALOG}.{SCHEMA}.recent_behavior_current",
               df=spark.createDataFrame(new_row), mode="merge")

fe.publish_table(
    online_store=fe.get_online_store(name=ONLINE_STORE),
    source_table_name=f"{CATALOG}.{SCHEMA}.recent_behavior_current",
    online_table_name=f"{CATALOG}.{SCHEMA}.online_recent_behavior",
    publish_mode="TRIGGERED",
)
print("re-published recent_behavior_current; waiting for sync...")
time.sleep(90)
print("online recent_behavior AFTER:")
print(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.online_recent_behavior WHERE viewer_id='{VID}'").toPandas().to_string(index=False))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Query again — the ranking moved
# COMMAND ----------
after, ms2 = query_ranker(records)
candidates["score_after"] = after
candidates["delta"] = (candidates["score_after"] - candidates["score_before"]).round(4)

moved = candidates.sort_values("delta", ascending=False)
print(f"second query: {ms2:.0f} ms")
print("\nBiggest movers (delta = after - before):")
print(moved[["title_name", "primary_genre", "score_before", "score_after", "delta"]].head(8).to_string(index=False))

top_before = candidates.loc[candidates["score_before"].idxmax(), "title_name"]
top_after = candidates.loc[candidates["score_after"].idxmax(), "title_name"]
print(f"\nTop pick before: {top_before}\nTop pick after:  {top_after}")
# COMMAND ----------
dbutils.notebook.exit(json.dumps({
    "viewer": VID,
    "top_before": str(top_before), "top_after": str(top_after),
    "max_delta": float(candidates["delta"].max()),
    "minutes_watched_24h": float(new_row["minutes_watched_24h"].iloc[0]),
    "last_primary_genre": str(last_genre),
}))
