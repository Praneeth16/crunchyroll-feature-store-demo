# Databricks notebook source
# PIT probe: for 5 real impressions, compare viewer features at impression time (viewer_features_ts) vs today (viewer_features_current)
dbutils.widgets.text("catalog", "serverless_lakebase_praneeth_catalog")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = "crunchyroll_demo"
spark.sql(f"USE {CATALOG}.{SCHEMA}")
import json

rows = spark.sql(f"""
WITH impressions AS (
  SELECT viewer_id, event_ts
  FROM {CATALOG}.{SCHEMA}.engagement_events
  WHERE event_type = 'impression'
  ORDER BY rand(42)
  LIMIT 5
),
pit AS (
  SELECT i.viewer_id, i.event_ts,
         v.minutes_watched_7d, v.genre_affinity_action,
         ROW_NUMBER() OVER (PARTITION BY i.viewer_id, i.event_ts ORDER BY v.ts DESC) rn
  FROM impressions i
  JOIN {CATALOG}.{SCHEMA}.viewer_features_ts v
    ON v.viewer_id = i.viewer_id AND v.ts <= i.event_ts
)
SELECT p.viewer_id,
       CAST(p.event_ts AS STRING) event_ts,
       ROUND(p.genre_affinity_action, 3) aff_action_pit,
       ROUND(c.genre_affinity_action, 3) aff_action_now,
       ROUND(p.minutes_watched_7d, 1) min7d_pit,
       ROUND(c.minutes_watched_7d, 1) min7d_now
FROM pit p
JOIN {CATALOG}.{SCHEMA}.viewer_features_current c ON c.viewer_id = p.viewer_id
WHERE p.rn = 1
""").collect()

out = [r.asDict() for r in rows]
import pandas as pd
print(pd.DataFrame(out).to_string(index=False))
# COMMAND ----------
dbutils.notebook.exit(json.dumps(out))
