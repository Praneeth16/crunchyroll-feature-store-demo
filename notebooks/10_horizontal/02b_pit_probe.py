# Databricks notebook source
# PIT probe: for 5 real impressions, compare viewer features at impression time (viewer_features_ts) vs today (viewer_features_current)
import os, sys
# Walk up to the repo root instead of assuming a depth. These notebooks sit in
# track folders (00_shared, 10_horizontal, ...), and the previous
# `os.getcwd()/".."` resolved to notebooks/ the moment one moved -- which fails as
# ModuleNotFoundError: src, from a line that looks like boilerplate.
_root = os.getcwd()
while _root != "/" and not os.path.isdir(os.path.join(_root, "src", "crfs")):
    _root = os.path.dirname(_root)
assert os.path.isdir(os.path.join(_root, "src", "crfs")), \
    f"src/crfs not found above {os.getcwd()} -- is the bundle's whole file tree synced?"
if _root not in sys.path:
    sys.path.insert(0, _root)
from src.crfs.config import Config

cfg = Config.from_widgets(dbutils)
CATALOG, SCHEMA = cfg.catalog, cfg.schema
spark.sql(f"USE {cfg.fq}")
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
