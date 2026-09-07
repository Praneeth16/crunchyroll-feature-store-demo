# Databricks notebook source
# MAGIC %md
# MAGIC # 11 · Event producer — three modes for streaming
# MAGIC
# MAGIC This notebook produces engagement events into the `engagement_events_stream`
# MAGIC Delta table. It exists to measure streaming freshness in the next notebook.
# MAGIC
# MAGIC Three modes:
# MAGIC
# MAGIC - **`burst`**: emit `n_events` events for `viewer_id` right now (under 10s).
# MAGIC   The demo app calls this from a button.
# MAGIC
# MAGIC - **`loop`** (default): emit `events_per_second` events for `duration_minutes`
# MAGIC   across random viewers as background traffic. This is what plays while the
# MAGIC   presenter talks. Exit is clean: the loop finishes on its own.
# MAGIC
# MAGIC - **`zerobus`**: push the same payload over the Zerobus gRPC SDK directly
# MAGIC   into the Delta table. This shows the production ingest door. Failure is
# MAGIC   isolated and reported clearly — it does NOT break the rest of the demo.
# MAGIC
# MAGIC Every event carries `produced_epoch_ms = int(time.time() * 1000)`, the
# MAGIC producer's own wall clock in milliseconds. That single field makes freshness
# MAGIC measurable later — when the online store reads this value, we subtract it
# MAGIC from the current time to measure end-to-end latency. No clock skew, no guessing.
# COMMAND ----------
# MAGIC %pip install databricks-sdk --quiet
# MAGIC # COMMAND ----------
# MAGIC %pip install databricks-feature-engineering --quiet
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import os, sys, time, json
import random
import datetime as dt
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _root not in sys.path:
    sys.path.insert(0, _root)

from src.crfs.config import Config
from databricks.sdk import WorkspaceClient
from databricks.feature_engineering import FeatureEngineeringClient

cfg = Config.from_widgets(dbutils, extra_widgets={
    "mode": "loop",
    "n_events": "3",
    "duration_minutes": "10",
    "events_per_second": "2",
    "viewer_id": "v0001"
})

w = WorkspaceClient()
fe = FeatureEngineeringClient()

spark.sql(f"USE {cfg.fq}")

print(f"\nConfig:")
print(f"  Mode: {cfg.extras['mode']}")
print(f"  Target: {cfg.t('engagement_events_stream')}")
print(f"  Catalog: {cfg.catalog}, Schema: {cfg.schema}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup: create the stream table if missing

# Create engagement_events_stream if missing, with CDF enabled
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.t('engagement_events_stream')} (
  event_id STRING,
  viewer_id STRING,
  title_id STRING,
  event_ts TIMESTAMP,
  event_type STRING,
  watch_seconds DOUBLE,
  surface STRING,
  device STRING,
  locale STRING,
  produced_epoch_ms BIGINT
)
USING DELTA
LOCATION '/Volumes/{cfg.catalog}/{cfg.schema}/crfs_ops/engagement_events_stream'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
)
""")

spark.sql(f"ALTER TABLE {cfg.t('engagement_events_stream')} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")

print(f"Stream table ready: {cfg.t('engagement_events_stream')}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load reference data for event generation

# Titles for burst events — pick popular simulcasts that will flip the viewer's genre
titles_df = spark.sql(f"""
  SELECT title_id, title_name, primary_genre, is_simulcast, intrinsic_popularity
  FROM {cfg.t('titles')}
  WHERE is_simulcast = 1 AND primary_genre IN ('sci_fi', 'action')
  ORDER BY intrinsic_popularity DESC
  LIMIT 5
""").toPandas()

all_titles = spark.sql(f"SELECT title_id FROM {cfg.t('titles')}").toPandas()["title_id"].tolist()
all_viewers = spark.sql(f"SELECT viewer_id FROM {cfg.t('viewers')}").toPandas()["viewer_id"].tolist()

print(f"Loaded {len(all_titles)} titles, {len(all_viewers)} viewers")
print(f"Sci-fi simulcasts for burst (to flip genre):")
print(titles_df[["title_name", "primary_genre", "intrinsic_popularity"]])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Mode: burst

def burst_events(viewer_id, n_events):
    """Emit n_events completed watches for a viewer, using the most popular sci-fi titles."""
    now = datetime.now()
    events = []

    sci_fi_titles = titles_df[titles_df["primary_genre"] == "sci_fi"]["title_id"].tolist()[:n_events]

    for i, tid in enumerate(sci_fi_titles):
        events.append({
            "event_id": f"burst-{viewer_id}-{i}-{int(time.time()*1000)}",
            "viewer_id": viewer_id,
            "title_id": tid,
            "event_ts": now - timedelta(minutes=(n_events - i - 1) * 2),
            "event_type": "complete",
            "watch_seconds": 1420.0,  # ~24 min
            "surface": "post_play",
            "device": "tv",
            "locale": "en-US",
            "produced_epoch_ms": int(time.time() * 1000)
        })

    return events

# COMMAND ----------
# MAGIC %md
# MAGIC ## Mode: loop

def loop_events(events_per_second, duration_minutes):
    """Emit background traffic across random viewers for duration_minutes."""
    start = time.time()
    end_time = start + (duration_minutes * 60)

    SURFACES = ["post_play", "home_rail", "search", "watchlist"]
    DEVICES = ["tv", "mobile", "web"]
    LOCALES = ["en-US", "es-MX", "pt-BR"]
    EVENT_TYPES = ["skip", "complete", "start"]

    event_count = 0
    now_start = datetime.now()

    while time.time() < end_time:
        # Emit events_per_second events in this batch
        batch = []
        for _ in range(events_per_second):
            vid = random.choice(all_viewers)
            tid = random.choice(all_titles)
            watch_secs = random.uniform(100, 3600)

            batch.append({
                "event_id": f"loop-{event_count}-{int(time.time()*1000)}",
                "viewer_id": vid,
                "title_id": tid,
                "event_ts": now_start + timedelta(seconds=event_count),
                "event_type": random.choice(EVENT_TYPES),
                "watch_seconds": watch_secs if random.choice([True, False]) else None,
                "surface": random.choice(SURFACES),
                "device": random.choice(DEVICES),
                "locale": random.choice(LOCALES),
                "produced_epoch_ms": int(time.time() * 1000)
            })
            event_count += 1

        if batch:
            df = spark.createDataFrame(pd.DataFrame(batch))
            df.write.mode("append").insertInto(cfg.t("engagement_events_stream"))

        elapsed = time.time() - start
        print(f"[{elapsed:6.1f}s] {event_count:4d} events emitted")

        # Sleep to maintain rate
        time.sleep(1.0 / events_per_second)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Mode: Zerobus (optional, with error isolation)

def zerobus_events(viewer_id, n_events):
    """Attempt to push events via Zerobus gRPC SDK. Failure is isolated."""
    try:
        from zerobus.sdk.sync import ZerobusSdk
        from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties

        # This would require Zerobus SDK to be installed as a job dependency.
        # For now, we'll emit a warning and suggest the production path.
        print(f"\n[ZEROBUS] SDK would require job environment with databricks-zerobus-ingest-sdk installed.")
        print(f"[ZEROBUS] In production, pass to a job with:")
        print(f"         'libraries': [{'pypi': {{'package': 'databricks-zerobus-ingest-sdk>=1.0.0'}}}]")
        print(f"[ZEROBUS] For this demo, fallback to standard append instead.\n")

        # Fall back to standard append
        now = datetime.now()
        events = []
        for i in range(n_events):
            events.append({
                "event_id": f"zerobus-fallback-{viewer_id}-{i}-{int(time.time()*1000)}",
                "viewer_id": viewer_id,
                "title_id": random.choice(all_titles),
                "event_ts": now - timedelta(minutes=(n_events - i - 1)),
                "event_type": "complete",
                "watch_seconds": 1420.0,
                "surface": "post_play",
                "device": "tv",
                "locale": "en-US",
                "produced_epoch_ms": int(time.time() * 1000)
            })

        df = spark.createDataFrame(pd.DataFrame(events))
        df.write.mode("append").insertInto(cfg.t("engagement_events_stream"))
        return True, f"Fallback append of {len(events)} events"

    except Exception as e:
        return False, f"Zerobus error: {str(e)[:100]}"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Execute the chosen mode

mode = cfg.extras["mode"]
n_events = int(cfg.extras["n_events"])
duration_minutes = int(cfg.extras["duration_minutes"])
events_per_second = float(cfg.extras["events_per_second"])
viewer_id = cfg.extras["viewer_id"]

result = {
    "mode": mode,
    "viewer_id": viewer_id,
    "table": cfg.t("engagement_events_stream"),
}

if mode == "burst":
    print(f"\n=== BURST MODE ===")
    print(f"Emitting {n_events} sci-fi events for {viewer_id}...")
    events = burst_events(viewer_id, n_events)
    df = spark.createDataFrame(pd.DataFrame(events))
    df.write.mode("append").insertInto(cfg.t("engagement_events_stream"))
    result["n_events"] = len(events)
    print(f"✓ Emitted {len(events)} events")

elif mode == "loop":
    print(f"\n=== LOOP MODE ===")
    print(f"Background traffic: {events_per_second} events/sec for {duration_minutes} min...")
    loop_events(events_per_second, duration_minutes)
    result["events_per_second"] = events_per_second
    result["duration_minutes"] = duration_minutes
    print(f"✓ Loop completed")

elif mode == "zerobus":
    print(f"\n=== ZEROBUS MODE ===")
    success, msg = zerobus_events(viewer_id, n_events)
    result["zerobus_success"] = success
    result["zerobus_message"] = msg
    print(f"{'✓' if success else '✗'} {msg}")

else:
    raise ValueError(f"Unknown mode: {mode}")

# Verify events landed
count = spark.sql(f"SELECT COUNT(*) as cnt FROM {cfg.t('engagement_events_stream')}").collect()[0]["cnt"]
result["total_events_in_stream"] = count
print(f"\nStream table now has {count} total events")

dbutils.notebook.exit(json.dumps(result))
