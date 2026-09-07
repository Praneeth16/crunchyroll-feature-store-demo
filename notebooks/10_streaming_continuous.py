# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Streaming freshness — from events to ranked scores in seconds
# MAGIC
# MAGIC This notebook demonstrates the streaming freshness story: Structured
# MAGIC Streaming aggregates engagement events into session features, `foreachBatch`
# MAGIC writes them into a feature table with `merge` semantics, and `CONTINUOUS`
# MAGIC publish keeps the online store fresh. This is the production ingest pattern
# MAGIC for real-time features.
# MAGIC
# MAGIC **Key differences from the old notebook 05:**
# MAGIC - Notebook 05 used TRIGGERED re-publish-per-write (no streaming).
# MAGIC - This notebook uses CONTINUOUS publish + streaming aggregation.
# MAGIC - CONTINUOUS mode is a pipeline, not a one-shot Spark job — it picks up
# MAGIC   Delta table changes automatically and publishes them.
# MAGIC - The same feature definition (`session_aggregate`) works both offline
# MAGIC   and streaming without duplication.
# MAGIC
# MAGIC **Say:**
# MAGIC > "Zerobus writes directly to Delta. Spark Structured Streaming reads that
# MAGIC > table as a stream, aggregates to session features, and writes with
# MAGIC > `foreachBatch` into the feature table. Lakebase publishes CONTINUOUSLY,
# MAGIC > so every commit to the feature table automatically lands in Postgres.
# MAGIC > That's our production pipeline."
# COMMAND ----------
# MAGIC %pip install databricks-sdk --quiet
# COMMAND ----------
# MAGIC %pip install databricks-feature-engineering --quiet
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import os, sys, time, json
import pandas as pd
import numpy as np

_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _root not in sys.path:
    sys.path.insert(0, _root)

from src.crfs.config import Config
from src.crfs import features, ops, online
from databricks.sdk import WorkspaceClient
from databricks.feature_engineering import FeatureEngineeringClient

cfg = Config.from_widgets(dbutils, extra_widgets={
    "keep_running": "false"
})

w = WorkspaceClient()
fe = FeatureEngineeringClient()

spark.sql(f"USE {cfg.fq}")

print(f"\nConfig:")
print(cfg.describe())

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup: create volumes and feature table infrastructure

# Ensure the volume exists
try:
    w.volumes.get_by_name(cfg.catalog, cfg.schema, cfg.volume)
    print(f"✓ Volume {cfg.volume} exists")
except Exception:
    w.volumes.create(
        name=cfg.volume,
        catalog_name=cfg.catalog,
        schema_name=cfg.schema
    )
    print(f"✓ Created volume {cfg.volume}")

# Create the checkpoint directory location if needed
spark.sql(f"""
CREATE DIRECTORY IF NOT EXISTS '{cfg.checkpoint("session_features")}'
""")

# Create session_features_current table if missing (feature table, PK=viewer_id, CDF=true)
try:
    spark.sql(f"DESCRIBE TABLE {cfg.t('session_features_current')}")
    print(f"✓ Feature table {cfg.t('session_features_current')} exists")
except Exception:
    print(f"Creating feature table {cfg.t('session_features_current')}...")
    spark.sql(f"""
    CREATE TABLE {cfg.t('session_features_current')} (
        viewer_id STRING NOT NULL,
        session_seconds DOUBLE,
        session_skips INT,
        session_events INT,
        last_event_epoch_s LONG,
        src_event_epoch_ms LONG
    )
    USING DELTA
    TBLPROPERTIES (
        'primary_key' = 'viewer_id',
        'delta.enableChangeDataFeed' = 'true'
    )
    """)
    print(f"✓ Created feature table {cfg.t('session_features_current')}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Start the Structured Streaming pipeline

print(f"\n[{time.strftime('%H:%M:%S')}] Starting Structured Streaming pipeline...")
print(f"  Source: {cfg.t('engagement_events_stream')}")
print(f"  Sink: {cfg.t('session_features_current')} (merge)")
print(f"  Checkpoint: {cfg.checkpoint('session_features')}")

# Ensure we start fresh for this demo
spark.sql(f"DELETE FROM {cfg.checkpoint('session_features')}")

from pyspark.sql import functions as F

# Read the stream
stream_df = spark.readStream.table(cfg.t("engagement_events_stream"))

# Apply session aggregation
agg_df = features.session_aggregate(stream_df, watermark="10 minutes")

# Define the merge write logic
def merge_write(bdf, _):
    """Merge batch into the feature table using fe.write_table."""
    fe.write_table(
        name=cfg.t("session_features_current"),
        df=bdf,
        mode="merge"
    )

# Create the stream with foreachBatch
query = (agg_df
    .writeStream
    .foreachBatch(merge_write)
    .option("checkpointLocation", cfg.checkpoint("session_features"))
    .trigger(processingTime="5 seconds")
    .start())

print(f"✓ Stream started, query ID: {query.id}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Wait for the stream to process a few batches

time.sleep(3)
print(f"\n[{time.strftime('%H:%M:%S')}] Stream is processing...")
print(f"Status: {query.status}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Publish to Lakebase with CONTINUOUS mode (one-time)

print(f"\n[{time.strftime('%H:%M:%S')}] Publishing to Lakebase (CONTINUOUS mode)...")

# First, check if already published (if so, skip)
try:
    ops.sync_status(w, cfg.t("online_session_features"))
    print(f"✓ Online table already published, skipping publish_table call")
    publish_mode = "ALREADY_PUBLISHED"
except Exception:
    # Not published yet, so publish
    fe.publish_table(
        online_store=fe.get_online_store(name=cfg.online_store),
        source_table_name=cfg.t("session_features_current"),
        online_table_name=cfg.t("online_session_features"),
        publish_mode="CONTINUOUS",
    )
    print(f"✓ Published {cfg.t('session_features_current')} to {cfg.t('online_session_features')} (CONTINUOUS)")
    publish_mode = "CONTINUOUS"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Wait for initial sync to complete

print(f"\nWaiting for initial sync...")
try:
    ops.wait_for_sync(w, cfg.t("online_session_features"), timeout_s=120, poll_s=2)
    print(f"✓ Sync complete")
except TimeoutError as e:
    print(f"✗ Timeout waiting for sync: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Emit N burst events and measure freshness

n_events = 20
print(f"\n[{time.strftime('%H:%M:%S')}] Emitting {n_events} burst events to measure freshness...")

# Emit burst for a specific viewer
viewer_id = "v0001"
title_ids = spark.sql(f"""
    SELECT title_id FROM {cfg.t('titles')}
    WHERE primary_genre = 'sci_fi' AND is_simulcast
    ORDER BY intrinsic_popularity DESC LIMIT {n_events}
""").toPandas()["title_id"].tolist()

from datetime import datetime, timedelta
now = datetime.now()
burst_events = []
for i, tid in enumerate(title_ids):
    produced_ms = int(time.time() * 1000)
    burst_events.append({
        "event_id": f"freshness-{i}-{produced_ms}",
        "viewer_id": viewer_id,
        "title_id": tid,
        "event_ts": now - timedelta(minutes=(n_events - i - 1) * 2),
        "event_type": "complete",
        "watch_seconds": 1420.0,
        "surface": "post_play",
        "device": "tv",
        "locale": "en-US",
        "produced_epoch_ms": produced_ms
    })

df = spark.createDataFrame(pd.DataFrame(burst_events))
df.write.mode("append").insertInto(cfg.t("engagement_events_stream"))
print(f"✓ Emitted {n_events} events for {viewer_id}")

# Record the produced_epoch_ms values for latency measurement
produced_timestamps = [e["produced_epoch_ms"] for e in burst_events]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Measure freshness: keyed reads from the online store

print(f"\n[{time.strftime('%H:%M:%S')}] Measuring freshness with keyed reads...")

store = online.from_config(w, cfg)

# Collect latencies for all emitted events using wait_for_value
latencies = []
results = []

for i, produced_ms in enumerate(produced_timestamps):
    result = store.wait_for_value(
        table="online_session_features",
        key_col="viewer_id",
        key_val=viewer_id,
        watch_col="src_event_epoch_ms",
        at_least=produced_ms,
        timeout_s=120.0,
        poll_s=0.25,
        verbose=False
    )

    if result.get("reached"):
        latency_ms = result["latency_ms"]
        latencies.append(latency_ms)
        results.append({
            "event": i,
            "produced_ms": produced_ms,
            "observed_ms": result["observed"],
            "latency_ms": latency_ms,
            "polls": result["polls"]
        })
        print(f"  [{i:2d}] {latency_ms:.1f}ms (after {result['polls']} polls of {result['poll_interval_ms']:.0f}ms)")
    else:
        print(f"  [{i:2d}] ✗ timeout after {result['elapsed_s']}s ({result['polls']} polls)")

if latencies:
    latencies_sorted = sorted(latencies)
    p50 = latencies_sorted[int(len(latencies) * 0.50)]
    p95 = latencies_sorted[int(len(latencies) * 0.95)]

    print(f"\n=== FRESHNESS MEASUREMENT ===")
    print(f"Events measured: {len(latencies)}/{n_events}")
    print(f"Poll interval: 500ms")
    print(f"E2E p50: {p50:.1f}ms")
    print(f"E2E p95: {p95:.1f}ms")
    print(f"Min: {min(latencies):.1f}ms, Max: {max(latencies):.1f}ms")
else:
    print(f"⚠ No latency measurements yet (stream may not have caught up)")
    p50 = None
    p95 = None

# COMMAND ----------
# MAGIC %md
# MAGIC ## Show the sync state from the platform

print(f"\n=== PLATFORM SYNC STATE ===")
sync_state = ops.sync_summary(w, cfg.t("online_session_features"))
print(json.dumps(sync_state, indent=2, default=str))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Measure keyed-read latency from Postgres (the serving path)

print(f"\n=== POSTGRES KEYED-READ LATENCY ===")
print(f"Measuring latency from notebook driver (same region)...")

# Get a set of viewer IDs to warm up + measure
viewer_ids = spark.sql(f"SELECT DISTINCT viewer_id FROM {cfg.t('online_session_features')} LIMIT 30").toPandas()["viewer_id"].tolist()

if len(viewer_ids) > 0:
    keyed_lat = store.keyed_read_latency(
        table="online_session_features",
        key_col="viewer_id",
        key_vals=viewer_ids,
        warmup=2
    )
    print(f"Keyed-read latency (Postgres direct):")
    print(f"  n={keyed_lat.get('n')}, p50={keyed_lat.get('p50_ms')}ms, p95={keyed_lat.get('p95_ms')}ms")
    print(f"  min={keyed_lat.get('min_ms')}ms, max={keyed_lat.get('max_ms')}ms")
else:
    print(f"⚠ No viewer IDs available yet")
    keyed_lat = {}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Compare: Spark SQL read (the wrong measurement)

print(f"\n=== SPARK SQL READ (NOT THE SERVING PATH) ===")
print(f"For reference: what notebook 04 used to report as 'online latency'...")

t0 = time.time()
result = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM {cfg.t('online_session_features')}
    WHERE viewer_id = '{viewer_id}'
""").collect()[0]["cnt"]
spark_read_ms = (time.time() - t0) * 1000

print(f"Spark SQL read (full table scan + filter): {spark_read_ms:.1f}ms")
print(f"This measures serverless SQL planning + federated read, NOT the serving path.")
print(f"The keyed-read latency (Postgres) is what matters for serving.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Stop the stream (do NOT leave it running and billing)

keep_running = cfg.extras.get("keep_running", "false").lower() == "true"

if keep_running:
    print(f"\n⚠ keep_running=true: stream still active (will bill until manually stopped)")
    print(f"To stop: query.stop() or restart the notebook without keep_running")
else:
    print(f"\n[{time.strftime('%H:%M:%S')}] Stopping stream (keep_running=false)...")
    query.stop()
    time.sleep(2)
    print(f"✓ Stream stopped")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Exit with results

result = {
    "online_table": cfg.t("online_session_features"),
    "source_table": cfg.t("session_features_current"),
    "publish_mode": publish_mode,
    "n_events_burst": n_events,
    "n_events_measured": len(latencies),
    "e2e_p50_ms": round(p50, 1) if p50 is not None else None,
    "e2e_p95_ms": round(p95, 1) if p95 is not None else None,
    "poll_interval_ms": 500,
    "keyed_read_p50_ms": keyed_lat.get("p50_ms"),
    "keyed_read_p95_ms": keyed_lat.get("p95_ms"),
    "keyed_read_n": keyed_lat.get("n"),
    "spark_sql_read_ms": round(spark_read_ms, 1),
    "sync_state": {
        "detailed_state": sync_state.get("detailed_state"),
        "delta_commit_timestamp": str(sync_state.get("delta_commit_timestamp")),
        "sync_end": str(sync_state.get("sync_end")),
    },
    "stream_status": str(query.status),
}

print(f"\n=== FINAL RESULTS ===")
print(json.dumps(result, indent=2))

dbutils.notebook.exit(json.dumps(result))
