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
# MAGIC %pip install databricks-feature-engineering "psycopg[binary]" --quiet
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import os, sys, time, json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

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
from src.crfs import features, ops, online
from databricks.sdk import WorkspaceClient
from databricks.feature_engineering import FeatureEngineeringClient

cfg = Config.from_widgets(dbutils, extra_widgets={
    "keep_running": "false", "reset_checkpoint": "false", "n_cycles": "8"})

w = WorkspaceClient()
fe = FeatureEngineeringClient()

spark.sql(f"USE {cfg.fq}")

print(f"\nConfig:")
print(cfg.describe())

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup: create volumes and feature table infrastructure

# COMMAND ----------
# The checkpoint volume is declared in the bundle (resources/storage.yml), so it
# already exists. Verify rather than create: w.volumes.get_by_name does not exist in
# this SDK and w.volumes.create() requires volume_type, so the previous attempt threw
# AttributeError then TypeError.
VOLUME_FQ = f"{cfg.catalog}.{cfg.schema}.{cfg.volume}"
try:
    spark.sql(f"DESCRIBE VOLUME {VOLUME_FQ}")
    print(f"volume {VOLUME_FQ} present")
except Exception:
    print(f"volume {VOLUME_FQ} missing - creating it")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUME_FQ}")

CHECKPOINT = cfg.checkpoint("session_features")
print("checkpoint:", CHECKPOINT)

# The session feature table must be created through the Feature Engineering client.
# A plain CREATE TABLE with TBLPROPERTIES ('primary_key' = ...) is NOT a feature
# table -- the property is inert, fe.write_table has nothing to merge on, and
# publish_table cannot attach an online table to it.
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()
SESSION = cfg.t("session_features_current")

if spark.catalog.tableExists(SESSION):
    print(f"feature table {SESSION} exists")
else:
    empty = spark.createDataFrame(
        [], "viewer_id STRING, session_seconds DOUBLE, session_skips INT, "
            "session_events INT, last_event_epoch_s LONG, src_event_epoch_ms LONG")
    fe.create_table(
        name=SESSION,
        primary_keys=["viewer_id"],
        df=empty,
        description="Live per-viewer session features, maintained by a streaming "
                    "aggregate and synced to Lakebase with publish_mode=CONTINUOUS")
    # CDF is the contract CONTINUOUS publish reads from.
    spark.sql(f"ALTER TABLE {SESSION} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    print(f"created feature table {SESSION}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Start the Structured Streaming pipeline

# COMMAND ----------
print(f"\n[{time.strftime('%H:%M:%S')}] Starting Structured Streaming pipeline...")
print(f"  Source: {cfg.t('engagement_events_stream')}")
print(f"  Sink: {cfg.t('session_features_current')} (merge)")
print(f"  Checkpoint: {cfg.checkpoint('session_features')}")

# Optionally start from a clean checkpoint. Deleting a checkpoint replays the
# source from the beginning, so it is opt-in rather than the default.
if cfg.extras.get("reset_checkpoint", "false").lower() == "true":
    try:
        dbutils.fs.rm(CHECKPOINT, recurse=True)
        print("cleared checkpoint", CHECKPOINT)
    except Exception as e:
        print("no checkpoint to clear:", str(e)[:120])

from pyspark.sql import functions as F

# Read the stream
stream_df = spark.readStream.table(cfg.t("engagement_events_stream"))

# Apply session aggregation
agg_df = features.session_aggregate(stream_df, watermark="10 minutes")

# Define the merge write logic
SESSION_TABLE = SESSION  # plain string; cfg is not serializable into foreachBatch


def merge_write(batch_df, batch_id):
    """Upsert one micro-batch into the session feature table.

    This runs in a SEPARATE Python process that has no Databricks credentials, so it
    cannot build an SDK-backed client. `FeatureEngineeringClient()` here dies with

      ValueError: default auth: cannot configure default credentials

    inside the foreachBatch worker. fe.write_table is therefore unavailable, and the
    upsert is a plain Delta MERGE via the batch's own session. That is the same
    operation fe.write_table(mode="merge") performs; the table stays a registered
    feature table with Change Data Feed on, so the CONTINUOUS publish keeps syncing it
    to Lakebase exactly as before.
    """
    if batch_df.isEmpty():
        return
    # DeltaTable's merge builder, not a temp view + SQL: serverless rejects
    #   [NOT_SUPPORTED_WITH_SERVERLESS] GLOBAL TEMPORARY VIEW is not supported
    # and a session-local view is awkward across the cloned foreachBatch session.
    from delta.tables import DeltaTable

    (DeltaTable.forName(batch_df.sparkSession, SESSION_TABLE)
        .alias("t")
        .merge(batch_df.alias("s"), "t.viewer_id = s.viewer_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"  batch {batch_id}: merged {batch_df.count()} viewer row(s)")


# Serverless notebook compute rejects an infinite trigger:
#   INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED: Trigger type ProcessingTime is not
#   supported for this cluster type. Use a different trigger type e.g. AvailableNow, Once.
# So the aggregation runs as repeated availableNow micro-batches instead of one
# always-on query. Say this out loud in the demo -- it is a real platform constraint,
# not a shortcut:
#
#   * events -> Delta                : Zerobus, continuous
#   * Delta  -> session features      : availableNow micro-batches here; an always-on
#                                      query needs classic compute or a Lakeflow pipeline
#   * features -> Lakebase           : publish_mode="CONTINUOUS", a streaming pipeline
#                                      the platform runs for us, genuinely always-on
#
# The Lakebase leg -- the one this demo is about -- is continuous either way.
def drain_once(label: str = ""):
    """Process everything currently in the source, then stop."""
    q = (agg_df
         .writeStream
         # groupBy("viewer_id") with no time window is a non-windowed aggregation, so
         # append is rejected:
         #   STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION: Invalid streaming output mode:
         #   append. This output mode is not supported for streaming aggregations
         #   without watermark on streaming DataFrames/DataSets.
         # update emits only the rows that changed in the batch, which is exactly what
         # a merge into the feature table wants.
         .outputMode("update")
         .foreachBatch(merge_write)
         .option("checkpointLocation", CHECKPOINT)
         .trigger(availableNow=True)
         .start())
    q.awaitTermination()
    if label:
        print(f"  drained ({label})")
    return q


query = drain_once("initial")
print("initial aggregation complete")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Wait for the stream to process a few batches

# COMMAND ----------
print(f"\n[{time.strftime('%H:%M:%S')}] session features after the initial drain:")
print(f"  {spark.table(SESSION).count()} viewer row(s)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Publish to Lakebase with CONTINUOUS mode (one-time)

# COMMAND ----------
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

# COMMAND ----------
print(f"\nWaiting for initial sync...")
try:
    ops.wait_for_sync(w, cfg.t("online_session_features"), timeout_s=120, poll_s=2)
    print(f"✓ Sync complete")
except TimeoutError as e:
    print(f"✗ Timeout waiting for sync: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Emit N burst events and measure freshness

# COMMAND ----------
n_events = 20
print(f"\n[{time.strftime('%H:%M:%S')}] Emitting {n_events} burst events to measure freshness...")

# Emit burst for a specific viewer
viewer_id = "v0001"
title_ids = spark.sql(f"""
    SELECT title_id FROM {cfg.t('titles')}
    WHERE primary_genre = 'sci_fi' AND is_simulcast
    ORDER BY intrinsic_popularity DESC LIMIT {n_events}
""").toPandas()["title_id"].tolist()

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

# COMMAND ----------
# MAGIC %md
# MAGIC ## Measure freshness, one cycle at a time
# MAGIC
# MAGIC Each cycle is one honest end-to-end measurement: emit an event carrying its own
# MAGIC `produced_epoch_ms`, drain the aggregation, then poll the Lakebase row until that
# MAGIC value appears. Subtracting two readings of the producer's clock removes any
# MAGIC clock-skew argument, and the poll interval is reported because it quantises the
# MAGIC answer.
# MAGIC
# MAGIC The number therefore includes the `availableNow` drain, which on serverless
# MAGIC notebook compute stands in for an always-on query. Read it as "event to online
# MAGIC value in this configuration", not as the platform's floor.

# COMMAND ----------
store = online.from_config(w, cfg)

N_CYCLES = int(cfg.extras.get("n_cycles", "8"))
latencies, results = [], []

for i in range(N_CYCLES):
    produced_ms = int(time.time() * 1000)
    one = [{
        "event_id": f"cycle-{produced_ms}-{i}",
        "viewer_id": viewer_id,
        "title_id": burst_events[i % len(burst_events)]["title_id"],
        "event_ts": datetime.now(),
        "event_type": "complete",
        "watch_seconds": 1400.0 + i,
        "surface": "post_play",
        "device": "tv",
        "locale": "en-US",
        "produced_epoch_ms": produced_ms,
    }]
    (spark.createDataFrame(one)
          .write.mode("append").saveAsTable(cfg.t("engagement_events_stream")))

    drain_once()

    result = store.wait_for_value(
        table="online_session_features",
        key_col="viewer_id",
        key_val=viewer_id,
        watch_col="src_event_epoch_ms",
        at_least=produced_ms,
        timeout_s=180.0,
        poll_s=0.25,
        verbose=False,
    )
    if result.get("reached"):
        latencies.append(result["latency_ms"])
        results.append({"cycle": i, "produced_ms": produced_ms,
                        "latency_ms": result["latency_ms"], "polls": result["polls"]})
        print(f"  [{i:2d}] {result['latency_ms']:.0f} ms "
              f"({result['polls']} polls of {result['poll_interval_ms']:.0f} ms)")
    else:
        print(f"  [{i:2d}] timeout after {result['elapsed_s']}s ({result['polls']} polls)")

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

# COMMAND ----------
print(f"\n=== PLATFORM SYNC STATE ===")
sync_state = ops.sync_summary(w, cfg.t("online_session_features"))
print(json.dumps(sync_state, indent=2, default=str))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Measure keyed-read latency from Postgres (the serving path)

# COMMAND ----------
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

# COMMAND ----------
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

# COMMAND ----------
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

# COMMAND ----------
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
