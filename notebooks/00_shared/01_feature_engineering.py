# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Feature engineering + the Lakebase Online Feature Store
# MAGIC
# MAGIC Turns raw `crunchyroll_demo` signals into governed features. Freshness is
# MAGIC chosen **per feature class**, never globally.
# MAGIC
# MAGIC | Feature table | PK | Freshness | Store |
# MAGIC |---|---|---|---|
# MAGIC | `viewer_features_ts` | viewer_id + ts | daily snapshots | offline only (point-in-time training) |
# MAGIC | `viewer_features_current` | viewer_id | latest | offline + **online (Lakebase)** |
# MAGIC | `title_features` | title_id | daily | offline + **online (Lakebase)** |
# MAGIC | `recent_behavior_current` | viewer_id | triggered refresh | offline + **online (Lakebase)** |
# MAGIC
# MAGIC One definition feeds both stores: historically correct training offline,
# MAGIC latest keyed values online. No rebuilt joins, no training-serving skew.
# MAGIC
# MAGIC Every definition lives in `src/crfs/features.py`, imported here and by
# MAGIC notebooks 05 and 10. That is not tidiness — the first version of this demo
# MAGIC re-derived the recent-behaviour maths separately in 01 and 05, which is
# MAGIC exactly the skew this demo argues against, committed in its own source.
# COMMAND ----------
# MAGIC %pip install databricks-feature-engineering "psycopg[binary]" --quiet
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
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

from src.crfs.config import Config, GENRES
from src.crfs import features as F
from src.crfs import online, ops

cfg = Config.from_widgets(dbutils)
spark.sql(f"USE {cfg.fq}")
print(cfg.describe())
# COMMAND ----------
import json
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.feature_engineering import FeatureEngineeringClient

w = WorkspaceClient()
fe = FeatureEngineeringClient()
# Postgres handle: unpublishing has to drop the Postgres table as well as the UC
# entry, or the next publish collides with a table UC can no longer see.
store_pg = online.from_config(w, cfg)

AS_OF = cfg.demo_now(spark)
print("computing features as of:", AS_OF)

events = spark.table(cfg.t("engagement_events")).toPandas()
titles = spark.table(cfg.t("titles")).toPandas()
print("events:", len(events), "| titles:", len(titles))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Viewer features — daily snapshots for point-in-time training
# MAGIC
# MAGIC Long-horizon signals recomputed **as of each day**, so training can ask what
# MAGIC we knew about this viewer at impression time rather than what we know now.
# MAGIC
# MAGIC Two columns exist purely to feed request-time features in notebook 06:
# MAGIC `typical_watch_hour` and `hour_concentration`. The habitual hour is a
# MAGIC **circular** mean — 23:00 and 01:00 are two hours apart, not twenty-two — so
# MAGIC it is rolled up by summing sin and cos and taking `atan2`.
# COMMAND ----------
viewer_ts = F.build_viewer_timeseries(events, titles)
viewer_current = F.viewer_current_from_ts(viewer_ts)
print("viewer_features_ts:", viewer_ts.shape)
print("viewer_features_current:", viewer_current.shape)
display(spark.createDataFrame(
    viewer_current[["viewer_id", "minutes_watched_7d", "typical_watch_hour",
                    "hour_concentration", "genre_affinity_action", "genre_affinity_sci_fi"]].head(5)))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Title features — catalog metadata plus derived popularity
# COMMAND ----------
title_features = F.build_title_features(titles, events, AS_OF)
print("title_features:", title_features.shape)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Recent behaviour — the freshness-sensitive class
# MAGIC
# MAGIC `last_event_epoch_s` is here so the on-demand `cr_session_decay` UDF has
# MAGIC something to subtract the request clock from. It is an epoch BIGINT rather
# MAGIC than a timestamp so the app, the endpoint and the training set cannot
# MAGIC disagree about a timezone.
# COMMAND ----------
recent_behavior = F.build_recent_behavior(events, titles, AS_OF)
print("recent_behavior_current:", recent_behavior.shape)

# Daily snapshots of the same 24h features. The _current table holds one row per viewer,
# so a training lookup against it gives every historical label TODAY's behaviour -- which
# for a log of past impressions is leakage. The rail ranker looks this up point-in-time
# (src/crfs/rails.py::rail_lookups); the online copy is deduplicated to the latest row per
# key, so serving reads the same values _current would have given it.
recent_ts = F.build_recent_behavior_timeseries(
    events, titles, dates=[d.date() for d in viewer_ts["ts"].unique()])
print("recent_behavior_ts:", recent_ts.shape,
      f"({recent_ts['ts'].nunique()} daily snapshots)")
print("viewers with activity in the last 24h:",
      int((recent_behavior["minutes_watched_24h"] > 0).sum()))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Register the feature tables
# MAGIC
# MAGIC `upsert_feature_table` overwrites data in place and only drops when the
# MAGIC schema genuinely changed — and when it must drop, it deletes the published
# MAGIC online table first. The original notebook dropped unconditionally on every
# MAGIC run, which pulled the source out from under a live sync pipeline and turned
# MAGIC the next publish into a fifteen-attempt retry loop.
# COMMAND ----------
def upsert_feature_table(pdf, name, primary_keys, description, timeseries=None,
                         online_name=None):
    full = cfg.t(name)
    sdf = spark.createDataFrame(pdf)
    exists = spark.catalog.tableExists(full)

    if exists:
        current = set(spark.table(full).columns)
        wanted = set(sdf.columns)
        if current == wanted:
            fe.write_table(name=full, df=sdf, mode="merge")
            print(f"{full}: merged {sdf.count()} rows (schema unchanged)")
            return full
        print(f"{full}: schema changed, recreating. added={sorted(wanted - current)} "
              f"removed={sorted(current - wanted)}")
        if online_name:
            ops.drop_synced_if_exists(w, cfg.t(online_name), online_store=store_pg)
        spark.sql(f"DROP TABLE IF EXISTS {full}")

    kwargs = dict(name=full, primary_keys=primary_keys, df=sdf, description=description)
    if timeseries:
        kwargs["timeseries_column"] = timeseries
    fe.create_table(**kwargs)
    # Change Data Feed is the contract an online store needs: TRIGGERED and
    # CONTINUOUS publish modes both read the feed rather than rescanning.
    spark.sql(f"ALTER TABLE {full} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    print(f"{full}: created, {spark.table(full).count()} rows | PK={primary_keys} | ts={timeseries}")
    return full


upsert_feature_table(
    viewer_ts, "viewer_features_ts", ["viewer_id", "ts"],
    "Daily snapshots of long-horizon viewer features for point-in-time-correct training",
    timeseries="ts")
upsert_feature_table(
    viewer_current, "viewer_features_current", ["viewer_id"],
    "Latest long-horizon viewer features - mirrored to the online store",
    online_name="online_viewer_features")
upsert_feature_table(
    title_features, "title_features", ["title_id"],
    "Title catalog and derived popularity features - mirrored to the online store",
    online_name="online_title_features")
upsert_feature_table(
    recent_behavior, "recent_behavior_current", ["viewer_id"],
    "Last-24h viewer behaviour - the freshness-sensitive class, mirrored to the online store",
    online_name="online_recent_behavior")
upsert_feature_table(
    recent_ts, "recent_behavior_ts", ["viewer_id", "ts"],
    "Daily snapshots of last-24h viewer behaviour - the point-in-time training source",
    timeseries="ts", online_name="online_recent_behavior_ts")
# COMMAND ----------
# MAGIC %md
# MAGIC ## The Lakebase-backed Online Feature Store
# MAGIC
# MAGIC `create_online_store` provisions a managed Lakebase Postgres project. The
# MAGIC capacity class governs the backing endpoint's compute floor: `CU_1` gives a
# MAGIC 4–8 CU endpoint, `CU_2` gives 8–16.
# MAGIC
# MAGIC Online stores **cannot scale to zero** — this is the one always-on cost in
# MAGIC the demo. Notebook 13 reports what it actually bills.
# COMMAND ----------
store = fe.get_online_store(name=cfg.online_store)
if store is None:
    store = fe.create_online_store(name=cfg.online_store, capacity=cfg.extras.get("online_capacity", "CU_1"))
    print("created online store:", cfg.online_store)
print("online store:", store)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Publish online — and wait on the real sync state, not on a sleep
# MAGIC
# MAGIC `publish_table(publish_mode="TRIGGERED")` stands up a sync pipeline per
# MAGIC table — but it is a *create*, not an upsert: called twice it fails with
# MAGIC `AlreadyExists`. `ops.publish_or_refresh` publishes the first time and
# MAGIC starts an update on the existing pipeline every time after, so re-running
# MAGIC this notebook is safe.
# MAGIC
# MAGIC Rather than sleeping and hoping, `ops.wait_for_sync` polls
# MAGIC `GET /api/2.0/database/synced_tables/{name}` until the pipeline reports it
# MAGIC has processed at least the source Delta commit we just wrote.
# MAGIC
# MAGIC Say:
# MAGIC > "Offline holds full history for training and backfills. Online holds only
# MAGIC > the serving-critical current values, keyed for lookup. Same definitions,
# MAGIC > two destinations — no rebuilt joins, no skew. And the online store is
# MAGIC > Lakebase: managed Postgres, built for frequent small upserts."
# COMMAND ----------
PUBLISH = [
    ("viewer_features_current", "online_viewer_features"),
    ("title_features", "online_title_features"),
    ("recent_behavior_current", "online_recent_behavior"),
    # The point-in-time tables are published too, because the rail ranker's feature spec
    # resolves THEM at serving time now (see rails.rail_lookups). Publishing a time series
    # table deduplicates it to the latest row per key -- the same behaviour
    # viewer_rail_features_ts already relies on -- so the online copy carries one row per
    # viewer and the values match what the _current tables hold.
    ("viewer_features_ts", "online_viewer_features_ts"),
    ("recent_behavior_ts", "online_recent_behavior_ts"),
]

published = []
for src, dst in PUBLISH:
    src_full, dst_full = cfg.t(src), cfg.t(dst)
    action = ops.publish_or_refresh(w, fe, cfg.online_store, src_full, dst_full,
                                    publish_mode="TRIGGERED", online_store=store_pg)
    published.append((src_full, dst_full, action))
# COMMAND ----------
summaries = []
for src_full, dst_full, action in published:
    if action == "published":
        # First publish: wait for the initial sync to reach the commit we wrote.
        version = ops.source_commit_version(spark, src_full)
        print(f"\nwaiting for {dst_full} to reach source commit {version}")
        summaries.append(ops.wait_for_sync(w, dst_full, min_commit_version=version, timeout_s=600))
    else:
        # A refresh has already been triggered; wait for a NEW sync to complete.
        # Waiting on the commit version alone can be satisfied by the previous
        # sync, which lets a caller read stale online values.
        version = ops.source_commit_version(spark, src_full)
        print(f"\nwaiting for a new sync of {dst_full} (or confirmation it already "
              f"covers source commit {version})")
        summaries.append(ops.refresh_and_wait(
            w, dst_full, min_commit_version=version, trigger=False, timeout_s=600))

for s in summaries:
    print(f"{s['name']}: {s['detailed_state']} | processed_commit={s['last_processed_commit_version']} "
          f"| sync_end={s['sync_end']}")
# COMMAND ----------
for _, dst_full, _ in published:
    print(f"online {dst_full}: {spark.table(dst_full).count()} rows")
# COMMAND ----------
store_pg.close()
dbutils.notebook.exit(json.dumps({
    "as_of": str(AS_OF),
    "online_store": cfg.online_store,
    "feature_tables": ["viewer_features_ts", "viewer_features_current",
                       "title_features", "recent_behavior_current",
                       "recent_behavior_ts"],
    "online_tables": [{"table": d, "action": a} for _, d, a in published],
    "viewer_feature_cols": F.VIEWER_FEATURE_COLS,
    "sync": [{k: s.get(k) for k in ("name", "detailed_state", "last_processed_commit_version")}
             for s in summaries],
}))
