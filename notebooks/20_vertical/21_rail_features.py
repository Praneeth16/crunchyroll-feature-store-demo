# Databricks notebook source
# MAGIC %md
# MAGIC # 21 · Rail features — two new tables, four reused
# MAGIC
# MAGIC The point of this notebook is how little it adds. Vertical ranking needs
# MAGIC exactly two feature tables the horizontal model does not have; everything else
# MAGIC it scores on is already governed, already published, already serving the
# MAGIC watch-next ranker.
# MAGIC
# MAGIC | Feature table | PK | Offline | Online | Shared with horizontal ranking? |
# MAGIC |---|---|---|---|---|
# MAGIC | `viewer_features_current` | viewer_id | latest | **yes** | **yes — unchanged** |
# MAGIC | `recent_behavior_current` | viewer_id | triggered | **yes** | **yes — unchanged** |
# MAGIC | `session_features_current` | viewer_id | streaming | **yes (CONTINUOUS)** | **yes — unchanged** |
# MAGIC | `title_features` | title_id | daily | **yes** | **yes — aggregated into the rail content stats** |
# MAGIC | `rail_features` | rail_id | daily | **yes** | new |
# MAGIC | `viewer_rail_features_ts` | viewer_id + rail_id (+ ts) | daily snapshots | **yes — latest per key** | new |
# MAGIC
# MAGIC That is the shared-feature-store claim made concrete: a second ranking model,
# MAGIC a different grain, a different label — and four of the six inputs are the same
# MAGIC governed tables with no second pipeline behind them.
# MAGIC
# MAGIC ## One table, not two
# MAGIC
# MAGIC `viewer_rail_features_ts` is a **time series feature table**, and it is
# MAGIC published online directly. That is not a shortcut — it is the documented
# MAGIC behaviour, and it was verified on this workspace before this notebook was
# MAGIC written:
# MAGIC
# MAGIC * offline the table holds every daily snapshot, so training does a real
# MAGIC   point-in-time join and reads only what was known at impression time;
# MAGIC * publishing it to the online store **deduplicates to the latest row per
# MAGIC   primary key** — verified: 16 offline rows across 4 keys became 4 online rows,
# MAGIC   each holding the newest snapshot. The synced table's spec came back as
# MAGIC   `primary_key_columns=[viewer_id, rail_id]`, `timeseries_key=ts`.
# MAGIC
# MAGIC So there is no `viewer_rail_current` mirror. Training and serving read the same
# MAGIC table through the same `FeatureLookup`, which is the strongest form of the
# MAGIC training-serving consistency argument: not "two tables built from one
# MAGIC definition", but one table.
# MAGIC
# MAGIC (The horizontal ranker still uses the older `_ts` + `_current` pair. Collapsing
# MAGIC it the same way is a real recommendation, written up in
# MAGIC `docs/vertical_ranking.md` rather than done here — rebuilding the horizontal
# MAGIC path is not this notebook's job.)
# MAGIC
# MAGIC **`viewer_rail_features_ts` is a composite-key online lookup** (viewer × rail).
# MAGIC A homepage request scores 14–16 rails for one viewer — measured; the eligible set
# MAGIC varies because four rails depend on viewer state — so the endpoint issues one
# MAGIC keyed read per candidate rail against this table. Notebook 25 measures what that
# MAGIC costs, per rail.
# COMMAND ----------
# MAGIC %pip install databricks-feature-engineering --quiet
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

from src.crfs.config import Config
from src.crfs import rails as R
from src.crfs import ops

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

AS_OF = cfg.demo_now(spark)
print("computing rail features as of:", AS_OF)

# Small frames only. The homepage log itself is never pulled to the driver --
# `R.rail_audience` aggregates it in Spark and returns 16 rows. Doing the
# aggregation in pandas killed the serverless kernel here at 363k impressions,
# after the feature tables had already been written.
rails_pdf = spark.table(cfg.t("rails")).toPandas()
rail_titles_pdf = spark.table(cfg.t("rail_title_map")).toPandas()
# The feature table, not the raw `titles` table: the rail content stats below are
# aggregates of governed title features, which is what makes the reuse real rather
# than nominal (verification_log V57).
title_features_pdf = spark.table(cfg.t("title_features")).toPandas()
n_impressions = spark.table(cfg.t("rail_impressions")).count()
print(f"rails {len(rails_pdf)} | impressions {n_impressions:,} (aggregated in Spark)")
# COMMAND ----------
# MAGIC %md
# MAGIC ## `rail_features` — the rail itself
# MAGIC
# MAGIC Audience behaviour over the last 30 days plus the content the rail carries.
# MAGIC Sixteen rows, shared by every viewer, and the cheapest possible online lookup.
# MAGIC
# MAGIC Personalized rails (Continue Watching, Because You Watched, Watchlist, New
# MAGIC Episodes) carry no static title membership, so their content columns are zero
# MAGIC and `rail_is_personalized` is the flag that tells the model to read them that
# MAGIC way. That is a modelling decision, not missing data.
# COMMAND ----------
audience = R.rail_audience(spark, cfg.t("rail_impressions"), AS_OF)
print("last-30-day audience per rail, aggregated in Spark:")
print(audience.sort_values("rail_impressions_30d", ascending=False).to_string(index=False))

rail_features = R.build_rail_features(rails_pdf, rail_titles_pdf, title_features_pdf,
                                      audience)
print("rail_features:", rail_features.shape)
display(spark.createDataFrame(rail_features))
# COMMAND ----------
# MAGIC %md
# MAGIC ## `viewer_rail_features_ts` — how this viewer treats this rail, point in time
# MAGIC
# MAGIC Built in Spark rather than pandas: it is a range window over a dense
# MAGIC (viewer × rail × day) grid, which is 300 × 16 × 90 here and viewers × rails ×
# MAGIC days in production — the one part of the pipeline that would not survive real
# MAGIC cardinality in pandas.
# MAGIC
# MAGIC **Each snapshot is stamped at the end of the day it summarises.** A
# MAGIC point-in-time lookup for an impression on day D therefore resolves to the
# MAGIC snapshot built from day D−1 and earlier. Stamping at day-start would let a
# MAGIC training row read clicks that happened after it — a leak that inflates offline
# MAGIC AUC and vanishes the moment the model is served.
# COMMAND ----------
STAGE = cfg.t("_vr_ts_stage")
vr_ts = R.build_viewer_rail_timeseries(spark, cfg.t("rail_impressions"))
vr_ts.createOrReplaceTempView("vr_ts_stage_view")
spark.sql(f"CREATE OR REPLACE TABLE {STAGE} AS SELECT * FROM vr_ts_stage_view")
staged = spark.table(STAGE)

# viewer_rail is the only lookup that misses, so every feature column has to be DOUBLE
# and nullable (see the note in rails.VIEWER_RAIL_TS_SQL). Asserted rather than assumed:
# a BIGINT here produces an endpoint that returns an empty `Error ''` twenty minutes and
# two tasks later, and an edit to the source SQL that silently fails to apply looks
# exactly like an edit that worked.
_bad = [f.name for f in staged.schema.fields
        if f.name not in ("viewer_id", "rail_id", "ts")
        and f.dataType.simpleString() != "double"]
assert not _bad, (
    f"these viewer_rail feature columns are not DOUBLE: {_bad}. A lookup miss returns "
    f"NULL, which an integral column cannot represent, and serving fails with an empty "
    f"error. Check the CAST list in src/crfs/rails.VIEWER_RAIL_TS_SQL.")
print("all viewer_rail feature columns are DOUBLE and nullable")
print("viewer_rail_features_ts rows:", staged.count())
print("distinct (viewer, rail) pairs:", staged.select("viewer_id", "rail_id").distinct().count())
print("ts range:", staged.selectExpr("min(ts) lo", "max(ts) hi").first())
display(staged.orderBy("viewer_id", "rail_id", "ts").limit(8))
# COMMAND ----------
# MAGIC %md
# MAGIC ### The snapshots really do move
# MAGIC
# MAGIC If a viewer × rail row were constant over time, the point-in-time join in
# MAGIC notebook 22 would be decoration. This is the check that it is not.
# COMMAND ----------
movement = spark.sql(f"""
    SELECT viewer_id, rail_id,
           COUNT(DISTINCT vr_ctr_30d)  AS distinct_ctr_values,
           ROUND(MIN(vr_ctr_30d), 4)   AS min_ctr,
           ROUND(MAX(vr_ctr_30d), 4)   AS max_ctr,
           MAX(vr_impressions_30d)     AS peak_impressions_30d
    FROM {STAGE}
    GROUP BY viewer_id, rail_id
    HAVING COUNT(DISTINCT vr_ctr_30d) > 3
    ORDER BY distinct_ctr_values DESC
    LIMIT 8
""")
display(movement)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Register the feature tables
# MAGIC
# MAGIC Same upsert discipline as notebook 01: overwrite in place, and only drop when
# MAGIC the schema genuinely changed — dropping a source out from under a live sync
# MAGIC pipeline is what turns the next publish into a retry loop.
# COMMAND ----------
def upsert_feature_table(sdf, name, primary_keys, description, timeseries=None,
                         online_name=None):
    full = cfg.t(name)
    exists = spark.catalog.tableExists(full)

    if exists:
        # Compare (name, type), not just names. A type-only change -- BIGINT to DOUBLE,
        # say -- leaves the name set identical, so a names-only check takes the merge
        # path and the old types survive. That silently defeated a fix whose entire
        # purpose was to change a column's type, and the failure only reappeared at
        # serving time.
        current = {(f.name, f.dataType.simpleString()) for f in spark.table(full).schema.fields}
        wanted = {(f.name, f.dataType.simpleString()) for f in sdf.schema.fields}
        if current == wanted:
            fe.write_table(name=full, df=sdf, mode="merge")
            print(f"{full}: merged {sdf.count()} rows (schema unchanged)")
            return full
        print(f"{full}: schema changed, recreating.")
        print(f"  added:   {sorted(wanted - current)}")
        print(f"  removed: {sorted(current - wanted)}")
        if online_name:
            # No online_store= here on purpose. delete_online_table removes the table
            # from BOTH Unity Catalog and the database, so the psycopg cleanup is a
            # legacy safety net for tables published before that API existed -- and a
            # psycopg call on this pipeline is what aborted the kernel with SIGABRT once
            # already (verification_log V12). Keeping psycopg off the critical path is
            # worth more than cleaning up an orphan that should no longer occur.
            ops.drop_synced_if_exists(w, cfg.t(online_name))
        spark.sql(f"DROP TABLE IF EXISTS {full}")

    kwargs = dict(name=full, primary_keys=primary_keys, df=sdf, description=description)
    if timeseries:
        # Plural `timeseries_columns` -- verified against databricks-feature-engineering
        # on this workspace. This is what makes the offline join point-in-time and
        # the online publish deduplicate to the latest row per key.
        kwargs["timeseries_columns"] = timeseries
    fe.create_table(**kwargs)
    spark.sql(f"ALTER TABLE {full} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    print(f"{full}: created, {spark.table(full).count()} rows | PK={primary_keys} | ts={timeseries}")
    return full


upsert_feature_table(
    spark.createDataFrame(rail_features), "rail_features", ["rail_id"],
    "Per-rail audience and content features for vertical (rail) ranking - mirrored to the online store",
    online_name="online_rail_features")
upsert_feature_table(
    staged, "viewer_rail_features_ts", ["viewer_id", "rail_id", "ts"],
    "Daily viewer x rail engagement snapshots. Point-in-time source for vertical ranking "
    "offline, and the same table published online where it deduplicates to the latest "
    "snapshot per (viewer, rail). Stamped at end of day so a training row cannot read "
    "same-day clicks.",
    timeseries="ts",
    online_name="online_viewer_rail")
# COMMAND ----------
spark.sql(f"DROP TABLE IF EXISTS {STAGE}")
print("staging table dropped")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Publish to the same online store
# MAGIC
# MAGIC No second store, no second Lakebase project, no second always-on capacity
# MAGIC bill. The vertical ranker's two tables land in the same online store the
# MAGIC horizontal ranker already reads from.
# COMMAND ----------
store = fe.get_online_store(name=cfg.online_store)
if store is None:
    store = fe.create_online_store(
        name=cfg.online_store, capacity=cfg.extras.get("online_capacity", "CU_1"))
    print("created online store:", cfg.online_store)
print("online store:", store)
# COMMAND ----------
PUBLISH = [
    ("rail_features", "online_rail_features"),
    ("viewer_rail_features_ts", "online_viewer_rail"),
]

published = []
for src, dst in PUBLISH:
    src_full, dst_full = cfg.t(src), cfg.t(dst)
    # online_store= omitted for the same reason as above: publish_or_refresh only uses
    # it to hand to drop_synced_if_exists, and delete_online_table already removes the
    # database table. This notebook now touches no Postgres connection at all.
    action = ops.publish_or_refresh(w, fe, cfg.online_store, src_full, dst_full,
                                    publish_mode="TRIGGERED")
    published.append((src_full, dst_full, action))
# COMMAND ----------
summaries = []
for src_full, dst_full, action in published:
    version = ops.source_commit_version(spark, src_full)
    if action == "published":
        print(f"\nwaiting for {dst_full} to reach source commit {version}")
        summaries.append(ops.wait_for_sync(w, dst_full, min_commit_version=version, timeout_s=900))
    else:
        print(f"\nwaiting for a new sync of {dst_full} (or confirmation it already "
              f"covers source commit {version})")
        summaries.append(ops.refresh_and_wait(
            w, dst_full, min_commit_version=version, trigger=False, timeout_s=900))

for s in summaries:
    print(f"{s['name']}: {s['detailed_state']} | processed_commit={s['last_processed_commit_version']} "
          f"| sync_end={s['sync_end']}")
# COMMAND ----------
# MAGIC %md
# MAGIC ### The publish really did deduplicate
# MAGIC
# MAGIC Offline row count over online row count is the compression the time series
# MAGIC designation buys. Online should hold exactly one row per (viewer, rail).
# COMMAND ----------
offline_rows = spark.table(cfg.t("viewer_rail_features_ts")).count()
offline_keys = spark.table(cfg.t("viewer_rail_features_ts")) \
    .select("viewer_id", "rail_id").distinct().count()
online_rows = spark.table(cfg.t("online_viewer_rail")).count()
print(f"offline: {offline_rows:,} rows across {offline_keys:,} (viewer, rail) keys")
print(f"online:  {online_rows:,} rows")
assert online_rows == offline_keys, (
    f"online row count {online_rows} != distinct keys {offline_keys}; the online "
    "table is not deduplicated to latest-per-key and every claim about the "
    "serving path in this demo depends on it being so")
print(f"one row per key confirmed; online is {offline_rows / max(online_rows, 1):.0f}x smaller")
print(f"online {cfg.t('online_rail_features')}: {spark.table(cfg.t('online_rail_features')).count()} rows")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Where the latency numbers live
# MAGIC
# MAGIC Not here. This notebook owned building and publishing the tables, and it is
# MAGIC finished. Measuring the online store belongs in notebook 25, which runs as its
# MAGIC own job (`make bench`) for a specific reason:
# MAGIC
# MAGIC a direct psycopg read against the Lakebase endpoint aborted the serverless
# MAGIC kernel here on 2026-09-16 — `exit code 134 (SIGABRT)`, a native crash no
# MAGIC `try/except` can contain — *after* the feature tables were built and published
# MAGIC and the dedup assertion above had passed. A diagnostic measurement that can
# MAGIC take a 20-minute pipeline down with it does not belong on the pipeline.
# MAGIC
# MAGIC So notebook 25 measures:
# MAGIC
# MAGIC * composite-key reads on `online_viewer_rail`, in region,
# MAGIC * single-key reads on `online_rail_features`,
# MAGIC * and the endpoint's own end-to-end behaviour, which is the number that
# MAGIC   actually matters — the serving path does its own lookups and a serial
# MAGIC   estimate from raw keyed reads is an upper bound, not a prediction.
# COMMAND ----------
# The published tables are readable through Unity Catalog as FOREIGN tables. This
# is a correctness check, not a latency measurement: a federated Spark read costs
# about a second and has nothing to do with the serving path.
online_sample = spark.sql(f"""
    SELECT * FROM {cfg.t('online_viewer_rail')}
    ORDER BY vr_ctr_30d DESC LIMIT 5
""")
display(online_sample)
print("online_rail_features:")
display(spark.table(cfg.t("online_rail_features")).orderBy("rail_editorial_rank"))
# COMMAND ----------
dbutils.notebook.exit(json.dumps({
    "as_of": str(AS_OF),
    "impressions": int(n_impressions),
    "rail_features_rows": int(len(rail_features)),
    "viewer_rail_ts_rows": int(offline_rows),
    "viewer_rail_keys": int(offline_keys),
    "viewer_rail_online_rows": int(online_rows),
    "reused_unchanged": ["viewer_features_current", "recent_behavior_current",
                         "session_features_current", "title_features"],
    "new_feature_tables": ["rail_features", "viewer_rail_features_ts"],
    "online_tables": [{"table": d, "action": a} for _, d, a in published],
    "latency_measured_by": "notebooks/90_ops/25_serving_benchmark.py (make bench)",
    "sync": [{k: s.get(k) for k in ("name", "detailed_state", "last_processed_commit_version")}
             for s in summaries],
}))
