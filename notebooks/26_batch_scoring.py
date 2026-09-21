# Databricks notebook source
# MAGIC %md
# MAGIC # 26 · The same model, scored in batch — no online store anywhere
# MAGIC
# MAGIC Crunchyroll's November deliverable is **batch**, because Lakebase is not yet
# MAGIC available in their region (GCP us-west1). Real-time is the end goal, not the
# MAGIC starting point. So the question this notebook answers is not "can Databricks do
# MAGIC batch" — it is:
# MAGIC
# MAGIC > **If we build the batch path now, how much of it survives when the online store
# MAGIC > arrives?**
# MAGIC
# MAGIC The answer here is: the feature definitions, the training set, the registered
# MAGIC model and the ranking logic are **the same objects**. What changes is one API call
# MAGIC and where the features are read from.
# MAGIC
# MAGIC | | batch (today, no online store) | online (when Lakebase lands) |
# MAGIC |---|---|---|
# MAGIC | features read from | offline Delta feature tables | published Lakebase copy |
# MAGIC | how | `fe.score_batch(model_uri, df)` | `POST /serving-endpoints/.../invocations` |
# MAGIC | feature definitions | `src/crfs/features.py`, `rails.py` | **identical** |
# MAGIC | feature spec | embedded in the model | **identical, same model version** |
# MAGIC | model | `crunchyroll_rail_ranker@champion` | **identical** |
# MAGIC | what the caller supplies | viewer + context rows | viewer + context rows |
# MAGIC | context features | fixed at scoring time | evaluated per request |
# MAGIC
# MAGIC `score_batch` performs the feature lookups against the **offline** store. Per the
# MAGIC Databricks documentation: "If the model at model_uri is packaged with the features,
# MAGIC the score_batch() call automatically retrieves the required features from Feature
# MAGIC Store before scoring the model." No online store is involved, so nothing in this
# MAGIC notebook depends on Lakebase being available.
# MAGIC
# MAGIC The last section is the important one: it scores the same viewer both ways and
# MAGIC compares. If batch and online disagree, the November work would need
# MAGIC re-validation when they switch. This measures whether it does.
# COMMAND ----------
# MAGIC %pip install databricks-feature-engineering --quiet
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import os, sys
_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _root not in sys.path:
    sys.path.insert(0, _root)

from src.crfs.config import Config
from src.crfs import rails as R

cfg = Config.from_widgets(dbutils, extra_widgets={
    "batch_mode": "full",              # full | incremental
    "batch_device": "tv",              # the context batch precomputes for
    "batch_hour": "21",
    "batch_output": "rail_rankings_batch",
    "batch_top_n": "0",                # 0 = keep every scored rail
})
spark.sql(f"USE {cfg.fq}")
MODE = cfg.extras["batch_mode"].strip().lower()
DEVICE = cfg.extras["batch_device"]
HOUR = int(cfg.extras["batch_hour"])
OUT = cfg.extras["batch_output"]
TOP_N = int(cfg.extras["batch_top_n"])
MODEL = cfg.t("crunchyroll_rail_ranker")
print(cfg.describe())
print(f"\nmode={MODE} context=({DEVICE}, {HOUR}:00) output={cfg.t(OUT)}")
# COMMAND ----------
import time, json
import pandas as pd
from pyspark.sql import functions as F, types as T
from databricks.feature_engineering import FeatureEngineeringClient
from mlflow.tracking import MlflowClient

fe = FeatureEngineeringClient()
mc = MlflowClient(registry_uri="databricks-uc")

# Resolve @champion to a concrete version, exactly as notebook 23 does for the
# endpoint. Batch and online must score the SAME version or the comparison at the end
# of this notebook is meaningless.
VERSION = str(mc.get_model_version_by_alias(MODEL, "champion").version)
MODEL_URI = f"models:/{MODEL}/{VERSION}"
print(f"scoring with {MODEL_URI}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Which viewers to score
# MAGIC
# MAGIC A refresh measured in **minutes** is the requirement, and a full rescore of every
# MAGIC viewer x collection every few minutes does not scale — at Crunchyroll's MAU that
# MAGIC is tens of millions of rows per run. So the batch path needs an incremental mode,
# MAGIC and the feature tables already support it: **Change Data Feed is enabled on all
# MAGIC four**, so "whose features moved since the last run" is a query, not a guess.
# MAGIC
# MAGIC `full` scores everyone (the nightly baseline and the first run).
# MAGIC `incremental` scores only viewers whose features changed.
# COMMAND ----------
def viewers_with_changed_features(since_version_by_table: dict) -> list:
    """Viewer ids whose feature rows changed, read from Change Data Feed.

    The point of CDF here is cost: at a minutes-level cadence the difference between
    rescoring everyone and rescoring the ~1% whose behaviour moved is the difference
    between a feasible job and an absurd one.
    """
    ids = set()
    for tbl, ver in since_version_by_table.items():
        try:
            # `startingVersion` is inclusive -- verified against this workspace:
            # table_changes('rail_features', 15) returns the 16 rows written *by*
            # commit 15, which is the commit the previous run already scored. Reading
            # from `ver` therefore re-reports the last run's own writes, and for the
            # rail-grain table that alone forces a full refresh on every incremental
            # run. The last scored version is `ver`, so changes start at `ver + 1`.
            latest = int(spark.sql(f"DESCRIBE HISTORY {cfg.t(tbl)} LIMIT 1")
                         .first()["version"])
            if ver >= latest:
                print(f"  {tbl}: no commits since version {ver}")
                continue
            cdf = (spark.read.format("delta")
                   .option("readChangeFeed", "true")
                   .option("startingVersion", ver + 1)
                   .table(cfg.t(tbl)))
            if "viewer_id" not in cdf.columns:
                # rail_features is keyed by rail_id: any change to it affects EVERY
                # viewer's ordering, so it forces a full refresh rather than a subset.
                if cdf.filter("_change_type != 'update_preimage'").limit(1).count():
                    print(f"  {tbl} changed and is rail-grain -> full refresh required")
                    return None
                continue
            changed = (cdf.filter("_change_type != 'update_preimage'")
                       .select("viewer_id").distinct())
            n = changed.count()
            print(f"  {tbl}: {n} viewers changed since version {ver}")
            ids |= {r["viewer_id"] for r in changed.collect()}
        except Exception as e:
            print(f"  {tbl}: CDF unavailable ({type(e).__name__}), forcing full refresh")
            return None
    return sorted(ids)


def current_versions(tables) -> dict:
    out = {}
    for t in tables:
        v = spark.sql(f"DESCRIBE HISTORY {cfg.t(t)} LIMIT 1").first()["version"]
        out[t] = int(v)
    return out


FEATURE_TABLES = ["viewer_features_current", "recent_behavior_current",
                  "rail_features", "viewer_rail_features_ts"]
versions_now = current_versions(FEATURE_TABLES)
print("feature table versions:", versions_now)

state_tbl = cfg.t("crfs_batch_state")
prev_state = None
try:
    prev_state = spark.table(state_tbl).orderBy(F.col("scored_at").desc()).first()
except Exception:
    print("no previous batch state; this run is a full refresh")

target_viewers = None
if MODE == "incremental" and prev_state is not None:
    since = json.loads(prev_state["table_versions"])
    target_viewers = viewers_with_changed_features(since)
    if target_viewers is None:
        MODE = "full"
        print("-> falling back to full refresh")
    else:
        print(f"-> incremental: {len(target_viewers)} viewers to rescore")
elif MODE == "incremental":
    MODE = "full"
    print("-> no prior state, running full refresh")
# COMMAND ----------
# MAGIC %md
# MAGIC ## The scoring frame
# MAGIC
# MAGIC One row per (viewer, eligible collection) — the same shape the endpoint receives,
# MAGIC because it is the same model. Two columns exist here that a live request would not
# MAGIC carry explicitly:
# MAGIC
# MAGIC * `ts` — the point-in-time key. Set to *now*, so the as-of lookup against
# MAGIC   `viewer_rail_features_ts` returns the latest snapshot per key. That is exactly
# MAGIC   the row the online store would have served, which is **why** batch and online
# MAGIC   agree.
# MAGIC * `request_epoch_s` — the UDF inputs need a clock. In batch it is the scoring
# MAGIC   time; in the request path it is the request time.
# MAGIC
# MAGIC Eligibility is applied here, before scoring, exactly as the homepage service would.
# COMMAND ----------
t_frame = time.perf_counter()
elig = R.eligible_rails_all(spark, cfg.fq, viewers=target_viewers)
scoring_sdf = (elig
               .withColumn("device", F.lit(DEVICE))
               .withColumn("locale", F.lit("en-US"))
               .withColumn("hour_of_day", F.lit(HOUR).cast("bigint"))
               .withColumn("day_of_week",
                           F.dayofweek(F.current_timestamp()).cast("bigint") - F.lit(1))
               .withColumn("ts", F.current_timestamp())
               .withColumn("request_epoch_s",
                           F.unix_timestamp(F.current_timestamp()).cast("bigint")))
n_rows = scoring_sdf.count()
n_viewers = scoring_sdf.select("viewer_id").distinct().count()
frame_s = time.perf_counter() - t_frame
print(f"scoring frame: {n_rows:,} rows across {n_viewers:,} viewers "
      f"({n_rows / max(n_viewers,1):.1f} eligible collections each) in {frame_s:.1f}s")
display(scoring_sdf.limit(5))
# COMMAND ----------
# MAGIC %md
# MAGIC ## `score_batch` — the offline twin of the endpoint
# MAGIC
# MAGIC No feature values are passed in. The model carries its own feature spec, so this
# MAGIC call resolves four feature tables and five UC Python UDFs against the **offline**
# MAGIC store and scores the result. The same spec, unchanged, is what the endpoint uses
# MAGIC against Lakebase.
# COMMAND ----------
t0 = time.perf_counter()
scored = fe.score_batch(model_uri=MODEL_URI, df=scoring_sdf)
# score_batch is lazy, so it has to be forced for the timing below to mean anything.
# `.cache()` cannot do it here: serverless compute rejects it with
# `[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported`. Writing the frame
# to its Delta destination is the honest force anyway -- it is the work the batch job
# actually has to do, and it means the reported rows/s includes the write rather than
# hiding it behind a cached count.
scored_path = cfg.t("crfs_batch_scored_tmp")
(scored.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(scored_path))
scored = spark.table(scored_path)
n_scored = scored.count()
score_s = time.perf_counter() - t0
print(f"scored {n_scored:,} rows in {score_s:.1f}s "
      f"({n_scored / max(score_s, 0.001):,.0f} rows/s, materialisation included)")
print("columns returned:", len(scored.columns))
display(scored.select("viewer_id", "rail_id", "prediction").limit(5))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Rank within viewer, and write the precomputed homepage order
# MAGIC
# MAGIC The endpoint returns `rail_rank` because the pyfunc ranks within the request. In
# MAGIC batch the same ranking is a window over the scored frame — one collection order
# MAGIC per viewer, which is what the homepage would read.
# COMMAND ----------
from pyspark.sql.window import Window

w = Window.partitionBy("viewer_id").orderBy(F.col("prediction").desc(), F.col("rail_id"))
ranked = (scored
          .withColumn("rail_rank", F.row_number().over(w))
          .withColumn("engagement_probability", F.col("prediction").cast("double"))
          .withColumn("scored_at", F.current_timestamp())
          .withColumn("model_version", F.lit(VERSION))
          .withColumn("context_device", F.lit(DEVICE))
          .withColumn("context_hour", F.lit(HOUR).cast("bigint"))
          .select("viewer_id", "rail_id", "rail_rank", "engagement_probability",
                  "model_version", "context_device", "context_hour", "scored_at"))
if TOP_N:
    ranked = ranked.filter(F.col("rail_rank") <= TOP_N)

t_write = time.perf_counter()
out_full = cfg.t(OUT)
if MODE == "incremental" and target_viewers is not None and not target_viewers:
    # Nothing moved since the last run. The correct action is to leave the table
    # alone -- an overwrite here would rewrite the whole table from an empty frame.
    print("no viewers changed since the last run; leaving the table untouched")
elif MODE == "incremental" and target_viewers:
    # Replace only the viewers this run rescored. replaceWhere keeps the rest of the
    # table intact, which is what makes a minutes-cadence refresh cheap.
    ids = ",".join(f"'{v}'" for v in target_viewers)
    (ranked.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"viewer_id IN ({ids})")
     .option("mergeSchema", "true").saveAsTable(out_full))
else:
    (ranked.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").saveAsTable(out_full))
write_s = time.perf_counter() - t_write
print(f"wrote {out_full} in {write_s:.1f}s")

spark.sql(f"""
    ALTER TABLE {out_full} SET TBLPROPERTIES (
      delta.enableChangeDataFeed = true,
      comment = 'Precomputed homepage collection order per viewer. Written by notebook 26 via fe.score_batch against the offline feature store -- no online store required.'
    )""")
display(spark.table(out_full).orderBy("viewer_id", "rail_rank").limit(20))
# COMMAND ----------
# record state so the next run can go incremental
state_rows = spark.createDataFrame(
    [(json.dumps(versions_now), MODE, int(n_scored), float(score_s))],
    schema=T.StructType([
        T.StructField("table_versions", T.StringType()),
        T.StructField("mode", T.StringType()),
        T.StructField("rows_scored", T.IntegerType()),
        T.StructField("score_seconds", T.DoubleType())])
).withColumn("scored_at", F.current_timestamp())
state_rows.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(state_tbl)
print(f"state recorded in {state_tbl}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Does batch agree with the endpoint?
# MAGIC
# MAGIC This is the section that decides whether the November batch work is throwaway.
# MAGIC
# MAGIC The same viewer, the same collections, the same model version, the same context —
# MAGIC scored once offline through `score_batch` and once through the live endpoint. If
# MAGIC the orderings match, then switching to real-time later is a deployment change, not
# MAGIC an ML change, and nothing needs re-validating.
# MAGIC
# MAGIC A caveat stated up front: exact float equality is not expected. The offline
# MAGIC point-in-time lookup and the online latest-per-key read can legitimately differ if
# MAGIC a feature was republished between the two calls. Rank agreement is the property
# MAGIC that matters for a homepage.
# COMMAND ----------
from databricks.sdk import WorkspaceClient

w_sdk = WorkspaceClient()
probe_viewer = spark.table(out_full).select("viewer_id").first()["viewer_id"]
batch_order = (spark.table(out_full)
               .filter(F.col("viewer_id") == probe_viewer)
               .orderBy("rail_rank")
               .select("rail_id", "rail_rank", "engagement_probability").toPandas())

agreement = {"viewer": probe_viewer, "compared": False}
try:
    records = R.rail_request_records(probe_viewer, batch_order["rail_id"].tolist(),
                                     device=DEVICE, locale="en-US", hour_of_day=HOUR)
    resp = w_sdk.serving_endpoints.query(name=cfg.rail_ranker_endpoint,
                                         dataframe_records=records)
    online = pd.DataFrame(list(resp.predictions or [])).sort_values("rail_rank")
    merged = batch_order.merge(online, on="rail_id", suffixes=("_batch", "_online"))
    merged["rank_delta"] = merged["rail_rank_batch"] - merged["rail_rank_online"]
    merged["prob_delta"] = (merged["engagement_probability_batch"]
                            - merged["engagement_probability_online"]).abs()
    same_rank = int((merged["rank_delta"] == 0).sum())
    spearman = merged["rail_rank_batch"].corr(merged["rail_rank_online"], method="spearman")
    print(f"viewer {probe_viewer}: {len(merged)} collections compared")
    print(merged[["rail_id", "rail_rank_batch", "rail_rank_online", "rank_delta",
                  "engagement_probability_batch", "engagement_probability_online",
                  "prob_delta"]].to_string(index=False))
    print(f"\nidentical rank: {same_rank}/{len(merged)}")
    print(f"Spearman(batch, online): {spearman:.4f}")
    print(f"max |probability difference|: {merged['prob_delta'].max():.8f}")
    agreement = {"viewer": probe_viewer, "compared": True,
                 "collections": int(len(merged)),
                 "identical_rank": same_rank,
                 "spearman": float(spearman) if spearman == spearman else None,
                 "max_prob_delta": float(merged["prob_delta"].max())}
    if same_rank == len(merged):
        print("\nBatch and online produce the SAME collection order. The switch from one "
              "to the other is a deployment change, not a modelling change.")
    else:
        print("\nWARNING: the orderings differ. Investigate before claiming the batch "
              "path is forward-compatible -- check whether a feature table was "
              "republished between the two calls.")
except Exception as e:
    print(f"endpoint comparison unavailable ({type(e).__name__}: {str(e)[:200]}).")
    print("The batch path above is unaffected -- it needs no endpoint and no online "
          "store. This section is the forward-compatibility check, not the deliverable.")
# COMMAND ----------
# MAGIC %md
# MAGIC ## What batch cannot do, measured
# MAGIC
# MAGIC Batch precomputes one order per viewer **for one context**. The five request-time
# MAGIC UDFs read `device`, `hour_of_day` and `request_epoch_s`, none of which exist until
# MAGIC a request arrives. So a precomputed table is correct for the context it was scored
# MAGIC at and progressively wrong for every other one.
# MAGIC
# MAGIC This scores the same viewer at four contexts and counts how many collections move.
# MAGIC That number is what the online store buys back — it is not a latency argument.
# COMMAND ----------
CONTEXTS = [("21:00 TV", "tv", 21), ("09:00 TV", "tv", 9),
            ("21:00 mobile", "mobile", 21), ("09:00 mobile", "mobile", 9)]
orders, ctx_err = {}, None
try:
    rails_for_probe = batch_order["rail_id"].tolist()
    for label, dev, hr in CONTEXTS:
        recs = R.rail_request_records(probe_viewer, rails_for_probe,
                                      device=dev, locale="en-US", hour_of_day=hr)
        r = w_sdk.serving_endpoints.query(name=cfg.rail_ranker_endpoint,
                                          dataframe_records=recs)
        df = pd.DataFrame(list(r.predictions or [])).sort_values("rail_rank")
        orders[label] = df.set_index("rail_id")["rail_rank"].to_dict()
    comp = pd.DataFrame(orders)
    base = comp[CONTEXTS[0][0]]
    moved = {lab: int((comp[lab] != base).sum()) for lab, _, _ in CONTEXTS[1:]}
    print(comp.to_string())
    print()
    for lab, n in moved.items():
        print(f"  {lab:14s} moves {n} of {len(comp)} collections vs {CONTEXTS[0][0]}")
    worst = max(moved.values()) if moved else 0
    print(f"\nA batch table scored at {CONTEXTS[0][0]} is wrong for up to {worst} of "
          f"{len(comp)} collections in another context. Precomputing every context "
          f"instead multiplies the table by the number of contexts.")
except Exception as e:
    ctx_err = f"{type(e).__name__}: {str(e)[:160]}"
    print("context sensitivity needs the endpoint; skipped:", ctx_err)
    worst = None
# COMMAND ----------
# MAGIC %md
# MAGIC ## Cost shape, and what a minutes cadence implies
# COMMAND ----------
per_row_ms = (score_s / max(n_scored, 1)) * 1000.0
print(f"measured on this workspace, model version {VERSION}:")
print(f"  rows scored            {n_scored:,}")
print(f"  viewers               {n_viewers:,}")
print(f"  score_batch wall time  {score_s:.1f}s  ({per_row_ms:.3f} ms/row)")
print(f"  write time             {write_s:.1f}s")
print()
print("Extrapolation is deliberately NOT printed as a single number: this ran on "
      f"{n_viewers:,} viewers on serverless compute, and the per-row cost of a Spark job "
      "does not scale linearly down to small inputs or up to large ones. The shape that "
      "does transfer: a full refresh is O(viewers x eligible collections), and an "
      "incremental refresh is O(viewers whose features changed). At a minutes cadence "
      "the second is the only viable one, which is why Change Data Feed matters here.")
# COMMAND ----------
dbutils.notebook.exit(json.dumps({
    "mode": MODE,
    "model_version": VERSION,
    "rows_scored": int(n_scored),
    "viewers": int(n_viewers),
    "score_seconds": round(score_s, 2),
    "rows_per_second": round(n_scored / max(score_s, 0.001), 1),
    "write_seconds": round(write_s, 2),
    "output_table": out_full,
    "context": {"device": DEVICE, "hour": HOUR},
    "batch_vs_online": agreement,
    "max_collections_moved_by_context": worst,
    "context_error": ctx_err,
}, default=str))
