# Databricks notebook source
# MAGIC %md
# MAGIC # 22 · The vertical ranker — trained from stored features, ranked at request time
# MAGIC
# MAGIC One label table (the homepage log), one set of `FeatureLookup`s, and the model
# MAGIC that comes out carries its own feature spec — so the serving endpoint retrieves
# MAGIC features itself and the request only has to say who and where.
# MAGIC
# MAGIC ## What it reads
# MAGIC
# MAGIC | Lookup | Table | Key | Shared with the watch-next ranker? |
# MAGIC |---|---|---|---|
# MAGIC | viewer, long horizon | `viewer_features_current` | viewer_id | **yes, unchanged** |
# MAGIC | viewer, last 24h | `recent_behavior_current` | viewer_id | **yes, unchanged** |
# MAGIC | rail | `rail_features` | rail_id | new |
# MAGIC | viewer × rail, **point in time** | `viewer_rail_features_ts` | viewer_id + rail_id, as of `ts` | new |
# MAGIC | request time | 3 new UC Python UDFs + **2 reused verbatim** | — | partly |
# MAGIC
# MAGIC ## Three things this notebook refuses to do the easy way
# MAGIC
# MAGIC **1 · It corrects for position bias.** Every label in a homepage log was
# MAGIC observed at a position the incumbent policy chose. Rails at the top are clicked
# MAGIC because they are at the top. Fit that raw and the model learns the old policy.
# MAGIC Clicked rows are weighted by `1 / P(viewport | position)` from
# MAGIC `rail_position_propensity`; the rendered position itself is **never a feature**,
# MAGIC because at request time it does not exist yet — it is the output.
# MAGIC
# MAGIC **2 · It trains on point-in-time features.** The viewer × rail lookup is an
# MAGIC as-of join against the time series table, so a row labelled in July reads July's
# MAGIC click history and not today's. Serving reads the same table, where the online
# MAGIC copy holds the latest snapshot per key. One table, one definition, no skew.
# MAGIC
# MAGIC **3 · It reports ranking quality, not just AUC.** Nobody ships a homepage
# MAGIC because AUC went up 0.004. The measure that matters is whether the rail a
# MAGIC viewer engaged with is nearer the top than the incumbent editorial order put
# MAGIC it — NDCG and MRR per homepage session, against that baseline, plus an ablation
# MAGIC that strips every rail-identity feature so the remaining lift is personalization
# MAGIC rather than a better fixed order.
# MAGIC
# MAGIC ## What the endpoint returns
# MAGIC
# MAGIC The logged model ranks **within the request**: all rows of one call are one
# MAGIC homepage render, so it returns `rail_id`, a probability, and the rank. The
# MAGIC caller does not have to sort anything, and rows are grouped by `viewer_id`
# MAGIC before ranking so a batched multi-viewer call cannot contaminate one viewer's
# MAGIC order with another's.
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

from src.crfs.config import Config, GENRES
from src.crfs import rails as R
from src.crfs import udfs as U

cfg = Config.from_widgets(dbutils, extra_widgets={
    "holdout_days": "10",
    # The point-in-time join is the expensive step, not the fit. Measured on this
    # workspace: an as-of join of 363k labels against a 421k-row time series table,
    # collected to pandas twice (train + holdout), takes ~25-30 minutes on serverless.
    # Measured on this workspace: at 1.0 (363k labels) the as-of join against the
    # 421k-row time series table did not finish inside a 60-minute task timeout --
    # twice. A point-in-time join is a range join, and range joins degrade badly as the
    # product of the two sides grows. 0.25 is the default because it finishes, keeps
    # ~6k holdout homepage sessions for the ranking metrics, and does not touch the
    # feature tables or the propensity table, which are still built from the FULL log.
    # Set it to 1.0 only with a long timeout and a reason.
    "label_sample_frac": "0.25",
})
spark.sql(f"USE {cfg.fq}")
HOLDOUT_DAYS = int(cfg.extras.get("holdout_days") or 10)
LABEL_FRAC = float(cfg.extras.get("label_sample_frac") or 1.0)
print(cfg.describe())
print(f"\nholdout_days={HOLDOUT_DAYS}  label_sample_frac={LABEL_FRAC}")
# COMMAND ----------
import json
import numpy as np
import pandas as pd
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

w = WorkspaceClient()
fe = FeatureEngineeringClient()
MODEL = cfg.t("crunchyroll_rail_ranker")
# COMMAND ----------
# MAGIC %md
# MAGIC ## The request-time UDFs
# MAGIC
# MAGIC Three new ones for the rail side; `cr_hour_affinity_delta` and
# MAGIC `cr_session_decay` are created by notebook 06 for the watch-next ranker and
# MAGIC reused here **unchanged**. Re-running their DDL is idempotent, so this notebook
# MAGIC creates all five and stays runnable on its own.
# COMMAND ----------
for stmt in U.rail_ddl(cfg.fq):
    spark.sql(stmt)
for stmt in U.ddl(cfg.fq):
    spark.sql(stmt)
created = [r["function"] for r in spark.sql(f"SHOW USER FUNCTIONS IN {cfg.fq} LIKE 'cr_*'").collect()]
print("request-time UDFs in", cfg.fq)
for f in sorted(created):
    short = f.split(".")[-1]
    tag = "  <- reused by both rankers" if short in U.REUSED_BY_VERTICAL else ""
    print(f"  {short}{tag}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Labels: the homepage log, with propensity weights attached
# MAGIC
# MAGIC `request_epoch_s` is derived from the impression's own timestamp rather than
# MAGIC from the clock now, so the two request-time decay UDFs compute the same values
# MAGIC during training that they would have computed at render time.
# COMMAND ----------
labels_sdf = spark.sql(f"""
    SELECT i.viewer_id,
           i.rail_id,
           i.rendered_ts                              AS ts,
           i.device,
           i.locale,
           i.hour_of_day,
           i.day_of_week,
           CAST(unix_timestamp(i.rendered_ts) AS BIGINT) AS request_epoch_s,
           i.rail_position,
           i.was_viewport,
           i.engaged,
           -- Inverse propensity, applied to observed engagements only. Weighting
           -- the zeros as well would inflate the influence of rails nobody
           -- scrolled to, which is the opposite of the correction we want.
           CASE WHEN i.engaged = 1 THEN p.ips_weight ELSE 1.0 END AS sample_weight
    FROM {cfg.t('rail_impressions')} i
    JOIN {cfg.t('rail_position_propensity')} p
      ON p.rail_position = i.rail_position
""")
full_labels = labels_sdf.count()
if LABEL_FRAC < 1.0:
    # Sampled by session, not by row: splitting a homepage session across the sample
    # boundary would break the per-session ranking metrics further down.
    labels_sdf = labels_sdf.filter(
        F.abs(F.hash(F.concat_ws("|", "viewer_id", "request_epoch_s"))) % 1000
        < int(LABEL_FRAC * 1000))
    print(f"sampled to {LABEL_FRAC:.0%} of homepage SESSIONS "
          f"(whole sessions, so per-session ranking metrics stay intact)")
n_labels = labels_sdf.count()
print(f"labels: {n_labels:,} of {full_labels:,} in the full log")
print("NOTE: the feature tables and rail_position_propensity are built from the full "
      "log by notebooks 20 and 21. This sample only affects what the model is fit on.")
display(labels_sdf.limit(5))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Proof the point-in-time join is real
# MAGIC
# MAGIC The same viewer × rail key, as of an old impression and as of now. If these
# MAGIC columns matched, the as-of join would be decoration and the model would be
# MAGIC training on the future.
# COMMAND ----------
# Every lookup is point-in-time, and the lookups live in src/crfs/rails.py so notebook 32
# trains on the identical set rather than a copy of it.
#
# This used to look up viewer_features_current, recent_behavior_current and rail_features
# WITHOUT a timestamp, which handed every historical label today's values. For the viewer
# tables that is ordinary leakage; for rail_features it is worse -- rail_ctr_30d and
# rail_clicks_30d are aggregates of the same `engaged` column this model predicts, so a
# holdout impression's own click sat inside its own features and the reported NDCG and AUC
# were invalid rather than optimistic. Notebooks 01 and 21 now build and publish the
# matching _ts tables; offline they are read as-of each label, online they deduplicate to
# the latest row per key, so serving is unchanged.
LOOKUPS = R.rail_lookups(cfg)
for _lk in LOOKUPS:
    _tbl = getattr(_lk, "table_name", None)
    if _tbl:
        print(f"  lookup {_tbl.split('.')[-1]:26s} as-of={getattr(_lk, 'timestamp_lookup_key', None)}")

# Narrowed to a handful of viewers BEFORE the join, not `orderBy(...).limit(400)`
# after it. The as-of join is the expensive operation in this notebook; asking for a
# global sort of every label and then 400 rows of it makes the probe as costly as the
# training set itself.
probe_viewers = [r["viewer_id"] for r in spark.sql(f"""
    SELECT viewer_id FROM {cfg.t('rail_impressions')}
    GROUP BY viewer_id ORDER BY COUNT(*) DESC LIMIT 3
""").collect()]
probe_labels = labels_sdf.filter(F.col("viewer_id").isin(probe_viewers)) \
                         .orderBy("ts").limit(200)
pit_probe = fe.create_training_set(
    df=probe_labels,
    feature_lookups=[FeatureLookup(table_name=cfg.t("viewer_rail_features_ts"),
                                   lookup_key=["viewer_id", "rail_id"],
                                   timestamp_lookup_key="ts")],
    label="engaged", exclude_columns=["ts"]).load_df().toPandas()

now_values = (spark.table(cfg.t("online_viewer_rail"))
              .select("viewer_id", "rail_id", "vr_ctr_30d", "vr_impressions_30d")
              .toPandas().set_index(["viewer_id", "rail_id"]))
rows = []
for _, r in pit_probe.iterrows():
    key = (r["viewer_id"], r["rail_id"])
    if key in now_values.index and float(r.get("vr_impressions_30d") or 0) > 0:
        rows.append({
            "viewer_id": r["viewer_id"], "rail_id": r["rail_id"],
            "vr_impressions_at_impression_time": int(r["vr_impressions_30d"]),
            "vr_impressions_now": int(now_values.loc[key, "vr_impressions_30d"]),
            "vr_ctr_at_impression_time": round(float(r["vr_ctr_30d"] or 0), 4),
            "vr_ctr_now": round(float(now_values.loc[key, "vr_ctr_30d"]), 4),
        })
    if len(rows) >= 6:
        break
proof = pd.DataFrame(rows)
print("point-in-time proof - same key, as of the impression vs as of now:")
print(proof.to_string(index=False) if len(proof) else "  (no overlapping keys found)")
if len(proof):
    differing = int((proof["vr_impressions_at_impression_time"] != proof["vr_impressions_now"]).sum())
    print(f"\n{differing} of {len(proof)} rows differ. If that were 0, the as-of join "
          f"would not be doing anything.")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Training and holdout sets
# MAGIC
# MAGIC Split by time, not at random: a random split over a homepage log leaks a
# MAGIC session's other impressions into the training half and reports a number the
# MAGIC model will never reproduce in production.
# COMMAND ----------
max_ts = pd.to_datetime(labels_sdf.agg({"ts": "max"}).first()[0])
cutoff = max_ts - pd.Timedelta(days=HOLDOUT_DAYS)
print("label window ends", max_ts, "| holdout after", cutoff)

train_labels = labels_sdf.filter(labels_sdf["ts"] <= cutoff)
test_labels = labels_sdf.filter(labels_sdf["ts"] > cutoff)

# What is excluded decides the served model's request schema, and getting it wrong is a
# 400 at serving time rather than an error here.
#
#   ts, rail_position, was_viewport, sample_weight  -> EXCLUDED.
#     Every column left in the training set becomes an input on the logged signature,
#     and label-side ones become REQUIRED. Measured on a live request carrying only the
#     seven legitimate request keys:
#       "Model is missing inputs ['rail_position', 'was_viewport', 'sample_weight']"
#     while all 45 looked-up features were correctly marked optional. They are still
#     needed for the fit and the evaluation, so they are joined back in pandas below.
#
#   viewer_id, rail_id  -> KEPT.
#     predict() reads both: rail_id names the rails in the response, viewer_id groups
#     rows so a batched multi-viewer call cannot mix orders. Excluding a column that
#     predict() reads is also what broke notebook 08's registration. Neither reaches the
#     feature matrix -- the NUMERIC/CATEGORICAL lists decide that.
LABEL_SIDE = ["rail_position", "was_viewport", "sample_weight"]
EXCLUDE = ["ts"] + LABEL_SIDE
# (viewer_id, rail_id, request_epoch_s) identifies one impression -- verified unique
# across all 363,351 rows of rail_impressions, so it is a safe merge key.
MERGE_KEY = ["viewer_id", "rail_id", "request_epoch_s"]


def with_label_side(joined_pdf, labels_spark_df):
    """Re-attach the excluded label-side columns by impression key."""
    side = labels_spark_df.select(*MERGE_KEY, *LABEL_SIDE).toPandas()
    out = joined_pdf.merge(side, on=MERGE_KEY, how="left", validate="one_to_one")
    missing = int(out["sample_weight"].isna().sum())
    assert missing == 0, f"{missing} rows failed to re-attach label-side columns"
    return out


training_set = fe.create_training_set(df=train_labels, feature_lookups=LOOKUPS,
                                      label="engaged", exclude_columns=EXCLUDE)
print("loading the point-in-time training set (as-of join over "
      f"{n_labels:,} labels)...")
_t0 = pd.Timestamp.utcnow()
train_pdf = with_label_side(training_set.load_df().toPandas(), train_labels)
print(f"  train set loaded in {(pd.Timestamp.utcnow() - _t0).total_seconds():.0f}s")
_t0 = pd.Timestamp.utcnow()
test_pdf = with_label_side(
    fe.create_training_set(df=test_labels, feature_lookups=LOOKUPS,
                           label="engaged", exclude_columns=EXCLUDE)
      .load_df().toPandas(),
    test_labels)
print(f"  holdout set loaded in {(pd.Timestamp.utcnow() - _t0).total_seconds():.0f}s")
# ---- the training frame's dtypes ARE the serving contract -------------------
# The logged signature is inferred from this frame. Where the point-in-time join finds
# no viewer x rail snapshot yet, the BIGINT columns arrive as NULL, pandas widens them to
# float64, and the signature says `double`. At serving the online lookup returns a real
# int64 and MLflow refuses to narrow:
#
#   Error: Incompatible input types for column vr_last_click_epoch_s.
#          Can not safely convert int64 to float64.
#
# which reaches the caller as an empty "Error ''" and is only visible in
#   databricks serving-endpoints logs <endpoint> <served-entity-name>
#
# So every column that is integral in its source feature table is restored to int64
# here. Zero is the semantically right fill in each case: a count of no impressions is
# 0, and cr_rail_click_recency already treats a 0 epoch as "never clicked".
LOOKUP_TABLES = ["viewer_features_current", "recent_behavior_current",
                 "rail_features", "viewer_rail_features_ts"]
INT_FEATURE_COLS = set()
for _tbl in LOOKUP_TABLES:
    for _f in spark.table(cfg.t(_tbl)).schema.fields:
        if _f.dataType.typeName() in ("long", "integer", "short", "byte"):
            INT_FEATURE_COLS.add(_f.name)


def restore_integer_dtypes(pdf):
    fixed = []
    for c in sorted(INT_FEATURE_COLS & set(pdf.columns)):
        if pdf[c].dtype.kind != "i":
            pdf[c] = pd.to_numeric(pdf[c], errors="coerce").fillna(0).astype("int64")
            fixed.append(c)
    return fixed


for _name, _pdf in (("train", train_pdf), ("holdout", test_pdf)):
    _fixed = restore_integer_dtypes(_pdf)
    print(f"  {_name}: restored int64 on {len(_fixed)} column(s) widened by the PIT join"
          + (f": {_fixed}" if _fixed else ""))

print(f"train {len(train_pdf):,} rows | holdout {len(test_pdf):,} rows")
print(f"train CTR {train_pdf['engaged'].mean():.4f} | holdout CTR {test_pdf['engaged'].mean():.4f}")
print(f"\ncolumns the training set produced ({len(train_pdf.columns)}):")
print("  " + "\n  ".join(sorted(train_pdf.columns)))
# COMMAND ----------
# MAGIC %md
# MAGIC ## The feature matrix
# MAGIC
# MAGIC Two exclusions are load-bearing:
# MAGIC
# MAGIC * **`rail_position` and `was_viewport` are not features.** Both are properties
# MAGIC   of the impression *after* the incumbent policy acted. Position especially:
# MAGIC   at request time the position is what we are computing.
# MAGIC * **raw epochs are not features.** `last_event_epoch_s` and
# MAGIC   `vr_last_click_epoch_s` feed the decay UDFs, which turn them into something
# MAGIC   with meaning. Fed to the tree directly they are a proxy for calendar date and
# MAGIC   they poison anything trained on one window and served in another.
# COMMAND ----------
VIEWER_NUM = ([f"genre_affinity_{g}" for g in GENRES]
              + ["minutes_watched_7d", "plays_7d", "completion_rate_30d",
                 "avg_watch_minutes_30d", "skips_7d", "typical_watch_hour",
                 "hour_concentration",
                 "minutes_watched_24h", "skips_24h", "active_titles_24h"])
RAIL_NUM = list(R.RAIL_FEATURE_COLS)
VR_NUM = [c for c in R.VIEWER_RAIL_FEATURE_COLS if not c.endswith("_epoch_s")]
CONTEXT_NUM = ["hour_of_day", "day_of_week"]
ONDEMAND = list(U.RAIL_ONDEMAND_OUTPUTS)

NUMERIC = VIEWER_NUM + RAIL_NUM + VR_NUM + CONTEXT_NUM + ONDEMAND
CATEGORICAL = ["device", "locale", "last_primary_genre"]
NOT_FEATURES = (["viewer_id", "rail_id", "engaged", "request_epoch_s",
                 "last_event_epoch_s", "vr_last_click_epoch_s"] + LABEL_SIDE)

missing = [c for c in NUMERIC + CATEGORICAL if c not in train_pdf.columns]
assert not missing, f"training set is missing expected columns: {missing}"
unused = sorted(set(train_pdf.columns) - set(NUMERIC) - set(CATEGORICAL) - set(NOT_FEATURES))
print(f"{len(NUMERIC)} numeric + {len(CATEGORICAL)} categorical features")
print("deliberately not features:", NOT_FEATURES)
if unused:
    print("WARNING - columns present but unused, check this is intended:", unused)
# COMMAND ----------
encoders = {c: {v: i for i, v in enumerate(sorted(train_pdf[c].astype(str).unique()))}
            for c in CATEGORICAL}


def encode(df, numeric=NUMERIC, categorical=CATEGORICAL, enc=encoders):
    """Mirror of CrunchyrollRailRanker._encode. Kept identical on purpose: if these two
    ever diverge, the model is fit on one representation and served on another."""
    X = pd.DataFrame(index=df.index)
    for c in numeric:
        # Series, not scalar -- see the note in _encode.
        col = df[c] if c in df else pd.Series(0.0, index=df.index)
        X[c] = pd.to_numeric(col, errors="coerce").fillna(0.0)
    for c in categorical:
        m = enc[c]
        # Unseen category -> len(vocab). A KeyError here would surface as a 500
        # from the endpoint on the first locale the training window did not contain.
        col = df[c] if c in df else pd.Series("unknown", index=df.index)
        X[c] = (col.astype(str).map(lambda v, m=m: m.get(v, len(m))).astype(float))
    return X[list(numeric) + list(categorical)].values


from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score

Xtr, ytr = encode(train_pdf), train_pdf["engaged"].values
Xte, yte = encode(test_pdf), test_pdf["engaged"].values

model = HistGradientBoostingClassifier(max_iter=260, learning_rate=0.07, max_depth=6,
                                       random_state=42)
model.fit(Xtr, ytr, sample_weight=train_pdf["sample_weight"].values)
p_test = model.predict_proba(Xte)[:, 1]

auc_all = roc_auc_score(yte, p_test)
viewed = test_pdf["was_viewport"].astype(bool).values
auc_viewed = roc_auc_score(yte[viewed], p_test[viewed]) if viewed.sum() > 50 else float("nan")
print(f"holdout AUC, all impressions   : {auc_all:.4f}")
print(f"holdout AUC, viewed impressions: {auc_viewed:.4f}   <- the honest one; "
      f"engagement on a rail nobody scrolled to is not a preference")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Ranking quality against the incumbent homepage
# MAGIC
# MAGIC The question is not "is the classifier calibrated". It is: **for a homepage
# MAGIC session, does the model put the rail the viewer actually engaged with higher
# MAGIC than the current editorial order does?**
# MAGIC
# MAGIC Restricted to sessions with at least one engagement, and to viewed impressions,
# MAGIC because a rail that never entered the viewport carries no evidence either way.
# COMMAND ----------
def ndcg_at_k(order_scores, relevance, k=5):
    """NDCG@k for one session. Binary relevance, log2 discount."""
    idx = np.argsort(-np.asarray(order_scores, dtype=float))[:k]
    gains = np.asarray(relevance, dtype=float)[idx]
    dcg = float(np.sum(gains / np.log2(np.arange(2, len(gains) + 2))))
    ideal = np.sort(np.asarray(relevance, dtype=float))[::-1][:k]
    idcg = float(np.sum(ideal / np.log2(np.arange(2, len(ideal) + 2))))
    return dcg / idcg if idcg > 0 else np.nan


def mrr(order_scores, relevance):
    idx = np.argsort(-np.asarray(order_scores, dtype=float))
    ranked = np.asarray(relevance, dtype=float)[idx]
    hit = np.nonzero(ranked > 0)[0]
    return 1.0 / (hit[0] + 1) if len(hit) else 0.0


ev = test_pdf.copy()
ev["model_score"] = p_test
# The incumbent policy ranks by editorial priority: lower rank number first, so
# negate it to make "higher is better" consistent across every scorer compared.
ev["editorial_score"] = -ev["rail_editorial_rank"].astype(float)
ev["popularity_score"] = ev["rail_ctr_30d"].astype(float)
rng = np.random.default_rng(0)
ev["random_score"] = rng.random(len(ev))
ev["session_key"] = ev["viewer_id"].astype(str) + "|" + ev["request_epoch_s"].astype(str)

sessions = [g for _, g in ev[ev["was_viewport"].astype(bool)].groupby("session_key")
            if g["engaged"].sum() > 0 and len(g) >= 4]
print(f"{len(sessions)} holdout homepage sessions with at least one engagement "
      f"and 4+ viewed rails")

scorers = {"vertical ranker": "model_score",
           "incumbent editorial order": "editorial_score",
           "rail popularity (global CTR)": "popularity_score",
           "random": "random_score"}
rank_metrics = {}
for label, col in scorers.items():
    n5 = [ndcg_at_k(g[col], g["engaged"], k=5) for g in sessions]
    n3 = [ndcg_at_k(g[col], g["engaged"], k=3) for g in sessions]
    mr = [mrr(g[col], g["engaged"]) for g in sessions]
    rank_metrics[label] = {"ndcg@3": round(float(np.nanmean(n3)), 4),
                           "ndcg@5": round(float(np.nanmean(n5)), 4),
                           "mrr": round(float(np.nanmean(mr)), 4)}
print()
print(pd.DataFrame(rank_metrics).T.to_string())
lift = (rank_metrics["vertical ranker"]["ndcg@5"]
        / max(rank_metrics["incumbent editorial order"]["ndcg@5"], 1e-9) - 1.0)
print(f"\nNDCG@5 vs the incumbent editorial order: {lift:+.1%}")
# COMMAND ----------
# MAGIC %md
# MAGIC ### Ablation — is the lift personalization, or just rail identity?
# MAGIC
# MAGIC The first version of this cell dropped `rail_editorial_rank` alone and reported
# MAGIC an identical NDCG. That was not a finding, it was a badly designed test: all
# MAGIC **13** `rail_features` columns are constant per rail, so with only 16 rails the
# MAGIC tree recovers rail identity — and therefore the incumbent's ordering — from any
# MAGIC one of the other twelve. Measured: Spearman 1.0 between the two models' scores.
# MAGIC
# MAGIC So the ablation now drops the **entire rail-identity block** and keeps only the
# MAGIC viewer × rail features, the request context and the on-demand crosses. That
# MAGIC answers the question actually worth asking: **is the lift coming from
# MAGIC personalization, or from learning a better fixed order of rails?**
# MAGIC
# MAGIC The `rail popularity (global CTR)` baseline in the table above is the other half
# MAGIC of the same question — it is a fixed order with no personalization at all.
# COMMAND ----------
# Everything that is a property of the rail and nothing else. Dropping one of these is
# meaningless; dropping all of them removes the model's ability to learn a fixed order.
ABL_NUMERIC = [c for c in NUMERIC if c not in set(R.RAIL_FEATURE_COLS)]
print(f"ablation drops the {len(R.RAIL_FEATURE_COLS)} rail-identity features: "
      f"{len(NUMERIC)} -> {len(ABL_NUMERIC)} numeric")
abl = HistGradientBoostingClassifier(max_iter=260, learning_rate=0.07, max_depth=6,
                                     random_state=42)
abl.fit(encode(train_pdf, numeric=ABL_NUMERIC), ytr,
        sample_weight=train_pdf["sample_weight"].values)
ev["ablation_score"] = abl.predict_proba(encode(test_pdf, numeric=ABL_NUMERIC))[:, 1]
sessions_abl = [g for _, g in ev[ev["was_viewport"].astype(bool)].groupby("session_key")
                if g["engaged"].sum() > 0 and len(g) >= 4]
abl_metrics = {
    "ndcg@3": round(float(np.nanmean([ndcg_at_k(g["ablation_score"], g["engaged"], 3)
                                      for g in sessions_abl])), 4),
    "ndcg@5": round(float(np.nanmean([ndcg_at_k(g["ablation_score"], g["engaged"], 5)
                                      for g in sessions_abl])), 4),
    "mrr": round(float(np.nanmean([mrr(g["ablation_score"], g["engaged"])
                                   for g in sessions_abl])), 4),
}
rank_metrics["vertical ranker, no rail-identity features"] = abl_metrics
print(pd.DataFrame(rank_metrics).T.to_string())
abl_lift = (abl_metrics["ndcg@5"]
            / max(rank_metrics["incumbent editorial order"]["ndcg@5"], 1e-9) - 1.0)
print(f"\nWith every rail-identity feature removed, NDCG@5 vs the incumbent order: "
      f"{abl_lift:+.1%}. What remains is viewer x rail history, request context and the "
      f"on-demand crosses -- i.e. personalization only.")

# The two NDCG figures can round to the same 4 decimal places, which looks exactly
# like a bug where the ablation silently reused the full feature set. These three
# checks distinguish "the models agree" from "the ablation did not happen".
ndcg5_full_raw = float(np.nanmean([ndcg_at_k(g["model_score"], g["engaged"], 5)
                                   for g in sessions_abl]))
ndcg5_abl_raw = float(np.nanmean([ndcg_at_k(g["ablation_score"], g["engaged"], 5)
                                  for g in sessions_abl]))
score_corr = float(ev["model_score"].corr(ev["ablation_score"], method="spearman"))
print(f"\nablation sanity checks:")
# The expected drop is the whole rail-identity block, not one column. The check still
# read `- 1` from the abandoned single-feature ablation, so every healthy run printed
# UNEXPECTED in the block whose job is to flag an ablation that did not happen.
_n_dropped = len(R.RAIL_FEATURE_COLS)
_abl_note = (f"all {_n_dropped} rail-identity features removed"
             if len(ABL_NUMERIC) == len(NUMERIC) - _n_dropped else "UNEXPECTED")
print(f"  features used            {len(NUMERIC)} -> {len(ABL_NUMERIC)} numeric "
      f"({_abl_note})")
print(f"  NDCG@5 unrounded         full {ndcg5_full_raw:.6f} | ablated {ndcg5_abl_raw:.6f} "
      f"| delta {ndcg5_abl_raw - ndcg5_full_raw:+.6f}")
print(f"  Spearman(score, ablated) {score_corr:.4f}")
if abs(score_corr) > 0.9999:
    print("  WARNING: the two models score identically, so the ablation removed nothing "
          "the model could not reconstruct from a remaining feature. Do not quote the "
          "ablated lift -- fix the ablation instead. This is exactly what happened when "
          "only rail_editorial_rank was dropped: 12 other constant-per-rail columns "
          "still identified the rail, and Spearman came back 1.0.")
else:
    print("  The two models differ, so the ablated lift is a real second measurement of "
          "how much of the lift is personalization rather than a better fixed order.")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Which features carry it
# MAGIC
# MAGIC Permutation importance on the holdout, grouped by where the feature comes
# MAGIC from. The grouping is the interesting part: it says how much of the signal is
# MAGIC coming out of the tables the two rankers share versus the two that are new.
# COMMAND ----------
from sklearn.inspection import permutation_importance

# n_jobs=1, not -1. Permutation importance over ~50 features is one model refit per
# feature per repeat, and joblib's process pool on serverless notebook compute spends
# more time spawning workers than the refits take -- the first run of this notebook sat
# in this cell for over twenty minutes. Two repeats on 8k rows is enough to rank the
# feature *sources*, which is the only thing this cell is used for.
sub = min(8000, len(test_pdf))
imp = permutation_importance(model, Xte[:sub], yte[:sub], n_repeats=2,
                             random_state=42, scoring="roc_auc", n_jobs=1)
names = NUMERIC + CATEGORICAL
imp_df = (pd.DataFrame({"feature": names, "importance": imp.importances_mean})
          .sort_values("importance", ascending=False).reset_index(drop=True))


def source_of(f):
    if f in ONDEMAND:
        return "request-time UDF"
    if f in VR_NUM:
        return "viewer_rail_features_ts (new)"
    if f in RAIL_NUM:
        return "rail_features (new)"
    if f in CONTEXT_NUM or f in CATEGORICAL:
        return "request context"
    return "shared viewer tables (reused)"


imp_df["source"] = imp_df["feature"].map(source_of)
print("top 15 features:")
print(imp_df.head(15).to_string(index=False))
print("\nimportance by source:")
by_source = (imp_df.groupby("source")["importance"].sum()
             .sort_values(ascending=False).round(5))
total = by_source.sum()
for src, val in by_source.items():
    print(f"  {src:34s} {val:>9.5f}   {val / total:>6.1%}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Wrap it so the endpoint returns a ranking, not a column of numbers
# MAGIC
# MAGIC All rows of one request are one homepage render, so the model can sort them
# MAGIC itself. It groups by `viewer_id` first: batching two viewers into one call is a
# MAGIC reasonable thing for a caller to do, and cross-contaminating their rankings
# MAGIC would be a silent bug rather than a loud one.
# COMMAND ----------
import pickle, tempfile, mlflow

art_dir = tempfile.mkdtemp(prefix="cr_rail_")
with open(os.path.join(art_dir, "model.pkl"), "wb") as f:
    pickle.dump(model, f)
with open(os.path.join(art_dir, "spec.pkl"), "wb") as f:
    pickle.dump({"numeric": NUMERIC, "categorical": CATEGORICAL, "encoders": encoders}, f)


class CrunchyrollRailRanker(mlflow.pyfunc.PythonModel):
    """Scores candidate rails and ranks them within the request."""

    def load_context(self, context):
        import pickle
        with open(context.artifacts["model"], "rb") as fh:
            self._model = pickle.load(fh)
        with open(context.artifacts["spec"], "rb") as fh:
            spec = pickle.load(fh)
        self._numeric = spec["numeric"]
        self._categorical = spec["categorical"]
        self._encoders = spec["encoders"]

    def _encode(self, df):
        import pandas as _pd
        X = _pd.DataFrame(index=df.index)
        for c in self._numeric:
            # A Series, never a bare scalar. `to_numeric(0)` returns the int 0, and
            # `0 .fillna(...)` raises AttributeError: 'int' object has no attribute
            # 'fillna' -- which Model Serving reports as
            #   "Encountered an unexpected error while evaluating the model. Error ''"
            # with no traceback. Every looked-up feature is optional in the signature,
            # so any of them can legitimately be absent from the frame.
            col = df[c] if c in df else _pd.Series(0.0, index=df.index)
            X[c] = _pd.to_numeric(col, errors="coerce").fillna(0.0)
        for c in self._categorical:
            m = self._encoders[c]
            col = df[c] if c in df else _pd.Series("unknown", index=df.index)
            X[c] = (col.astype(str).map(lambda v, m=m: m.get(v, len(m))).astype(float))
        return X[self._numeric + self._categorical].values

    def predict(self, context, model_input, params=None):
        import numpy as _np
        import pandas as _pd
        df = model_input if isinstance(model_input, _pd.DataFrame) else _pd.DataFrame(model_input)
        prob = self._model.predict_proba(self._encode(df))[:, 1]
        out = _pd.DataFrame({
            "rail_id": (df["rail_id"].astype(str) if "rail_id" in df
                        else _pd.Series([""] * len(df), index=df.index)),
            "engagement_probability": prob.astype(float),
        }, index=df.index)
        # Rank within viewer. One request is one homepage, but a caller may batch
        # several viewers; ranking across them would silently mix orders.
        group = df["viewer_id"].astype(str) if "viewer_id" in df else _pd.Series("_", index=df.index)
        out["rail_rank"] = (out.groupby(group.values)["engagement_probability"]
                            .rank(ascending=False, method="first").astype(int))
        return out


# The input example goes in, not a hand-built signature: fe.log_model derives the
# request schema from the feature spec, so the model advertises keys + context as
# required and every looked-up feature as optional. Handing it a signature built
# from the joined training frame would instead advertise 60 required columns and
# defeat automatic feature lookup at the endpoint.
# ---------------------------------------------------------------- self-test
# Exercise the wrapper exactly as Model Serving will, BEFORE logging it. A failure
# inside predict() at serving time comes back as
#   BadRequest: Encountered an unexpected error while evaluating the model. Error ''
# with no traceback and no line number -- which is unactionable. Running it here turns
# the same bug into a normal Python traceback in this notebook.
class _LocalCtx:
    """Mimics the PythonModelContext artifact dict load_context() expects."""
    artifacts = {"model": os.path.join(art_dir, "model.pkl"),
                 "spec": os.path.join(art_dir, "spec.pkl")}


_probe = CrunchyrollRailRanker()
_probe.load_context(_LocalCtx())

# The frame the endpoint hands over: request keys plus looked-up features, no label side.
_serving_like = train_pdf.drop(columns=["engaged"] + LABEL_SIDE, errors="ignore")

for _label, _frame in [
    ("one rail", _serving_like.head(1)),
    ("one viewer, many rails", _serving_like[
        _serving_like["viewer_id"] == _serving_like["viewer_id"].iloc[0]].head(12)),
    ("several viewers batched", _serving_like.head(24)),
    # A lookup miss leaves NaN in the feature columns; the endpoint does this whenever a
    # (viewer, rail) pair has no online row, so it must not raise.
    ("all lookups missing", _serving_like.head(4).assign(
        **{c: np.nan for c in _serving_like.columns
           if c not in R.RAIL_REQUEST_KEYS and _serving_like[c].dtype.kind in "fi"})),
    ("non-default index", _serving_like.head(6).set_axis(
        pd.Index(range(100, 106)), axis=0)),
    # The case that actually broke serving: every looked-up feature is optional in the
    # signature, so the frame can arrive carrying only the request keys.
    ("request keys only", _serving_like[R.RAIL_REQUEST_KEYS].head(5)),
]:
    _out = _probe.predict(None, _frame)
    assert list(_out.columns) == ["rail_id", "engagement_probability", "rail_rank"], _out.columns
    assert len(_out) == len(_frame), f"{_label}: {len(_out)} rows out of {len(_frame)} in"
    assert _out["engagement_probability"].notna().all(), f"{_label}: NaN probability"
    assert _out["rail_rank"].min() == 1, f"{_label}: ranks do not start at 1"
    print(f"  self-test ok: {_label:24s} -> {len(_out)} rows, "
          f"ranks {_out['rail_rank'].min()}..{_out['rail_rank'].max()}")
print("predict() behaves under every shape the endpoint can hand it")
# COMMAND ----------
# The example must describe a real request: the seven keys plus the looked-up features,
# and nothing from the label side.
input_example = train_pdf.drop(columns=["engaged"] + LABEL_SIDE, errors="ignore").head(3)
assert not (set(LABEL_SIDE) & set(input_example.columns)), \
    "label-side columns must not reach the signature or they become required inputs"
_widened = [c for c in sorted(INT_FEATURE_COLS & set(input_example.columns))
            if input_example[c].dtype.kind != "i"]
assert not _widened, (
    f"these columns are integral in their feature table but float in the example, so the "
    f"signature would say double and serving would refuse to narrow int64: {_widened}")
print("signature dtypes match the source feature tables")
print("input example columns:", len(input_example.columns))
print("response columns:", ["rail_id", "engagement_probability", "rail_rank"])
# COMMAND ----------
mlflow.set_registry_uri("databricks-uc")

with mlflow.start_run(run_name="rail_ranker_ips") as run:
    mlflow.log_params({
        "estimator": "HistGradientBoostingClassifier",
        "max_iter": 260, "learning_rate": 0.07, "max_depth": 6,
        "holdout_days": HOLDOUT_DAYS,
        "n_numeric": len(NUMERIC), "n_categorical": len(CATEGORICAL),
        "position_bias_correction": "IPS on observed engagements, clipped at 10x",
        "shared_feature_tables": "viewer_features_current, recent_behavior_current",
        "new_feature_tables": "rail_features, viewer_rail_features_ts",
        "reused_udfs": ",".join(U.REUSED_BY_VERTICAL),
    })
    mlflow.log_metrics({
        "holdout_auc_all": float(auc_all),
        "holdout_auc_viewed": float(auc_viewed) if auc_viewed == auc_viewed else 0.0,
        "ndcg5_model": rank_metrics["vertical ranker"]["ndcg@5"],
        "ndcg5_editorial_baseline": rank_metrics["incumbent editorial order"]["ndcg@5"],
        "ndcg5_lift_vs_baseline": float(lift),
        "ndcg5_lift_vs_baseline_ablated": float(abl_lift),
        "ndcg5_model_unrounded": ndcg5_full_raw,
        "ndcg5_ablated_unrounded": ndcg5_abl_raw,
        "spearman_model_vs_ablated": score_corr,
        "mrr_model": rank_metrics["vertical ranker"]["mrr"],
        "mrr_editorial_baseline": rank_metrics["incumbent editorial order"]["mrr"],
        "train_rows": float(len(train_pdf)),
        "holdout_rows": float(len(test_pdf)),
        "holdout_sessions_evaluated": float(len(sessions)),
    })
    mlflow.log_dict(rank_metrics, "ranking_metrics.json")
    mlflow.log_dict(imp_df.head(30).to_dict(orient="records"), "feature_importance.json")

    # fe.log_model is what makes the feature spec travel with the model: the endpoint
    # then does its own lookups and the request only carries keys and context. Logging
    # with plain mlflow here would produce a model that expects 45 pre-joined feature
    # columns from the caller -- precisely the training-serving skew this architecture
    # exists to remove.
    #
    # Two attempts, because Unity Catalog registration of a pyfunc is the sharpest edge
    # in this repo. UC requires a signature with BOTH input and output specs, and
    # fe.log_model derives the output spec by *running the model* on an example. Two
    # recipes are known to work here, and which one applies has varied:
    #
    #   * an explicit `input_example` -- what notebook 02's ranker registers with;
    #   * `infer_input_example=True`  -- what docs/verification_log.md records as the
    #     resolution after notebook 08's retriever failed six times with
    #     "a signature that includes only inputs".
    #
    # This is also why viewer_id and rail_id are NOT in exclude_columns: notebook 08's
    # real cause was that predict() read a column exclude_columns had removed, so the
    # inference run failed, no output schema was produced, and UC refused. predict()
    # here reads both, so both stay in the training set.
    base = dict(
        model=CrunchyrollRailRanker(),
        artifact_path="rail_ranker",
        flavor=mlflow.pyfunc,
        training_set=training_set,
        artifacts={"model": os.path.join(art_dir, "model.pkl"),
                   "spec": os.path.join(art_dir, "spec.pkl")},
    )
    attempts = [("explicit input_example", dict(input_example=input_example)),
                ("infer_input_example=True", dict(infer_input_example=True))]
    registered_with = None
    for label, extra in attempts:
        try:
            fe.log_model(registered_model_name=MODEL, **base, **extra)
            registered_with = label
            print(f"registered to Unity Catalog with {label}")
            break
        except Exception as e:
            print(f"UC registration via {label} failed: {type(e).__name__}: {str(e)[:300]}")
    if registered_with is None:
        # Log unregistered so the artifacts and metrics survive, then fail loudly:
        # notebook 23 has nothing to deploy without a registered version, and a
        # silently-skipped registration would surface as a confusing 404 there.
        fe.log_model(**base)
        raise RuntimeError(
            "Unity Catalog registration failed on both known recipes. The model and its "
            "metrics are logged to the run, but there is no registered version for "
            "notebook 23 to deploy. See docs/verification_log.md for the six failure "
            "modes notebook 08 hit and what each one meant.")
    mlflow.log_param("registered_with", registered_with)
    RUN_ID = run.info.run_id
print("logged run", RUN_ID)
# COMMAND ----------
from mlflow.tracking import MlflowClient

mc = MlflowClient()
versions = mc.search_model_versions(f"name='{MODEL}'")
latest = max(int(v.version) for v in versions)
mc.set_registered_model_alias(MODEL, "champion", latest)
mc.set_model_version_tag(MODEL, str(latest), "position_bias_correction", "ips")
mc.set_model_version_tag(MODEL, str(latest), "ndcg5_lift_vs_editorial", f"{lift:+.4f}")
mc.set_model_version_tag(MODEL, str(latest), "serves", "vertical rail ranking, homepage request path")
mc.update_model_version(
    name=MODEL, version=str(latest),
    description=(
        "Vertical (rail) ranker. Scores candidate homepage rails for one viewer and "
        "returns them ranked within the request.\n\n"
        "Reads four feature tables through its own feature spec: "
        "viewer_features_current and recent_behavior_current (shared unchanged with the "
        "watch-next ranker), rail_features, and viewer_rail_features_ts (point-in-time "
        "offline, latest-per-key online). Five request-time UC Python UDFs, two of them "
        "reused from the watch-next ranker.\n\n"
        f"Trained with inverse-propensity weights from rail_position_propensity. "
        f"Holdout AUC (viewed impressions) {auc_viewed:.4f}. NDCG@5 {lift:+.1%} vs the "
        f"incumbent editorial order; {abl_lift:+.1%} with every rail-identity feature "
        f"removed, which isolates the personalization component. "
        f"Spearman(full, ablated) = {score_corr:.4f}."))
print(f"{MODEL} version {latest} @champion")

# This block is display only, and it has now failed this task twice AFTER the model was
# trained, logged, registered, aliased and tagged -- once on
# `TypeError: 'method' object is not iterable`, then on
# `UC Model Versions gathered through search_model_versions do not have aliases`.
# search_model_versions returns ModelVersionSearch objects whose `aliases` property
# raises on purpose; aliases only come from get_model_version. Both the correct call and
# a guard, because nothing cosmetic should be able to cost a 30-minute pipeline.
try:
    for v in sorted(versions, key=lambda v: -int(v.version))[:5]:
        detail = mc.get_model_version(MODEL, v.version)
        print(f"  v{v.version:>3}  {detail.status}  aliases={list(detail.aliases or [])}")
except Exception as e:
    print(f"  (version summary unavailable: {type(e).__name__}: {e}); "
          f"registration itself succeeded -- v{latest} is @champion")
# COMMAND ----------
dbutils.notebook.exit(json.dumps({
    "model": MODEL,
    "version": latest,
    "run_id": RUN_ID,
    "train_rows": int(len(train_pdf)),
    "holdout_rows": int(len(test_pdf)),
    "holdout_auc_all": round(float(auc_all), 4),
    "holdout_auc_viewed": round(float(auc_viewed), 4) if auc_viewed == auc_viewed else None,
    "ranking_metrics": rank_metrics,
    "ndcg5_lift_vs_editorial": round(float(lift), 4),
    "ndcg5_lift_vs_editorial_ablated": round(float(abl_lift), 4),
    "ablation_check": {"ndcg5_full_unrounded": round(ndcg5_full_raw, 6),
                       "ndcg5_ablated_unrounded": round(ndcg5_abl_raw, 6),
                       "spearman": round(score_corr, 4),
                       "numeric_features": [len(NUMERIC), len(ABL_NUMERIC)]},
    "sessions_evaluated": int(len(sessions)),
    "n_features": len(NUMERIC) + len(CATEGORICAL),
    "importance_by_source": {k: round(float(v), 5) for k, v in by_source.items()},
    "response_columns": ["rail_id", "engagement_probability", "rail_rank"],
    "registered_with": registered_with,
}, default=str))
