# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · On-demand features and ranker v2
# MAGIC
# MAGIC The previous ranker (v1) scored from precomputed, static features. This version
# MAGIC adds four on-demand (request-time) features computed by UC Python UDFs that
# MAGIC integrate request context: the hour the viewer is watching, the wall clock at
# MAGIC request time, and the title's intrinsic properties.
# MAGIC
# MAGIC Four UDFs:
# MAGIC - **affinity_match**: dot product of viewer genre affinities and title genres
# MAGIC - **affinity_x_popularity**: scaled by title popularity
# MAGIC - **hour_affinity**: how close the request hour is to the viewer's typical habit
# MAGIC - **session_decay**: exponential decay since the last watch event
# MAGIC
# MAGIC Each arrives in the request and is computed at serving time, never on the client.
# MAGIC The training set mixes precomputed features (lookups) with request-time features
# MAGIC (UDFs), proving the model learns to use wall-clock context.
# MAGIC
# MAGIC Say: *Build and train the ranker with on-demand context features.*
# COMMAND ----------
# MAGIC %pip install databricks-feature-engineering --quiet
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import os, sys
_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _root not in sys.path: sys.path.insert(0, _root)
from src.crfs.config import Config
from src.crfs import udfs, features

cfg = Config.from_widgets(dbutils)

import mlflow
mlflow.set_registry_uri("databricks-uc")
experiment = f"/Users/{spark.sql('SELECT current_user()').first()[0]}/crunchyroll_ranker_v2_experiment"
mlflow.set_experiment(experiment)

spark.sql(f"USE {cfg.fq}")
print(f"Working in: {cfg.fq}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Guard: check for required columns from notebook 01
# MAGIC
# MAGIC The UDFs cr_hour_affinity_delta and cr_session_decay require columns
# MAGIC (typical_watch_hour, hour_concentration, last_event_epoch_s) that notebook 01
# MAGIC must populate. If 01 hasn't been re-run yet, fail loudly.
# COMMAND ----------
viewer_cols = spark.table(cfg.t("viewer_features_current")).columns
required = ["typical_watch_hour", "hour_concentration"]
missing = [c for c in required if c not in viewer_cols]

if missing:
    msg = f"BLOCKED: notebook 01 must be re-run. Missing columns in viewer_features_current: {missing}"
    print(msg)
    dbutils.notebook.exit(msg)

recent_cols = spark.table(cfg.t("recent_behavior_current")).columns
if "last_event_epoch_s" not in recent_cols:
    msg = "BLOCKED: notebook 01 must be re-run. Missing column 'last_event_epoch_s' in recent_behavior_current"
    print(msg)
    dbutils.notebook.exit(msg)

print("✓ All required columns present.")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Create/replace the on-demand UDFs
# COMMAND ----------
udf_ddl = udfs.ddl(cfg.fq)
for sql in udf_ddl:
    spark.sql(sql)
    print(f"✓ {sql.split('(')[1].split('(')[0].strip() if '(' in sql else 'UDF'} created")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify a UDF is a governed UC object
# COMMAND ----------
udf_info = spark.sql(f"DESCRIBE FUNCTION EXTENDED {cfg.fq}.cr_genre_affinity_match")
print(udf_info.show(20, truncate=False))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Build the training and test sets
# MAGIC
# MAGIC Split on the last 10 days of data to match notebook 02. The key addition is
# MAGIC `request_epoch_s`: the point-in-time-correct definition of when the request
# MAGIC arrived. It must stay OUT of the model's feature list -- only `session_decay`
# MAGIC depends on it, or the model learns absolute time and rots (trained on Sept 8,
# MAGIC deployed Sept 9, it predicts everything with zero decay).
# COMMAND ----------
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup, FeatureFunction
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

fe = FeatureEngineeringClient()

events = spark.table(cfg.t("engagement_events"))
labels_sdf = (events.filter("event_type = 'impression'")
              .select("viewer_id", "title_id",
                      events["event_ts"].alias("ts"),
                      "surface", "device", "locale", "hour_of_day",
                      events["played"].alias("played")))

# Add request_epoch_s: unix timestamp of the event
labels_sdf = labels_sdf.withColumn("request_epoch_s", F.unix_timestamp("ts"))

print(f"total impressions: {labels_sdf.count()}")

cutoff = pd.to_datetime(events.agg({"event_ts": "max"}).first()[0]) - pd.Timedelta(days=10)
train_labels = labels_sdf.filter(labels_sdf["ts"] <= cutoff)
test_labels = labels_sdf.filter(labels_sdf["ts"] > cutoff)

print(f"train: {train_labels.count()} | test (last 10d): {test_labels.count()}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Create training set with FeatureLookups + FeatureFunctions
# MAGIC
# MAGIC The feature spec includes both static lookups (viewer, title) and on-demand
# MAGIC UDFs. The UDFs depend on request columns (hour_of_day, request_epoch_s) that
# MAGIC the app sends, and on lookup outputs (genre affinities, title genres, popularity).
# COMMAND ----------
lookups = [
    FeatureLookup(table_name=cfg.t("viewer_features_current"), lookup_key="viewer_id"),
    FeatureLookup(table_name=cfg.t("recent_behavior_current"), lookup_key="viewer_id"),
    FeatureLookup(table_name=cfg.t("title_features"), lookup_key="title_id"),
]

on_demand = udfs.feature_functions(cfg.fq)

# FeatureLookups and FeatureFunctions go in the SAME list. There is no
# feature_functions= parameter; the endpoint evaluates the functions after the
# lookups they depend on.
#
# viewer_id and title_id are kept in the loaded frame so the notebook can prove the
# request-time features vary for one viewer; they are dropped from the model matrix
# by NUMERIC/CATEGORICAL. request_epoch_s is excluded deliberately -- only
# session_decay may reach the model, or it learns absolute time.
training_set = fe.create_training_set(
    df=train_labels,
    feature_lookups=lookups + on_demand,
    label="played",
    exclude_columns=["ts", "request_epoch_s"],
)

train_pdf = training_set.load_df().toPandas()
print(f"training set shape: {train_pdf.shape}")
print("columns:")
print(sorted(train_pdf.columns))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Prove on-demand features vary with the request context
# MAGIC
# MAGIC Pick a viewer and title that appear multiple times, request them at different
# MAGIC hours/times, and show that hour_affinity and session_decay change.
# COMMAND ----------
test_set = fe.create_training_set(
    df=test_labels,
    feature_lookups=lookups + on_demand,
    label="played",
    exclude_columns=["ts", "request_epoch_s"],
)
test_pdf = test_set.load_df().toPandas()

# Find a row pair with same viewer & title but different hour/request_epoch_s
demo = []
for viewer_id in train_pdf["viewer_id"].unique()[:20]:
    rows = train_pdf[train_pdf["viewer_id"] == viewer_id].head(4)
    if len(rows) >= 2:
        r1, r2 = rows.iloc[0], rows.iloc[1]
        if r1.get("hour_affinity") != r2.get("hour_affinity") or r1.get("session_decay") != r2.get("session_decay"):
            demo.append({
                "viewer_id": viewer_id,
                "hour_affinity_1": round(float(r1.get("hour_affinity", 0)), 4),
                "hour_affinity_2": round(float(r2.get("hour_affinity", 0)), 4),
                "session_decay_1": round(float(r1.get("session_decay", 0)), 4),
                "session_decay_2": round(float(r2.get("session_decay", 0)), 4),
            })
            if len(demo) >= 3:
                break

if demo:
    demo_df = pd.DataFrame(demo)
    print("✓ On-demand features vary with request context (same viewer, different times):")
    print(demo_df.to_string(index=False))
else:
    print("Note: could not find rows with varying on-demand features in sample.")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Train and log ranker v2
# MAGIC
# MAGIC 33 precomputed numeric features + 4 on-demand outputs = 37 numeric.
# MAGIC 4 categorical (surface, device, locale, last_primary_genre).
# COMMAND ----------
import pickle, json, os
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score

# Define the feature lists
ONDEMAND_FEATURES = ["affinity_match", "affinity_x_popularity", "hour_affinity", "session_decay"]

NUMERIC = (
    [f"genre_affinity_{g}" for g in ["action","adventure","fantasy","sci_fi","sports","drama","romance","slice_of_life"]]
    + ["minutes_watched_7d", "plays_7d", "completion_rate_30d", "avg_watch_minutes_30d", "skips_7d",
       "minutes_watched_24h", "skips_24h", "active_titles_24h",
       "popularity_30d", "plays_30d", "avg_rating", "days_since_release",
       "maturity_rank", "is_simulcast", "episodes_log",
       "typical_watch_hour", "hour_concentration"]
    + [f"genre_{g}" for g in ["action","adventure","fantasy","sci_fi","sports","drama","romance","slice_of_life"]]
    + ["hour_of_day"]
    + ONDEMAND_FEATURES
)

CATEGORICAL = ["surface", "device", "locale", "last_primary_genre"]

print(f"numeric features: {len(NUMERIC)} ({len([f for f in NUMERIC if not f in ONDEMAND_FEATURES])} precomputed + {len(ONDEMAND_FEATURES)} on-demand)")
print(f"categorical features: {len(CATEGORICAL)}")

encoders = {c: {v: i for i, v in enumerate(sorted(train_pdf[c].astype(str).unique()))} for c in CATEGORICAL}

def encode(df):
    X = pd.DataFrame(index=df.index)
    for c in NUMERIC:
        X[c] = pd.to_numeric(df[c] if c in df else 0, errors="coerce").fillna(0.0)
    for c in CATEGORICAL:
        m = encoders[c]
        X[c] = (df[c] if c in df else "unknown").astype(str).map(lambda v, m=m: m.get(v, len(m))).astype(float)
    return X[NUMERIC + CATEGORICAL].values

model = HistGradientBoostingClassifier(max_iter=220, learning_rate=0.08, max_depth=6, random_state=42)
model.fit(encode(train_pdf), train_pdf["played"].values)

auc_v2 = roc_auc_score(test_pdf["played"].values, model.predict_proba(encode(test_pdf))[:, 1])
# v1's AUC is read back from the registry so the comparison cannot go stale if
# notebook 02 is re-run on a different data window. The constant is only a fallback.
AUC_V1_FALLBACK = 0.6643
auc_v1 = AUC_V1_FALLBACK
try:
    _v1 = mc.get_model_version(MODEL, "1")
    _metrics = mlflow.get_run(_v1.run_id).data.metrics
    auc_v1 = float(_metrics.get("holdout_auc", AUC_V1_FALLBACK))
    print(f"v1 holdout_auc read from run {_v1.run_id}: {auc_v1:.4f}")
except Exception as _e:
    print(f"could not read v1 AUC from the registry ({str(_e)[:100]}); "
          f"falling back to {AUC_V1_FALLBACK}")

print("\nModel performance:")
print(f"  v1 (baseline, from the registry): {auc_v1:.4f}")
print(f"  v2 (with on-demand, today): {auc_v2:.4f}")
print(f"  improvement: {'+' if auc_v2 > auc_v1 else ''}{(auc_v2 - auc_v1):.4f}")

if auc_v2 > auc_v1:
    print(f"✓ v2 outperforms v1 by {(auc_v2 - auc_v1)*100:.2f} percentage points")
else:
    print(f"⚠ v2 does not outperform v1. v1 is still better by {(auc_v1 - auc_v2)*100:.2f} percentage points")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Define the pyfunc wrapper
# COMMAND ----------
class CrunchyrollRankerV2(mlflow.pyfunc.PythonModel):
    """Scores (viewer, title, context, request-time) rows for play-start probability.

    On-demand features are computed by UC UDFs at serving time, so predict()
    receives the full training schema minus the label.
    """
    NUMERIC = NUMERIC
    CATEGORICAL = CATEGORICAL

    def load_context(self, context):
        import pickle
        with open(context.artifacts["model"], "rb") as f:
            self.model = pickle.load(f)
        with open(context.artifacts["encoders"], "rb") as f:
            self.encoders = pickle.load(f)

    def predict(self, context, model_input):
        df = model_input.copy()
        X = pd.DataFrame(index=df.index)
        for c in self.NUMERIC:
            X[c] = pd.to_numeric(df[c] if c in df else 0, errors="coerce").fillna(0.0)
        for c in self.CATEGORICAL:
            m = self.encoders.get(c, {})
            X[c] = (df[c] if c in df else "unknown").astype(str).map(lambda v, m=m: m.get(v, len(m))).astype(float)
        return self.model.predict_proba(X[self.NUMERIC + self.CATEGORICAL].values)[:, 1]
# COMMAND ----------
# MAGIC %md
# MAGIC ## Log the model with FeatureEngineeringClient
# COMMAND ----------
os.makedirs("/tmp/cr_ranker_v2", exist_ok=True)
with open("/tmp/cr_ranker_v2/model.pkl", "wb") as f:
    pickle.dump(model, f)
with open("/tmp/cr_ranker_v2/encoders.pkl", "wb") as f:
    pickle.dump(encoders, f)

input_example = train_pdf.drop(columns=["played"]).head(3)

with mlflow.start_run(run_name="crunchyroll_watch_next_ranker_v2") as run:
    fe.log_model(
        model=CrunchyrollRankerV2(),
        artifact_path="cr_ranker_v2",
        flavor=mlflow.pyfunc,
        training_set=training_set,
        registered_model_name=cfg.t("crunchyroll_ranker"),
        artifacts={"model": "/tmp/cr_ranker_v2/model.pkl", "encoders": "/tmp/cr_ranker_v2/encoders.pkl"},
        input_example=input_example,
    )
    mlflow.log_metric("holdout_auc", auc_v2)
    mlflow.log_metric("v1_reference_auc", auc_v1)
    mlflow.log_param("features_numeric", len(NUMERIC))
    mlflow.log_param("features_categorical", len(CATEGORICAL))
    mlflow.log_param("ondemand_features", len(ONDEMAND_FEATURES))
    run_id = run.info.run_id

print(f"✓ logged run {run_id}")

from mlflow.tracking import MlflowClient
mc = MlflowClient()
MODEL = cfg.t("crunchyroll_ranker")
versions = mc.search_model_versions(f"name='{MODEL}'")
latest_version = max(int(v.version) for v in versions)
print(f"✓ registered as {cfg.t('crunchyroll_ranker')} version {latest_version}")

dbutils.notebook.exit(json.dumps({
    "model": cfg.t("crunchyroll_ranker"),
    "version": latest_version,
    "holdout_auc": round(auc_v2, 4),
    "auc_v1_reference": auc_v1,
    "auc_improvement": round(auc_v2 - auc_v1, 4),
    "n_numeric": len(NUMERIC),
    "n_categorical": len(CATEGORICAL),
    "ondemand": ONDEMAND_FEATURES,
    "train_rows": len(train_pdf),
    "test_rows": len(test_pdf),
    "run_id": run_id,
}))
