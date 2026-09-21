# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Train the Watch-Next ranker and log it with its feature spec
# MAGIC
# MAGIC Two things happen here:
# MAGIC
# MAGIC 1. **Point-in-time proof** — a small training set built with a timestamp
# MAGIC    lookup against `viewer_features_ts`, showing feature values *as they were
# MAGIC    at impression time* (no leakage from the future).
# MAGIC 2. **The served model** — trained with plain lookups against the same
# MAGIC    governed feature tables that are published online, then logged with
# MAGIC    `FeatureEngineeringClient.log_model`. The feature spec travels with the
# MAGIC    registered model, so the serving endpoint retrieves features itself.
# MAGIC
# MAGIC The application never rebuilds feature joins or knows where features live.
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

cfg = Config.from_widgets(dbutils)
CATALOG, SCHEMA = cfg.catalog, cfg.schema
MODEL = cfg.t("crunchyroll_ranker")
EXPERIMENT = f"/Users/{spark.sql('SELECT current_user()').first()[0]}/crunchyroll_ranker_experiment"

spark.sql(f"USE {cfg.fq}")

import mlflow
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENT)

import pandas as pd
import numpy as np
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()

events = spark.table(f"{CATALOG}.{SCHEMA}.engagement_events")
labels_sdf = (events.filter("event_type = 'impression'")
              .select("viewer_id", "title_id",
                      events["event_ts"].alias("ts"),
                      "surface", "device", "locale", "hour_of_day",
                      events["played"].alias("played")))
print("labels:", labels_sdf.count())
# COMMAND ----------
# MAGIC %md
# MAGIC ## Point-in-time proof
# COMMAND ----------
pit_sample = labels_sdf.sample(fraction=0.002, seed=42)
pit_set = fe.create_training_set(
    df=pit_sample,
    feature_lookups=[FeatureLookup(
        table_name=f"{CATALOG}.{SCHEMA}.viewer_features_ts",
        lookup_key="viewer_id",
        timestamp_lookup_key="ts",
    )],
    label="played",
    exclude_columns=["title_id", "ts"],
)
pit_pdf = pit_set.load_df().toPandas()
current = spark.table(f"{CATALOG}.{SCHEMA}.viewer_features_current").toPandas().set_index("viewer_id")

demo_rows = []
for _, r in pit_pdf.head(5).iterrows():
    vid = r["viewer_id"]
    if vid in current.index:
        demo_rows.append({
            "viewer_id": vid,
            "affinity_action_at_impression_time": round(float(r.get("genre_affinity_action", 0)), 3),
            "affinity_action_current": round(float(current.loc[vid, "genre_affinity_action"]), 3),
            "minutes_7d_at_impression_time": round(float(r.get("minutes_watched_7d", 0)), 1),
            "minutes_7d_current": round(float(current.loc[vid, "minutes_watched_7d"]), 1),
        })
proof = pd.DataFrame(demo_rows)
print("PIT proof — features at impression time vs today (values differ => time travel works):")
print(proof.to_string(index=False))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Training set for the served model
# COMMAND ----------
cutoff = pd.to_datetime(events.agg({"event_ts": "max"}).first()[0]) - pd.Timedelta(days=10)
train_labels = labels_sdf.filter(labels_sdf["ts"] <= cutoff)
test_labels = labels_sdf.filter(labels_sdf["ts"] > cutoff)

# Point-in-time, from rails.title_lookups so the two rankers share one definition of the
# two viewer tables. These used to be the _current tables with no timestamp, which joined
# every historical label to today's values -- and `title_features.popularity_30d` aggregates
# engagement, this model's own label, so a holdout impression's play sat inside the
# popularity of the title it was shown for. Same defect as the rail ranker's
# rail_ctr_30d (verification_log V76).
lookups = R.title_lookups(cfg)
for _lk in lookups:
    print(f"  lookup {_lk.table_name.split('.')[-1]:24s} as-of={_lk.timestamp_lookup_key}")
training_set = fe.create_training_set(
    df=train_labels, feature_lookups=lookups, label="played",
    exclude_columns=["viewer_id", "title_id", "ts"],
)
train_pdf = training_set.load_df().toPandas()

test_set = fe.create_training_set(
    df=test_labels, feature_lookups=lookups, label="played",
    exclude_columns=["viewer_id", "title_id", "ts"],
)
test_pdf = test_set.load_df().toPandas()
print(f"train: {len(train_pdf)} | test (last 10d): {len(test_pdf)}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Train + wrap as a self-contained pyfunc
# COMMAND ----------
import pickle, os, json

NUMERIC = ([f"genre_affinity_{g}" for g in ["action","adventure","fantasy","sci_fi","sports","drama","romance","slice_of_life"]]
           + ["minutes_watched_7d", "plays_7d", "completion_rate_30d", "avg_watch_minutes_30d", "skips_7d",
              "minutes_watched_24h", "skips_24h", "active_titles_24h",
              "popularity_30d", "plays_30d", "avg_rating", "days_since_release",
              "maturity_rank", "is_simulcast", "episodes_log",
              "genre_action", "genre_adventure", "genre_fantasy", "genre_sci_fi",
              "genre_sports", "genre_drama", "genre_romance", "genre_slice_of_life",
              "hour_of_day"])
CATEGORICAL = ["surface", "device", "locale", "last_primary_genre"]

encoders = {c: {v: i for i, v in enumerate(sorted(train_pdf[c].astype(str).unique()))} for c in CATEGORICAL}

def encode(df):
    # An absent column must become a full Series, not a scalar. `df[c] if c in df else 0`
    # yields the int 0, and `pd.to_numeric(0).fillna(...)` raises
    # `AttributeError: 'int' object has no attribute 'fillna'`; the categorical branch
    # has the same defect with the bare string ("unknown".astype). Model Serving surfaces
    # either as `Error ''` with no traceback, which cost hours on the rail ranker
    # (docs/verification_log.md V28-V30). Both branches now build an explicit Series.
    X = pd.DataFrame(index=df.index)
    for c in NUMERIC:
        col = df[c] if c in df else pd.Series(0.0, index=df.index)
        X[c] = pd.to_numeric(col, errors="coerce").fillna(0.0)
    for c in CATEGORICAL:
        m = encoders[c]
        col = df[c] if c in df else pd.Series("unknown", index=df.index)
        X[c] = col.astype(str).map(lambda v, m=m: m.get(v, len(m))).astype(float)
    return X[NUMERIC + CATEGORICAL].values

from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score

model = HistGradientBoostingClassifier(max_iter=220, learning_rate=0.08, max_depth=6, random_state=42)
model.fit(encode(train_pdf), train_pdf["played"].values)
auc = roc_auc_score(test_pdf["played"].values, model.predict_proba(encode(test_pdf))[:, 1])
print(f"holdout AUC (last 10 days): {auc:.4f}")

os.makedirs("/tmp/cr_ranker", exist_ok=True)
with open("/tmp/cr_ranker/model.pkl", "wb") as f:
    pickle.dump(model, f)
with open("/tmp/cr_ranker/encoders.pkl", "wb") as f:
    pickle.dump(encoders, f)

class CrunchyrollRanker(mlflow.pyfunc.PythonModel):
    """Scores (viewer, title, context) rows for play-start probability.

    At serving time the endpoint merges request keys + context with features
    retrieved from the online store, so predict() sees the full training
    schema minus the label.
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
        # Same explicit-Series construction as encode() above, and it matters more here:
        # this is the path Model Serving runs, where a missing optional column arrives as
        # an absent key rather than a null and the scalar form raises AttributeError
        # inside the container. The two must stay identical or training and serving encode
        # differently -- which is the whole failure mode this architecture avoids.
        df = model_input.copy()
        X = pd.DataFrame(index=df.index)
        for c in self.NUMERIC:
            col = df[c] if c in df else pd.Series(0.0, index=df.index)
            X[c] = pd.to_numeric(col, errors="coerce").fillna(0.0)
        for c in self.CATEGORICAL:
            m = self.encoders.get(c, {})
            col = df[c] if c in df else pd.Series("unknown", index=df.index)
            X[c] = col.astype(str).map(lambda v, m=m: m.get(v, len(m))).astype(float)
        return self.model.predict_proba(X[self.NUMERIC + self.CATEGORICAL].values)[:, 1]
# COMMAND ----------
# MAGIC %md
# MAGIC ## Log with the feature spec and register in Unity Catalog
# COMMAND ----------
input_example = train_pdf.drop(columns=["played"]).head(3)

with mlflow.start_run(run_name="crunchyroll_watch_next_ranker") as run:
    fe.log_model(
        model=CrunchyrollRanker(),
        artifact_path="cr_ranker",
        flavor=mlflow.pyfunc,
        training_set=training_set,
        registered_model_name=MODEL,
        artifacts={"model": "/tmp/cr_ranker/model.pkl", "encoders": "/tmp/cr_ranker/encoders.pkl"},
        input_example=input_example,
    )
    mlflow.log_metric("holdout_auc", auc)
    mlflow.log_param("features_numeric", len(NUMERIC))
    mlflow.log_param("features_categorical", len(CATEGORICAL))
    run_id = run.info.run_id
print("logged. run:", run_id)

from mlflow.tracking import MlflowClient
mc = MlflowClient()
versions = mc.search_model_versions(f"name='{MODEL}'")
latest = max(int(v.version) for v in versions)
print("registered:", MODEL, "| latest version:", latest)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Sanity: score_batch does the same feature lookup offline
# COMMAND ----------
sample = labels_sdf.orderBy("ts", ascending=False).limit(200).drop("played")
scored = fe.score_batch(model_uri=f"models:/{MODEL}/{latest}", df=sample)
scored_pdf = scored.select("viewer_id", "title_id", "prediction").toPandas()
print(scored_pdf.head(10).to_string(index=False))
print("scored rows:", len(scored_pdf), "| mean prediction:", round(float(scored_pdf.prediction.mean()), 4))

dbutils.notebook.exit(json.dumps({
    "model": MODEL, "version": latest, "holdout_auc": round(auc, 4),
    "train_rows": len(train_pdf), "test_rows": len(test_pdf), "run_id": run_id,
}))
