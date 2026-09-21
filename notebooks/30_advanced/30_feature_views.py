# Databricks notebook source
# MAGIC %md
# MAGIC # 30 · Feature Views — the same job, declared instead of computed
# MAGIC
# MAGIC Everything else in this repo authors features the GA way: compute a DataFrame,
# MAGIC `fe.create_table`, `fe.write_table`, `fe.publish_table`. That is 400 lines of
# MAGIC pandas in `src/crfs/features.py` plus a notebook to run it on a schedule.
# MAGIC
# MAGIC **Feature Views** (Public Preview) invert it: declare *what* the feature is —
# MAGIC source, entity, timestamp, aggregation, window — and the platform owns the
# MAGIC computation, the backfill, the refresh and the online copy. There is no
# MAGIC DataFrame to write and no publish step to call.
# MAGIC
# MAGIC This notebook is a **second authoring path, not a migration.** Nothing in the GA
# MAGIC pipeline changes and no existing table is touched. The point is that a team can
# MAGIC see both against the same data and decide which to standardise on.
# MAGIC
# MAGIC | | GA feature tables | Feature Views |
# MAGIC |---|---|---|
# MAGIC | where the logic lives | `src/crfs/features.py` (pandas) | the `Feature` definition |
# MAGIC | who computes it | this repo's notebooks and jobs | the platform |
# MAGIC | window semantics | hand-written, per table | `SlidingWindow` / `TumblingWindow` / … |
# MAGIC | online copy | `fe.publish_table` + a sync to wait on | `fe.materialize_features(online_config=…)` |
# MAGIC | training set | `FeatureLookup(table_name=…)` | `features=[…]`, no table name anywhere |
# MAGIC | point-in-time | `timestamp_lookup_key` against a `_ts` table | inherent — the definition has a window |
# MAGIC | expressible | anything Python can compute | 18 operators, plus `CustomUDF` / `RowTransformation` / chaining |
# MAGIC | status | GA | **Public Preview** |
# MAGIC
# MAGIC Run `29_preview_probe` first if you are on a new workspace.
# MAGIC
# MAGIC Blog: <https://www.databricks.com/blog/introducing-feature-views>
# COMMAND ----------
# MAGIC %pip install 'databricks-feature-engineering>=0.16.0' --quiet
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
from src.crfs import feature_views as FV

cfg = Config.from_widgets(dbutils, extra_widgets={
    "fv_prefix": "fv_viewer",          # offline/online table prefix for materialization
    "fv_label_frac": "0.05",           # sample of the impression log used as labels
    "fv_materialize": "true",          # set false to stop before creating pipelines
})
spark.sql(f"USE {cfg.fq}")

MODEL = cfg.t("crunchyroll_ranker_fv")
EXPERIMENT = f"/Users/{spark.sql('SELECT current_user()').first()[0]}/crunchyroll_feature_views"

import json
import time

import mlflow
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENT)

from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()
# COMMAND ----------
# MAGIC %md
# MAGIC ## 1 · Declare the features
# MAGIC
# MAGIC Seven viewer features over `engagement_events`, defined in
# MAGIC [`src/crfs/feature_views.py`](../../src/crfs/feature_views.py) so the definitions
# MAGIC live in one importable place — the same rule the GA path follows with
# MAGIC `features.py`.
# MAGIC
# MAGIC Three choices worth stating, because each is a trap:
# MAGIC
# MAGIC * **`SlidingWindow`, not `RollingWindow`.** Batch rolling-window features cannot
# MAGIC   be materialized, so a rolling definition trains fine and then cannot be served.
# MAGIC * **No feature derived from `played`.** `played` is this notebook's label and it
# MAGIC   is a column of the source table; the docs say the label must not exist in a
# MAGIC   feature source, and a `played`-derived feature would leak the answer anyway.
# MAGIC * **`event_ts` keeps its name.** Entity and timestamp column names have to match
# MAGIC   between the label DataFrame and the definitions, so the label frame below
# MAGIC   carries `viewer_id` and `event_ts` rather than the GA path's `ts`.
# COMMAND ----------
# Two grains. A viewer-only feature set cannot rank titles for a viewer -- every
# candidate row of one impression shares the viewer, so there is nothing to discriminate
# on, and the first run of this notebook scored a holdout AUC of 0.4992: exactly random.
# The title-side features are what make the label learnable, and one create_training_set
# call resolves both because the label frame carries both keys.
features = FV.all_features(cfg.catalog, cfg.schema)
for f in features:
    print(f"{f.name:28s} {FV.describe(f)}")
print(f"\n{len(FV.viewer_features(cfg.catalog, cfg.schema))} viewer-grain + "
      f"{len(FV.title_features(cfg.catalog, cfg.schema))} title-grain")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 2 · Compute them, before registering anything
# MAGIC
# MAGIC `compute_features` evaluates the definitions on the spot. Nothing is registered
# MAGIC in Unity Catalog and nothing is written, which makes this the cheap way to check
# MAGIC a window is what you meant.
# COMMAND ----------
t0 = time.perf_counter()
preview = fe.compute_features(features=features)   # local definitions, pre-registration
print("columns:", preview.columns)
display(preview.limit(10))
print(f"computed in {time.perf_counter() - t0:0.1f}s")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 3 · Register them as Unity Catalog objects
# MAGIC
# MAGIC `register_feature` makes each definition a governed UC object -- discoverable,
# MAGIC grantable, with lineage back to `engagement_events`. Re-running is safe: an
# MAGIC already-registered feature is fetched rather than duplicated.
# COMMAND ----------
# register_all returns the REGISTERED features, and those are what every later call
# uses. A local Feature has no catalog or schema, so passing these definitions to
# create_training_set raises
#   ValueError: Feature does not have a catalog and schema.
# which is what the first run of this notebook did.
features = FV.register_all(fe, features, cfg.catalog, cfg.schema)
print(f"\n{len(features)} features registered in {cfg.fq}")
print("full names now resolvable:",
      [getattr(f, "full_name", None) or f"{cfg.fq}.{f.name}" for f in features][:3], "...")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 4 · A training set, with no table name in it
# MAGIC
# MAGIC This is the part that answers "how do we use Feature Views for training". The
# MAGIC label frame carries keys, the timestamp and the outcome; `features=[…]` carries
# MAGIC the definitions. There is no join, no table name and no `timestamp_lookup_key` --
# MAGIC the window is part of the definition, so the point-in-time behaviour comes with it.
# MAGIC
# MAGIC The label is renamed `engaged`: `played` is a column of the feature source, and
# MAGIC the docs are explicit that a label must not be.
# COMMAND ----------
FRAC = float(cfg.extras["fv_label_frac"])
labels = (spark.table(cfg.t("engagement_events"))
          .filter("event_type = 'impression'")
          .selectExpr("viewer_id", "title_id", "event_ts",
                      "surface", "device", "locale", "hour_of_day",
                      "played AS engaged")
          .sample(fraction=FRAC, seed=42))
n_labels = labels.count()
print(f"labels: {n_labels} ({FRAC:.0%} of the impression log)")

# `viewer_id` and `title_id` stay IN, and only the point-in-time key is excluded.
# predict() reads both -- it ranks within viewer and returns title_id -- and
# exclude_columns removes a column from the training set AND from what the endpoint
# hands the model. Excluding them makes every row fall into one group, so the ranking
# silently becomes global instead of per-viewer. Notebook 22 keeps its keys for the
# same reason, and docs/verification_log.md records the retriever failing UC
# registration over exactly this.
#
# They are not features: encode() reads FEATURE_COLS + context + categorical only, so
# an id cannot reach the model.
training_set = fe.create_training_set(
    df=labels,
    features=features,
    label="engaged",
    exclude_columns=["event_ts"],
)
train_df = training_set.load_df()
print("training columns:", train_df.columns)
# COMMAND ----------
train_pdf = train_df.toPandas()
print("rows:", len(train_pdf))
print(train_pdf.head(8).to_string(index=False))
print("\nnull rate per feature column (a viewer with no events in the window is null,")
print("not zero -- the model has to be told which of those it is):")
print(train_pdf.isna().mean().round(4).to_string())
# COMMAND ----------
# MAGIC %md
# MAGIC ## 5 · Train, and log the model with its feature definitions attached
# MAGIC
# MAGIC `fe.log_model(training_set=…)` is what makes the rest work: the feature
# MAGIC dependencies travel inside the model, so `score_batch` and a serving endpoint
# MAGIC both resolve them themselves. The caller sends keys and context; it never sends
# MAGIC feature values and never learns where they live.
# COMMAND ----------
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score

FEATURE_COLS = [f.name for f in features]
CONTEXT_NUM = ["hour_of_day"]
CATEGORICAL = ["surface", "device", "locale"]
# viewer_id and title_id are in the frame (predict needs them to rank within viewer and
# to label its output) but are deliberately absent from all three lists above, so they
# are carried, not learned from.
IDS = ["viewer_id", "title_id"]

# Time-ordered split. A random split on an impression log leaks the future into the
# training set through the very windows these features aggregate.
cut = train_pdf["event_ts"].quantile(0.8) if "event_ts" in train_pdf else None
if cut is not None:
    tr = train_pdf[train_pdf["event_ts"] <= cut]
    te = train_pdf[train_pdf["event_ts"] > cut]
else:
    tr, te = train_pdf.iloc[: int(0.8 * len(train_pdf))], train_pdf.iloc[int(0.8 * len(train_pdf)):]
print(f"train {len(tr)} | holdout {len(te)}")

encoders = {c: {v: i for i, v in enumerate(sorted(train_pdf[c].dropna().unique()))}
            for c in CATEGORICAL}


def encode(df):
    X = pd.DataFrame(index=df.index)
    for c in FEATURE_COLS + CONTEXT_NUM:
        col = df[c] if c in df else pd.Series(np.nan, index=df.index)
        X[c] = pd.to_numeric(col, errors="coerce")
    for c in CATEGORICAL:
        m = encoders[c]
        col = df[c] if c in df else pd.Series("unknown", index=df.index)
        X[c] = col.astype(str).map(lambda v, m=m: m.get(v, len(m))).astype(float)
    return X[FEATURE_COLS + CONTEXT_NUM + CATEGORICAL]


# NaN is left as NaN on purpose: HistGradientBoosting handles missing natively, and
# filling with 0 would tell the model "no watching" where the truth is "no data".
model = HistGradientBoostingClassifier(max_iter=120, learning_rate=0.1, random_state=42)
model.fit(encode(tr), tr["engaged"].astype(int))
auc = roc_auc_score(te["engaged"].astype(int), model.predict_proba(encode(te))[:, 1])
print(f"holdout AUC: {auc:0.4f}")
# COMMAND ----------
import pickle
import tempfile

art_dir = tempfile.mkdtemp(prefix="cr_fv_")
with open(os.path.join(art_dir, "model.pkl"), "wb") as fh:
    pickle.dump(model, fh)
with open(os.path.join(art_dir, "spec.pkl"), "wb") as fh:
    pickle.dump({"features": FEATURE_COLS, "context": CONTEXT_NUM,
                 "categorical": CATEGORICAL, "encoders": encoders}, fh)


class FeatureViewRanker(mlflow.pyfunc.PythonModel):
    """Scores viewer x title rows and ranks them within each viewer.

    Defined inline rather than imported from src/crfs/: a serving endpoint has no
    access to the bundle's workspace files, so anything the model needs at load time
    has to be in the artifact. Same rule as notebooks 02, 08 and 22.
    """

    def load_context(self, context):
        import pickle
        with open(context.artifacts["model"], "rb") as fh:
            self._model = pickle.load(fh)
        with open(context.artifacts["spec"], "rb") as fh:
            spec = pickle.load(fh)
        self._features = spec["features"]
        self._context = spec["context"]
        self._categorical = spec["categorical"]
        self._encoders = spec["encoders"]

    def _encode(self, df):
        import pandas as _pd
        X = _pd.DataFrame(index=df.index)
        for c in self._features + self._context:
            # A Series, never a bare scalar: every looked-up feature is optional in
            # the signature, so any of them can be absent from the frame, and
            # `_pd.to_numeric(0)` returns an int with no .fillna -- which Model
            # Serving reports as "unexpected error ... Error ''" with no traceback.
            col = df[c] if c in df else _pd.Series(float("nan"), index=df.index)
            X[c] = _pd.to_numeric(col, errors="coerce")
        for c in self._categorical:
            m = self._encoders[c]
            col = df[c] if c in df else _pd.Series("unknown", index=df.index)
            X[c] = col.astype(str).map(lambda v, m=m: m.get(v, len(m))).astype(float)
        return X[self._features + self._context + self._categorical]

    def predict(self, context, model_input, params=None):
        import pandas as _pd
        df = model_input if isinstance(model_input, _pd.DataFrame) else _pd.DataFrame(model_input)
        prob = self._model.predict_proba(self._encode(df))[:, 1]
        out = _pd.DataFrame({
            "title_id": (df["title_id"].astype(str) if "title_id" in df
                         else _pd.Series([""] * len(df), index=df.index)),
            "play_probability": prob.astype(float),
        }, index=df.index)
        group = df["viewer_id"].astype(str) if "viewer_id" in df else _pd.Series("_", index=df.index)
        out["title_rank"] = (out.groupby(group.values)["play_probability"]
                             .rank(ascending=False, method="first").astype(int))
        return out


# Exercise the wrapper exactly as Model Serving will, BEFORE logging it. The same
# failure inside a live endpoint arrives as a 500 with no traceback.
class _LocalCtx:
    artifacts = {"model": os.path.join(art_dir, "model.pkl"),
                 "spec": os.path.join(art_dir, "spec.pkl")}


_probe = FeatureViewRanker()
_probe.load_context(_LocalCtx())

# The input example has to come back through Spark, not out of the pandas frame the
# model was fit on. `fe.log_model` infers the signature from the example, and a column
# that pandas holds as float64 (because this sample contained a null) but Spark returns
# as a nullable Int64 fails at scoring time with
#   Incompatible input types for column fv_watch_seconds_24h.
#   Can not safely convert Int64 to float64.
# Round-tripping the example through `load_df().toPandas()` is the same conversion
# `score_batch` performs, so the signature and the served frame agree by construction.
_example_sdf = training_set.load_df().drop("engaged")
_serving_like = _example_sdf.limit(24).toPandas()

# Now coerce to the dtypes the SERVING path will actually present. Two conversions
# disagree and the signature has to match the second one:
#
#   toPandas()   widens a nullable bigint to float64 as soon as the slice contains a
#                null -- so an example built this way types a Sum() feature as double
#   score_batch  delivers the same column as pandas nullable Int64
#
# and mlflow refuses the mismatch at scoring time with
#   Incompatible input types for column fv_watch_seconds_24h.
#   Can not safely convert Int64 to float64.
# which surfaces inside a Spark UDF, so the traceback names mlflow rather than this cell.
# Reading the Spark schema and casting to Int64 makes the logged signature agree with
# what the platform hands the model.
_SPARK_TO_PANDAS = {"bigint": "Int64", "int": "Int32", "smallint": "Int16",
                    "double": "float64", "float": "float32", "boolean": "boolean"}
for _f in _example_sdf.schema.fields:
    _target = _SPARK_TO_PANDAS.get(_f.dataType.simpleString())
    if _target and _f.name in _serving_like.columns:
        _serving_like[_f.name] = _serving_like[_f.name].astype(_target)
print("example dtypes, matched to the Spark schema:")
print(_serving_like.dtypes.to_string())
for label, frame in [("one row", _serving_like.head(1)),
                     ("one viewer, many titles", _serving_like.head(12)),
                     ("features absent (endpoint lookup miss)",
                      _serving_like.head(3).drop(columns=FEATURE_COLS, errors="ignore"))]:
    got = _probe.predict(None, frame)
    print(f"self-test {label:40s} -> {len(got)} rows, ranks {sorted(got['title_rank'])[:5]}")
# COMMAND ----------
with mlflow.start_run(run_name="crunchyroll_ranker_feature_views") as run:
    info = fe.log_model(
        model=FeatureViewRanker(),
        artifact_path="ranker_fv",
        flavor=mlflow.pyfunc,
        training_set=training_set,
        registered_model_name=MODEL,
        artifacts=_LocalCtx.artifacts,
        input_example=_serving_like.head(3),
    )
    mlflow.log_metric("holdout_auc", auc)
    mlflow.log_param("n_features", len(FEATURE_COLS))
    mlflow.log_param("n_labels", n_labels)
    mlflow.log_param("authoring", "feature_views")
    run_id = run.info.run_id
print("logged:", MODEL, "| run:", run_id)
# COMMAND ----------
# MAGIC %md
# MAGIC ## 6 · The feature dependencies really are inside the model
# MAGIC
# MAGIC Not an assertion -- read back off the registered version. This is the same
# MAGIC mechanism the GA path relies on, and it is what makes a feature-definition change
# MAGIC safe for an already-deployed model: the model carries the definitions it was
# MAGIC trained with.
# COMMAND ----------
from mlflow.tracking import MlflowClient

mc = MlflowClient()
latest = max(int(v.version) for v in mc.search_model_versions(f"name='{MODEL}'"))
print(f"{MODEL} version {latest}")

from src.crfs import versioning as V

spec = V.feature_spec_of(f"models:/{MODEL}/{latest}")
print("\nfeature spec inside the model:")
print(V.render_spec(spec))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 7 · Batch scoring resolves the features itself
# MAGIC
# MAGIC `score_batch` gets keys and context only. Every feature column in its output was
# MAGIC retrieved by the platform from the definitions inside the model -- no join here,
# MAGIC and no feature engineering in the calling code.
# COMMAND ----------
score_input = (labels.select("viewer_id", "title_id", "event_ts",
                             "surface", "device", "locale", "hour_of_day")
               .limit(200))
scored = fe.score_batch(model_uri=f"models:/{MODEL}/{latest}", df=score_input)
cols = [c for c in scored.columns if c in FEATURE_COLS]
print("feature columns resolved by score_batch:", cols)
display(scored.select("viewer_id", "title_id", *cols, "prediction").limit(10))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 8 · Materialize, so the same definitions can serve online
# MAGIC
# MAGIC `materialize_features` is the replacement for "write a table, then publish it":
# MAGIC one call takes an offline destination, an online destination and a trigger, and
# MAGIC the platform owns the pipeline that keeps both current.
# MAGIC
# MAGIC The online destination is the **online store the GA demo already created** in
# MAGIC notebook 01, so this costs no new always-on infrastructure -- it is a second set
# MAGIC of tables in the same Lakebase project.
# MAGIC
# MAGIC `TableTrigger()` refreshes on commits to `engagement_events`, which is the right
# MAGIC cadence for a table an ingest job appends to. `CronSchedule` is the alternative
# MAGIC when cost matters more than freshness.
# COMMAND ----------
if cfg.extras["fv_materialize"].strip().lower() != "true":
    print("fv_materialize=false, stopping before any pipeline is created")
    dbutils.notebook.exit(json.dumps({"model": MODEL, "version": latest,
                                      "holdout_auc": round(auc, 4),
                                      "materialized": False}))
# COMMAND ----------
from databricks.feature_engineering.entities import (
    OfflineStoreConfig, OnlineStoreConfig, TableTrigger)

PREFIX = cfg.extras["fv_prefix"]
t0 = time.perf_counter()
materialized = fe.materialize_features(
    features=features,
    offline_config=OfflineStoreConfig(
        catalog_name=cfg.catalog,
        schema_name=cfg.schema,
        table_name_prefix=PREFIX,
    ),
    online_config=OnlineStoreConfig(
        catalog_name=cfg.catalog,
        schema_name=cfg.schema,
        table_name_prefix=f"{PREFIX}_online",
        online_store_name=cfg.online_store,
    ),
    trigger=TableTrigger(),
)
print(f"materialize_features returned in {time.perf_counter() - t0:0.1f}s")
for m in (materialized or []):
    print(" ", m)
# COMMAND ----------
# MAGIC %md
# MAGIC ## 9 · What it actually created
# MAGIC
# MAGIC Materialization is asynchronous: the call returns once the pipeline exists, not
# MAGIC once the data has landed. This waits on the tables rather than sleeping, the same
# MAGIC way `src/crfs/ops.py` waits on a publish.
# COMMAND ----------
# What materialization actually creates, read back from the platform rather than guessed
# from the prefix. Three things the first run taught, all worth seeing printed:
#
#   * `table_name_prefix` really is a PREFIX -- the platform appends a generated suffix,
#     so the offline table is `fv_viewer_<id>`, not `fv_viewer`.
#   * one table per (entity, window) grouping, not one table per call: the 24h, 7d and
#     30d features land in different tables, and mixing grains adds more.
#   * an internal `<name>_partial_aggregates` table appears beside each one.
#
# A wait loop that stops at "some table with rows exists" therefore reports success on a
# partial materialization, which is what the first run did.
materialized_report = {}
try:
    listed = fe.list_materialized_features()
    rows = list(listed) if listed is not None else []
    print(f"list_materialized_features: {len(rows)} entries")
    for m in rows[:40]:
        print("  ", m)
    materialized_report["listed"] = [str(m) for m in rows]
except Exception as e:
    print("list_materialized_features unavailable:", type(e).__name__, str(e)[:160])
    materialized_report["listed"] = f"{type(e).__name__}: {e}"
# COMMAND ----------
# MAGIC %md
# MAGIC ### Wait for the offline tables, then for the online copy
# MAGIC
# MAGIC Materialization is asynchronous: the call returns when the pipeline exists, not
# MAGIC when data has landed. Both destinations are waited on separately, and the online
# MAGIC one is reported as a gap rather than hidden if it does not arrive -- an online
# MAGIC feature that is not there is the difference between this being a serving path and
# MAGIC a training convenience.
# COMMAND ----------
def fv_tables(pattern: str):
    """Offline tables the materialization created, with their row counts."""
    out = {}
    for r in spark.sql(f"SHOW TABLES IN {cfg.fq} LIKE '{pattern}'").collect():
        name = r["tableName"]
        if name.endswith("_partial_aggregates"):
            continue          # internal intermediate, not a feature table
        try:
            out[name] = spark.table(cfg.t(name)).count()
        except Exception as e:
            out[name] = f"unreadable ({type(e).__name__})"
    return out


def online_tables(pattern: str):
    """The online copies show up in UC as FOREIGN tables, the same as publish_table's."""
    rows = spark.sql(f"""
        SELECT table_name FROM {cfg.catalog}.information_schema.tables
        WHERE table_schema = '{cfg.schema}' AND table_type = 'FOREIGN'
          AND table_name LIKE '{pattern}'
    """).collect()
    return [r["table_name"] for r in rows]


deadline = time.time() + 1200
offline, online = {}, []
while time.time() < deadline:
    offline = fv_tables(f"{PREFIX}*")
    online = online_tables(f"{PREFIX}_online%")
    ready = offline and all(isinstance(v, int) and v > 0 for v in offline.values())
    if ready and online:
        break
    print(f"waiting: offline={offline or '{}'} online={online or '[]'}")
    time.sleep(45)

print("\noffline feature tables:")
for name, n in sorted(offline.items()):
    print(f"  {cfg.t(name):72s} {n}")
    try:
        cols = [c for c in spark.table(cfg.t(name)).columns
                if c.startswith(("fv_", "fvt_"))]
        print(f"      features: {', '.join(cols)}")
    except Exception:
        pass

print("\nonline copies in the Lakebase store:")
if online:
    for name in sorted(online):
        print(f"  {cfg.t(name)}")
else:
    print("  NONE YET. The offline side is materialized and the online pipeline was")
    print("  requested; the online tables had not appeared within the wait. Check")
    print("  fe.list_materialized_features() and the pipeline it names before claiming")
    print("  a serving path.")

materialized_report["offline"] = offline
materialized_report["online"] = online
# COMMAND ----------
dbutils.notebook.exit(json.dumps({
    "model": MODEL,
    "version": latest,
    "holdout_auc": round(auc, 4),
    "n_features": len(FEATURE_COLS),
    "n_labels": n_labels,
    "materialized": True,
    "materialization": materialized_report,
    "online_ready": bool(materialized_report.get("online")),
}, default=str))
