# Databricks notebook source
# MAGIC %md
# MAGIC # 32 · GPU training on AI Runtime, fed by the feature store
# MAGIC
# MAGIC Every other model in this repo is scikit-learn on one driver, and
# MAGIC [`docs/open_items.md` §4](../../docs/open_items.md) says so plainly: the
# MAGIC point-in-time join is Spark and scales, the estimator does not. At 363k labels
# MAGIC against a 421k-row time-series table it did not finish inside a 60-minute task,
# MAGIC twice, which is why the rail ranker trains on a 25% sample.
# MAGIC
# MAGIC This notebook substitutes the estimator and nothing else:
# MAGIC
# MAGIC * the **same** point-in-time training set from `fe.create_training_set`,
# MAGIC * a torch MLP trained in minibatches on a serverless GPU,
# MAGIC * the **same** `fe.log_model` contract, so the feature spec still travels with
# MAGIC   the model and automatic feature lookup still works at serving time.
# MAGIC
# MAGIC Measured on this workspace by `29b_gpu_probe`: a task that asks for
# MAGIC `GPU_1xA10` gets an **NVIDIA A10G, 23 GB, torch 2.7.1+cu126**.
# MAGIC
# MAGIC ### Three ways to get a GPU, and when to use which
# MAGIC
# MAGIC | | How | Use it for |
# MAGIC |---|---|---|
# MAGIC | notebook, interactive | compute selector → Serverless → Accelerator | developing the training code |
# MAGIC | **job task** (this notebook) | `compute.hardware_accelerator: GPU_1xA10` + an `environment_key` | scheduled retraining in a bundle |
# MAGIC | `ai_runtime_task` / `air` CLI | `deployments[].command_path` + `accelerator_type`, or `air run --file ai/train.yaml` | multi-node, long runs, laptop submission |
# MAGIC
# MAGIC The `air` path is in [`ai/train.yaml`](../../ai/train.yaml) and runs
# MAGIC `src/crfs/train_gpu.py` — the same module this notebook imports, not a copy.
# MAGIC
# MAGIC **Cost.** A10 is the cheapest accelerator and one is enough here; there is no
# MAGIC budget cap on serverless GPU beyond the task timeout, so the job sets one.
# COMMAND ----------
# MAGIC %pip install databricks-feature-engineering mlflow --quiet
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
from src.crfs import train_gpu as TG
from src.crfs import versioning as V

cfg = Config.from_widgets(dbutils, extra_widgets={
    "gpu_epochs": "8",
    "gpu_batch_size": "4096",
    "gpu_lr": "0.001",
    "gpu_label_sample_frac": "0.25",   # matches notebook 22 so the two are comparable
    "gpu_use_distributed": "true",     # run through serverless_gpu.distributed
    "gpu_register": "true",
})
spark.sql(f"USE {cfg.fq}")

import json
import time

import mlflow
mlflow.set_registry_uri("databricks-uc")
EXPERIMENT = f"/Users/{spark.sql('SELECT current_user()').first()[0]}/crunchyroll_rail_ranker_gpu"
mlflow.set_experiment(EXPERIMENT)

MODEL = cfg.t("crunchyroll_rail_ranker_gpu")
EPOCHS = int(cfg.extras["gpu_epochs"])
BATCH = int(cfg.extras["gpu_batch_size"])
LR = float(cfg.extras["gpu_lr"])
FRAC = float(cfg.extras["gpu_label_sample_frac"])
VOLUME_DIR = f"/Volumes/{cfg.catalog}/{cfg.schema}/{cfg.volume}/gpu_training"
CKPT_DIR = f"{VOLUME_DIR}/checkpoints"
# COMMAND ----------
# MAGIC %md
# MAGIC ## 1 · What the task was given
# MAGIC
# MAGIC Printed before anything expensive: a run that silently trained on CPU would still
# MAGIC produce a model, and the number that matters here would be meaningless.
# COMMAND ----------
import torch

print("torch:", torch.__version__, "| cuda available:", torch.cuda.is_available())
if torch.cuda.is_available():
    print("device:", torch.cuda.get_device_name(0),
          f"| {torch.cuda.get_device_properties(0).total_memory / 1e9:0.1f} GB",
          "| cuda", torch.version.cuda)
else:
    print("NO GPU. This notebook still runs, and the comparison at the end will say so.")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 2 · The same training set as the sklearn rail ranker
# MAGIC
# MAGIC Four `FeatureLookup`s and five `FeatureFunction`s, point-in-time against
# MAGIC `viewer_rail_features_ts` — the identical construction as
# MAGIC [notebook 22](../20_vertical/22_train_rail_ranker.py), so the only difference
# MAGIC between the two models is the estimator. The labels carry IPS weights, because a
# MAGIC homepage log's labels were observed at positions the incumbent policy chose.
# COMMAND ----------
from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import functions as F

fe = FeatureEngineeringClient()

# Byte-for-byte the label query from notebook 22, including the two details that are
# easy to get wrong: request_epoch_s comes from the impression's own timestamp so the
# request-time decay UDFs compute at training what they would have computed at render
# time, and the IPS weight is applied to observed engagements only -- weighting the
# zeros too would inflate rails nobody scrolled to.
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
           CASE WHEN i.engaged = 1 THEN p.ips_weight ELSE 1.0 END AS sample_weight
    FROM {cfg.t('rail_impressions')} i
    JOIN {cfg.t('rail_position_propensity')} p
      ON p.rail_position = i.rail_position
""")
full_labels = labels_sdf.count()
if FRAC < 1.0:
    # Sampled by session, not by row -- the same rule as notebook 22. A session split
    # across the sample boundary breaks per-session ranking metrics.
    labels_sdf = labels_sdf.filter(
        F.abs(F.hash(F.concat_ws("|", "viewer_id", "request_epoch_s"))) % 1000
        < int(FRAC * 1000))
labels = labels_sdf
n_labels = labels.count()
print(f"labels: {n_labels:,} of {full_labels:,} in the full log ({FRAC:.0%} of sessions)")

# The same object notebook 22 trains on, from src/crfs/rails.py -- not a copy. When these
# were two lists they drifted: notebook 22's version looked up three tables without a
# timestamp, so the GPU model inherited the same leakage by construction.
LOOKUPS = R.rail_lookups(cfg)
for _lk in LOOKUPS:
    _tbl = getattr(_lk, "table_name", None)
    if _tbl:
        print(f"  lookup {_tbl.split('.')[-1]:26s} as-of={getattr(_lk, 'timestamp_lookup_key', None)}")

# sample_weight stays OUT of the feature set and comes back via the export below:
# a weight is not a feature, and leaving it in would let the model read the label.
LABEL_SIDE = ["rail_position", "was_viewport", "sample_weight"]
EXCLUDE = ["ts"] + LABEL_SIDE

training_set = fe.create_training_set(df=labels, feature_lookups=LOOKUPS,
                                      label="engaged", exclude_columns=EXCLUDE)
print("training columns:", len(training_set.load_df().columns))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 3 · Export it to a volume instead of collecting it
# MAGIC
# MAGIC This is the step that decides whether the training path scales. `toPandas()` on
# MAGIC the full join is what put notebook 22 on a 25% sample; Parquet on a UC volume is
# MAGIC read back in minibatches, so the memory ceiling is the batch size.
# MAGIC
# MAGIC It is also what lets the `air` CLI path run the same code with no Spark session:
# MAGIC the GPU task reads files, not a DataFrame.
# COMMAND ----------
t0 = time.perf_counter()
parquet_path = TG.export_training_set(training_set, VOLUME_DIR, label="engaged",
                                      # `ts` comes back too: train() splits on it, and
                                      # without it the split fell through to physical
                                      # Parquet order, which Spark does not guarantee.
                                      extra=labels.select("viewer_id", "rail_id",
                                                          "request_epoch_s", "sample_weight",
                                                          "ts"),
                                      join_keys=["viewer_id", "rail_id", "request_epoch_s"])
print(f"exported to {parquet_path} in {time.perf_counter() - t0:0.1f}s")
files = dbutils.fs.ls(parquet_path)
print(f"{len(files)} files, {sum(f.size for f in files) / 1e6:0.1f} MB")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 4 · Train
# MAGIC
# MAGIC `@distributed(gpus=1, gpu_type="A10")` is the AI Runtime API: it runs the function
# MAGIC across the accelerators of the node, propagates the MLflow run, and populates the
# MAGIC rank environment variables. At `gpus=1` it is a thin wrapper — the same call with
# MAGIC `gpus=8, gpu_type="H100"` is how this scales to a node of eight, and the training
# MAGIC function does not change.
# MAGIC
# MAGIC The data is loaded **inside** the function on purpose: closing over a DataFrame
# MAGIC from the enclosing scope is the documented way to get a serialization error.
# COMMAND ----------
# The column taxonomy comes from `R.model_columns()`, which notebook 22 also uses --
# not from an exclusion list here. Deriving it by exclusion produced two bugs in one
# run: `last_primary_genre` is a string, so training died with `could not convert
# string to float: 'sci_fi'`, and the raw epochs would have been fed to the model as
# numbers, where they are a proxy for calendar date and poison anything trained in one
# window and served in another.
FEATURE_COLS, CATEGORICAL, NOT_FEATURES = R.model_columns()

head = TG.load_frame(parquet_path).head(200)
missing, unused = R.check_model_columns(list(head.columns), FEATURE_COLS, CATEGORICAL,
                                        NOT_FEATURES)
assert not missing, f"the exported training set is missing expected columns: {missing}"
if unused:
    print("WARNING - columns present but unused, check this is intended:", unused)
print(f"{len(FEATURE_COLS)} numeric + {len(CATEGORICAL)} categorical features")
print("deliberately not features:", NOT_FEATURES)
print("first 12 numeric:", FEATURE_COLS[:12])
# COMMAND ----------
USE_DIST = cfg.extras["gpu_use_distributed"].strip().lower() == "true"
result = None

if USE_DIST:
    try:
        from serverless_gpu import distributed

        @distributed(gpus=1, gpu_type="A10")
        def train_remote():
            # Imports and data loading inside the function: it is serialized to the
            # worker, and a closed-over DataFrame or client does not survive that.
            from src.crfs import train_gpu as _TG

            return _TG.train(
                parquet_path=parquet_path,
                feature_cols=FEATURE_COLS,
                categorical=CATEGORICAL,
                label="engaged",
                weight_col="sample_weight",
                epochs=EPOCHS,
                batch_size=BATCH,
                lr=LR,
                checkpoint_dir=CKPT_DIR,
            )

        result = train_remote.distributed()
        # `.distributed()` returns per-rank results when there are several ranks; at
        # gpus=1 it is either the value or a one-element list, and both shapes show up
        # depending on client version.
        if isinstance(result, (list, tuple)):
            result = result[0]
        print("trained through serverless_gpu.distributed")
    except Exception as e:
        print(f"distributed path unavailable ({type(e).__name__}: {str(e)[:200]})")
        print("falling back to training in this task, which already has the accelerator")
        result = None

if result is None:
    result = TG.train(
        parquet_path=parquet_path,
        feature_cols=FEATURE_COLS,
        categorical=CATEGORICAL,
        label="engaged",
        weight_col="sample_weight",
        epochs=EPOCHS,
        batch_size=BATCH,
        lr=LR,
        checkpoint_dir=CKPT_DIR,
    )

print("\n" + TG.summary(result))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 5 · Wrap it so the endpoint gets a ranking, not logits
# MAGIC
# MAGIC Same output contract as the sklearn rail ranker — `rail_id`,
# MAGIC `engagement_probability`, `rail_rank`, ranked within `viewer_id` — so this model
# MAGIC is a drop-in for the same endpoint and the same app.
# MAGIC
# MAGIC The class is defined here rather than imported from `src/crfs/`: a served model
# MAGIC has no access to the bundle's workspace files. Same rule as notebooks 02, 08, 22
# MAGIC and 30.
# COMMAND ----------
import pickle
import tempfile

art_dir = tempfile.mkdtemp(prefix="cr_gpu_")
torch.save(result["state_dict"], os.path.join(art_dir, "state_dict.pt"))
with open(os.path.join(art_dir, "spec.pkl"), "wb") as fh:
    pickle.dump({"encoder": result["encoder"], "feature_cols": result["feature_cols"],
                 "categorical": result["categorical"], "n_inputs": result["n_inputs"],
                 "hidden": result["hidden"]}, fh)


class GpuRailRanker(mlflow.pyfunc.PythonModel):
    """The torch rail ranker, scoring on CPU at serving time.

    Training needed the accelerator; inference on a 60-feature MLP over tens of rows
    does not, and a CPU endpoint is both cheaper and what the latency benchmark in
    notebook 25 already characterises.
    """

    def load_context(self, context):
        import pickle

        import torch
        import torch.nn as nn

        with open(context.artifacts["spec"], "rb") as fh:
            spec = pickle.load(fh)
        self._enc = spec["encoder"]
        self._features = spec["feature_cols"]
        self._categorical = spec["categorical"]

        layers, prev = [], spec["n_inputs"]
        for h in spec["hidden"]:
            layers += [nn.Linear(prev, h), nn.ReLU(), nn.Dropout(0.1)]
            prev = h
        layers += [nn.Linear(prev, 1)]
        self._model = nn.Sequential(*layers)
        self._model.load_state_dict(torch.load(context.artifacts["state_dict"],
                                               map_location="cpu"))
        self._model.eval()
        self._torch = torch

    def _encode(self, df):
        import numpy as np
        import pandas as _pd

        out = np.empty((len(df), len(self._features) + len(self._categorical)), dtype="float32")
        for i, c in enumerate(self._features):
            # A Series, never a scalar: every looked-up feature is optional in the
            # signature, so any of them can be missing from the request frame.
            col = df[c] if c in df else _pd.Series(0.0, index=df.index)
            v = _pd.to_numeric(col, errors="coerce").astype("float64").fillna(0.0)
            s = self._enc["stats"].get(c, {"mean": 0.0, "scale": 1.0})
            out[:, i] = ((v - s["mean"]) / s["scale"]).to_numpy(dtype="float32")
        for j, c in enumerate(self._categorical):
            m = self._enc["encoders"].get(c, {})
            col = df[c] if c in df else _pd.Series("unknown", index=df.index)
            out[:, len(self._features) + j] = (col.astype(str)
                                               .map(lambda x, m=m: m.get(x, len(m)))
                                               .astype("float32").to_numpy(dtype="float32"))
        return out

    def predict(self, context, model_input, params=None):
        import pandas as _pd

        df = model_input if isinstance(model_input, _pd.DataFrame) else _pd.DataFrame(model_input)
        x = self._torch.from_numpy(self._encode(df))
        with self._torch.no_grad():
            prob = self._torch.sigmoid(self._model(x).squeeze(-1)).numpy()
        out = _pd.DataFrame({
            "rail_id": (df["rail_id"].astype(str) if "rail_id" in df
                        else _pd.Series([""] * len(df), index=df.index)),
            "engagement_probability": prob.astype(float),
        }, index=df.index)
        group = df["viewer_id"].astype(str) if "viewer_id" in df else _pd.Series("_", index=df.index)
        out["rail_rank"] = (out.groupby(group.values)["engagement_probability"]
                            .rank(ascending=False, method="first").astype(int))
        return out


class _LocalCtx:
    artifacts = {"state_dict": os.path.join(art_dir, "state_dict.pt"),
                 "spec": os.path.join(art_dir, "spec.pkl")}


# Exercise the wrapper the way Model Serving will, before logging. The same failure in
# a live endpoint returns a 500 with no traceback and no line number.
_probe = GpuRailRanker()
_probe.load_context(_LocalCtx())
# The export carries `ts` and the label-side columns back for training (see §3), but the
# endpoint never sends them. The input example below becomes the raw model's signature,
# so anything left in it is a REQUIRED serving input. v5 was registered with `ts` in it
# and failed every request once served --
#   MlflowException: Model is missing inputs ['ts'].
# -- found by notebook 33's canary gate (docs/verification_log.md V96). Notebook 22 builds
# its example from the training frame, which never had `ts`, and asserts the same thing.
serving_like = (TG.load_frame(parquet_path)
                .drop(columns=["engaged", "ts"] + LABEL_SIDE, errors="ignore")
                .head(16))
assert not ({"ts", "engaged"} | set(LABEL_SIDE)) & set(serving_like.columns), \
    "training-only columns must not reach the signature or they become required inputs"
for label, frame in [("one rail", serving_like.head(1)),
                     ("one viewer, many rails", serving_like),
                     ("features absent (lookup miss)",
                      serving_like.head(3).drop(columns=FEATURE_COLS, errors="ignore")),
                     # Every looked-up feature is optional in the signature, so the
                     # endpoint can hand predict() only the seven request fields.
                     ("request keys only", serving_like[R.RAIL_REQUEST_KEYS].head(5))]:
    got = _probe.predict(None, frame)
    assert len(got) == len(frame) and got["engagement_probability"].notna().all(), label
    print(f"self-test {label:34s} -> {len(got)} rows, ranks {sorted(got['rail_rank'])[:6]}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 6 · Log it with the feature spec attached
# MAGIC
# MAGIC `fe.log_model(training_set=…)` is what keeps this model interchangeable with the
# MAGIC sklearn one: the four lookups and five request-time functions travel inside the
# MAGIC model version, so a serving endpoint retrieves features itself and the caller
# MAGIC still sends seven request fields.
# MAGIC
# MAGIC The version is tagged with the definition fingerprint from
# MAGIC [`src/crfs/versioning.py`](../../src/crfs/versioning.py), so notebook 31's drift
# MAGIC report has a baseline for this model from the moment it exists.
# COMMAND ----------
if cfg.extras["gpu_register"].strip().lower() != "true":
    print("gpu_register=false, stopping before registration")
    dbutils.notebook.exit(TG.summary(result))
# COMMAND ----------
from mlflow.tracking import MlflowClient

mc = MlflowClient()

with mlflow.start_run(run_name="crunchyroll_rail_ranker_gpu") as run:
    fe.log_model(
        model=GpuRailRanker(),
        artifact_path="rail_ranker_gpu",
        flavor=mlflow.pyfunc,
        training_set=training_set,
        artifacts=_LocalCtx.artifacts,
        input_example=serving_like.head(3),
        extra_pip_requirements=[f"torch=={torch.__version__.split('+')[0]}"],
    )
    mlflow.log_metrics({"holdout_auc": float(result["holdout_auc"] or 0.0),
                        "train_seconds": float(result["seconds"]),
                        "train_rows": float(result["train_rows"])})
    mlflow.log_params({"estimator": "torch_mlp", "device": result["device"],
                       "gpu": result["gpu_name"] or "none", "epochs": EPOCHS,
                       "batch_size": BATCH, "lr": LR, "hidden": result["hidden"],
                       "label_sample_frac": FRAC})
    run_id = run.info.run_id

# Registered in a second step rather than via registered_model_name. The shared
# metastore this was built on sits at its 5,000-registered-model quota, and MLflow's
# register path calls create_registered_model first: the quota check fires before the
# already-exists check, so even a new VERSION of an existing model failed with
#   QUOTA_EXCEEDED: Cannot create 1 Registered Model(s) ... (limit: 5000)
# Creating the version directly needs no new model. (verification_log.md V97)
model_uri = f"runs:/{run_id}/rail_ranker_gpu"
try:
    mc.get_registered_model(MODEL)
    version = int(mc.create_model_version(MODEL, source=model_uri, run_id=run_id).version)
except mlflow.exceptions.RestException as e:
    if "RESOURCE_DOES_NOT_EXIST" not in str(e) and "NOT_FOUND" not in str(e):
        raise
    version = int(mlflow.register_model(model_uri, MODEL).version)
print(f"registered {MODEL} v{version} (run {run_id})")

spec = V.feature_spec_of(f"models:/{MODEL}/{version}")
for k, val in V.training_tags(spark, spec).items():
    mc.set_model_version_tag(MODEL, str(version), k, val)
    print(f"  tagged {k}={val}")
mc.set_registered_model_alias(MODEL, "challenger", str(version))
print(f"  alias @challenger -> v{version}")

print("\nfeature spec inside the GPU model:")
print(V.render_spec(spec))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 7 · Is it interchangeable? Score the same rows both ways
# MAGIC
# MAGIC The claim worth checking is not "the GPU model is better" — on 16 synthetic rails
# MAGIC it need not be. It is that **the feature layer, the request contract and the
# MAGIC deployment path are unchanged**, so choosing an estimator is not an architectural
# MAGIC decision. `score_batch` against both models resolves the same features from the
# MAGIC same tables, and the rank correlation says how differently they order rails.
# COMMAND ----------
from scipy.stats import spearmanr

score_input = (labels.select("viewer_id", "rail_id", "ts", "device", "locale",
                             "hour_of_day", "day_of_week", "request_epoch_s")
               .limit(400))

# `env_manager="virtualenv"` is required for THIS model and not for the sklearn one.
# score_batch evaluates the model inside a Spark UDF, and the executor environment is not
# the notebook's: with the default env_manager the worker imports the model directly and
# fails with `ModuleNotFoundError: No module named 'torch'`, reported as a
# PythonException from mlflow/pyfunc rather than as a missing dependency. Restoring the
# logged environment costs a minute of setup per worker and is what makes a torch model
# scoreable through Spark at all.
def score_with_env(model_uri, df):
    """Try hardest-first, and never fail the run over an optional comparison.

    `score_batch` evaluates the model inside a Spark UDF, and the executor environment is
    not the notebook's: the worker imports the model directly and raises
    `ModuleNotFoundError: No module named 'torch'`, surfaced as a PythonException from
    mlflow/pyfunc. `env_manager="virtualenv"` is meant to rebuild the logged environment
    there; on this workspace's serverless compute it did not succeed either.

    So this is reported as a platform limitation rather than dressed up or crashed over.
    The model itself is already trained, registered, tagged and aliased by the cells above;
    what cannot be demonstrated here is *batch scoring a torch model through a Spark UDF*.
    Model Serving is unaffected -- it builds the model's own environment when it deploys.
    """
    attempts = [("virtualenv", {"env_manager": "virtualenv"}),
                ("local", {})]
    for label, kw in attempts:
        try:
            return fe.score_batch(model_uri=model_uri, df=df, **kw).toPandas(), label
        except Exception as e:
            print(f"  score_batch({label}) failed: {type(e).__name__}: {str(e)[:180]}")
    return None, "unavailable"


gpu_scored, gpu_env = score_with_env(f"models:/{MODEL}/{version}", score_input)
comparison = {}
if gpu_scored is None:
    comparison = {"error": "score_batch could not run the torch model in a Spark worker "
                           "on this workspace (torch missing in the executor environment, "
                           "and env_manager=virtualenv did not resolve it)"}
    print("\nGPU model scored: NOT via score_batch.", comparison["error"])
    # The model is still exercised, on the driver, where torch exists. This proves the
    # artifact loads and predicts; it does NOT exercise automatic feature lookup, and the
    # difference is stated rather than glossed.
    try:
        import mlflow.pyfunc as _pyfunc

        local_model = _pyfunc.load_model(f"models:/{MODEL}/{version}")
        local_pred = local_model.predict(serving_like)
        print(f"driver-side predict: {len(local_pred)} rows, "
              f"ranks {sorted(local_pred['rail_rank'])[:6]}")
        comparison["driver_side_rows"] = int(len(local_pred))
        comparison["note"] = ("driver-side predict works; feature values were supplied by "
                              "the exported training set, so automatic lookup is untested "
                              "here. The sklearn twin in notebook 26 covers that path.")
    except Exception as e:
        print("driver-side predict also failed:", type(e).__name__, str(e)[:160])
        comparison["driver_side_error"] = f"{type(e).__name__}: {str(e)[:160]}"
else:
    print(f"GPU model scored: {len(gpu_scored)} rows (env_manager={gpu_env})")
    sk_model = cfg.t("crunchyroll_rail_ranker")
    try:
        from scipy.stats import spearmanr

        sk_version = mc.get_model_version_by_alias(sk_model, "champion").version
        # The sklearn model needs no env restore: scikit-learn is in the worker already.
        sk_scored = fe.score_batch(model_uri=f"models:/{sk_model}/{sk_version}",
                                   df=score_input).toPandas()
        key = ["viewer_id", "rail_id"]
        merged = (gpu_scored[key + ["engagement_probability"]]
                  .merge(sk_scored[key + ["engagement_probability"]], on=key,
                         suffixes=("_gpu", "_sk")))
        rho = spearmanr(merged["engagement_probability_gpu"],
                        merged["engagement_probability_sk"]).statistic
        print(f"\nsklearn v{sk_version} vs GPU v{version} on {len(merged)} rows: "
              f"Spearman {rho:0.4f}")
        print("Both resolved their own features from their own pinned specs -- neither call")
        print("supplied a feature value.")
        comparison = {"sklearn_version": sk_version, "spearman": round(float(rho), 4),
                      "rows": len(merged)}
    except Exception as e:
        print("sklearn comparison unavailable:", type(e).__name__, str(e)[:160])
        comparison = {"error": f"{type(e).__name__}: {str(e)[:160]}"}
# COMMAND ----------
dbutils.notebook.exit(json.dumps({
    "model": MODEL, "version": version, "run_id": run_id,
    "device": result["device"], "gpu": result["gpu_name"],
    "holdout_auc": result["holdout_auc"], "train_seconds": result["seconds"],
    "train_rows": result["train_rows"], "n_features": len(FEATURE_COLS),
    "epochs": EPOCHS, "comparison_with_sklearn": comparison,
}, default=str))
