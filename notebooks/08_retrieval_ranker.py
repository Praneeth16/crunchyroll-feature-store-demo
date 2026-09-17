# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Two-model pattern: retrieval model feeds the ranker
# MAGIC
# MAGIC The funnel for "watch next" recommendations:
# MAGIC
# MAGIC 1. **Retrieval**: 132 titles → 60 candidates (fast, approximate)
# MAGIC 2. **Entitlement**: 60 candidates → N eligible (policy filter)
# MAGIC 3. **Ranker**: N eligible → 25 ranked (slow, expensive, precise)
# MAGIC
# MAGIC The retrieval model learns implicit viewer-title affinities using Truncated SVD
# MAGIC on the play matrix. Viewer factors become a governed feature table, published
# MAGIC to Lakebase. Item factors stay in the model artifact.
# MAGIC
# MAGIC Say:
# MAGIC > "Retrieval is the gate, ranker is the judge. Both read the same Lakebase
# MAGIC > features. The viewer side moves to the feature store (governed, auditable,
# MAGIC > composable). The item side lives in the model artifact — at Crunchyroll
# MAGIC > scale this moves to Vector Search, but the request contract stays the same."
# COMMAND ----------
# MAGIC %pip install databricks-feature-engineering scikit-learn --quiet
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import json
import os, sys, json, time
_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _root not in sys.path: sys.path.insert(0, _root)
from src.crfs.config import Config

cfg = Config.from_widgets(dbutils)
print(cfg.describe())
# COMMAND ----------
import numpy as np
import pandas as pd
import mlflow
from databricks.sdk import WorkspaceClient
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from src.crfs import candidates, ops

mlflow.set_registry_uri("databricks-uc")
spark.sql(f"USE {cfg.fq}")
fe = FeatureEngineeringClient()
w = WorkspaceClient()

EXPERIMENT = f"/Users/{spark.sql('SELECT current_user()').first()[0]}/crunchyroll_retriever_experiment"
mlflow.set_experiment(EXPERIMENT)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Build the play matrix and factorize
# COMMAND ----------
# Restrict to training window (exclude last 10 days) to prevent leakage
max_ts = spark.sql(f"SELECT MAX(event_ts) as m FROM {cfg.t('engagement_events')}").first()["m"]
cutoff_ts = pd.Timestamp(max_ts) - pd.Timedelta(days=10)

events_pdf = spark.sql(f"""
    SELECT DISTINCT viewer_id, title_id
    FROM {cfg.t('engagement_events')}
    WHERE event_type = 'complete' AND event_ts <= '{cutoff_ts}'
""").toPandas()

titles_pdf = spark.sql(f"SELECT title_id FROM {cfg.t('titles')}").toPandas()
viewers_pdf = spark.sql(f"SELECT viewer_id FROM {cfg.t('viewers')}").toPandas()

print(f"Training window: up to {cutoff_ts.date()}")
print(f"Unique viewer-title pairs: {len(events_pdf)}")
print(f"Total viewers: {len(viewers_pdf)}, total titles: {len(titles_pdf)}")

# Build implicit matrix
from scipy.sparse import csr_matrix
viewer_map = {v: i for i, v in enumerate(sorted(viewers_pdf["viewer_id"].unique()))}
title_map = {t: i for i, t in enumerate(sorted(titles_pdf["title_id"].unique()))}

rows = [viewer_map[r["viewer_id"]] for _, r in events_pdf.iterrows()]
cols = [title_map[r["title_id"]] for _, r in events_pdf.iterrows()]
matrix = csr_matrix((np.ones(len(rows)), (rows, cols)),
                    shape=(len(viewer_map), len(title_map)))

print(f"Play matrix: {matrix.shape[0]} viewers x {matrix.shape[1]} titles, {matrix.nnz} plays")

# COMMAND ----------
# Factorize with Truncated SVD
from sklearn.decomposition import TruncatedSVD

svd = TruncatedSVD(n_components=8, random_state=42)
U = svd.fit_transform(matrix)  # (viewers, 8)
V = svd.components_.T  # (titles, 8)
s = svd.singular_values_  # (8,)

# Fold singular values into item side (typical for retrieval)
V_scaled = V @ np.diag(s)

print(f"SVD: U={U.shape}, V={V_scaled.shape}, s={s}")
print(f"Explained variance: {sum(svd.explained_variance_ratio_):.4f}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Publish viewer factors as a governed feature table
# MAGIC
# MAGIC The headline: viewer embeddings are themselves features, served from Lakebase.
# COMMAND ----------
# Build viewer embedding table
viewer_embeddings_pdf = pd.DataFrame({
    "viewer_id": [list(viewer_map.keys())[i] for i in range(len(viewer_map))],
    **{f"vf_{j}": U[:, j] for j in range(U.shape[1])}
})

# Create the table in UC via the Feature Engineering client so it is a registered
# feature table with primary-key metadata. A plain saveAsTable is NOT a feature
# table -- the endpoint cannot resolve the lookup key, and create_training_set
# will reject the FeatureLookup at training time.
# Local driver paths are fine for model artifacts (they are packaged by MLflow),
# but never hand a /tmp/ path to Spark.
embedding_sdf = spark.createDataFrame(viewer_embeddings_pdf)
EMB = cfg.t("viewer_embedding_current")


def is_feature_table(full_name: str) -> bool:
    """A feature table has a PRIMARY KEY constraint. A table created with a plain
    saveAsTable does not, and publish_table rejects it with
    'Tables without primary keys cannot be published on Databricks Online Feature
    Store.' -- so an earlier non-feature-table version of this table has to be
    replaced rather than merged into."""
    catalog, schema, table = full_name.split(".")
    rows = spark.sql(f"""
        SELECT constraint_type FROM {catalog}.information_schema.table_constraints
        WHERE table_schema = '{schema}' AND table_name = '{table}'
          AND constraint_type = 'PRIMARY KEY'
    """).collect()
    return len(rows) > 0


if spark.catalog.tableExists(EMB) and is_feature_table(EMB):
    fe.write_table(name=EMB, df=embedding_sdf, mode="merge")
    print(f"{EMB}: merged {embedding_sdf.count()} rows")
else:
    if spark.catalog.tableExists(EMB):
        print(f"{EMB} exists but is not a feature table (no primary key) - replacing")
        ops.drop_synced_if_exists(w, cfg.t("online_viewer_embedding"))
        spark.sql(f"DROP TABLE IF EXISTS {EMB}")
    fe.create_table(
        name=EMB,
        primary_keys=["viewer_id"],
        df=embedding_sdf,
        description="Viewer SVD factors from the retrieval model - the retriever's own "
                    "representation, served from Lakebase by the same publish path")
    spark.sql(f"ALTER TABLE {EMB} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    print(f"{EMB}: created as a feature table, {embedding_sdf.count()} rows")
print(f"Created/updated {cfg.t('viewer_embedding_current')}")

# Get current commit version
source_version = ops.source_commit_version(spark, cfg.t("viewer_embedding_current"))
print(f"Source table version: {source_version}")

# Publish to online store
print("Publishing viewer embeddings to online store...")
try:
    # publish_or_refresh publishes the first time and refreshes the existing
    # pipeline after that -- publish_table is a create, not an upsert, and
    # get_online_store takes its argument by keyword only.
    action = ops.publish_or_refresh(
        w, fe, cfg.online_store,
        cfg.t("viewer_embedding_current"),
        cfg.t("online_viewer_embedding"),
        publish_mode="TRIGGERED")
    print(f"{cfg.t('online_viewer_embedding')}: {action}")
except Exception as e:
    if "already exists" in str(e):
        print(f"Online table {cfg.t('online_viewer_embedding')} already exists")
    else:
        raise

# Wait for sync
print("Waiting for online sync...")
ops.wait_for_sync(w, cfg.t("online_viewer_embedding"), min_commit_version=source_version)
print("Online sync complete")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Define pyfunc retriever with built-in item factors and title index
# COMMAND ----------
import pickle

# Build title index
title_id_list = sorted(title_map.keys())
title_popularity = spark.sql(f"""
    SELECT title_id, intrinsic_popularity
    FROM {cfg.t('titles')}
    ORDER BY title_id
""").toPandas().set_index("title_id").loc[title_id_list, "intrinsic_popularity"].values

# Item factors (already scaled by singular values)
V_scaled_by_title = V_scaled[[title_map[t] for t in title_id_list]]

print(f"Item factors shape: {V_scaled_by_title.shape}")

# COMMAND ----------
# Create pyfunc class with item factors baked in
class CrunchyrollRetriever(mlflow.pyfunc.PythonModel):
    """Retrieves top-k candidate titles by viewer-title affinity.

    At serving time, the endpoint looks up the viewer's embedding (8D) from
    Lakebase, dot-products against pre-computed item factors, and returns
    top-k title IDs sorted by score.

    Scale note: item factors are currently baked as a dense array (132 titles x 8 dims).
    Production moves this to a Vector Search index and the request contract is unchanged.
    """

    def load_context(self, context):
        import pickle
        with open(context.artifacts["item_factors"], "rb") as f:
            self.V_scaled = pickle.load(f)
        with open(context.artifacts["title_ids"], "rb") as f:
            self.title_ids = pickle.load(f)
        with open(context.artifacts["title_popularity"], "rb") as f:
            self.title_popularity = pickle.load(f)

    def predict(self, context, model_input):
        """Input: {"viewer_id": str, "top_k": int}
        Output: [{"title_id": str, "retrieval_score": float}, ...]
        """
        import json
        import numpy as np
        import pandas as pd
        results = []

        for _, row in model_input.iterrows():
            viewer_id = row.get("viewer_id")
            top_k = int(row.get("top_k", 60))

            # Viewer embedding lookup happens at request time via feature lookup
            # Here we assume the vector is passed in as vf_0..vf_7 columns
            viewer_vec = np.array([float(row.get(f"vf_{j}", 0.0)) for j in range(8)])

            # Dot product with all items
            scores = self.V_scaled @ viewer_vec  # (132,)

            # Add small popularity bonus for tie-breaking
            popularity_bonus = 0.01 * self.title_popularity
            scores = scores + popularity_bonus

            # Top-k
            top_indices = np.argsort(-scores)[:top_k]

            # Return list of {title_id, score}
            candidates_list = [
                {"title_id": str(self.title_ids[i]), "retrieval_score": float(scores[i])}
                for i in top_indices
            ]
            results.append(json.dumps(candidates_list))

        # One JSON string per input row, always -- never a bare list, and never a
        # different type for a single row.
        #
        # Returned as a single-column pandas DataFrame, which is the shape that Unity
        # Catalog registration actually accepts here. The previous comment claimed a
        # numpy string array "is what MLflow can infer an OUTPUT schema from"; the
        # measured truth is the opposite. With the errors finally surfaced into the task
        # output, BOTH registration recipes failed identically with:
        #
        #   MlflowException: Model passed for registration contained a signature that
        #   includes only inputs.
        #
        # i.e. MLflow could not infer an output spec from a `<U...` unicode array. The
        # two models in this repo that register cleanly return a float array (notebook
        # 02) and a DataFrame (notebook 22); a DataFrame gives an explicit named column,
        # so that is what this returns.
        return pd.DataFrame({"candidates": results})

# Save artifacts
# A writable directory the driver actually owns. /tmp is not reliably writable on
# serverless: PermissionError: [Errno 13] Permission denied: '/tmp/cr_retriever/...'
import tempfile

ARTIFACT_DIR = tempfile.mkdtemp(prefix="cr_retriever_")
print("artifact dir:", ARTIFACT_DIR)
with open(os.path.join(ARTIFACT_DIR, "item_factors.pkl"), "wb") as f:
    pickle.dump(V_scaled_by_title, f)
with open(os.path.join(ARTIFACT_DIR, "title_ids.pkl"), "wb") as f:
    pickle.dump(title_id_list, f)
with open(os.path.join(ARTIFACT_DIR, "title_popularity.pkl"), "wb") as f:
    pickle.dump(title_popularity, f)

print("Retriever artifacts saved")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Compute recall metrics
# COMMAND ----------
# Holdout plays (last 10 days) for metric evaluation
holdout_pdf = spark.sql(f"""
    SELECT DISTINCT viewer_id, title_id
    FROM {cfg.t('engagement_events')}
    WHERE event_type = 'complete' AND event_ts > '{cutoff_ts}'
""").toPandas()

print(f"Holdout plays: {len(holdout_pdf)}")

# Compute recall@60 for SVD retriever
recall_svd_hits = 0
recall_svd_total = 0
popularity_hits = 0
random_hits = 0

for viewer_id in holdout_pdf["viewer_id"].unique():
    viewer_plays = set(holdout_pdf[holdout_pdf["viewer_id"] == viewer_id]["title_id"].values)
    if not viewer_plays:
        continue

    # Get viewer embedding
    if viewer_id in viewer_map:
        viewer_idx = viewer_map[viewer_id]
        viewer_vec = U[viewer_idx, :]

        # SVD retrieval: top 60
        scores = V_scaled @ viewer_vec
        top_60 = set([title_id_list[i] for i in np.argsort(-scores)[:60]])
        recall_svd_hits += len(viewer_plays & top_60)
        recall_svd_total += len(viewer_plays)

        # Popularity baseline: top 60 by intrinsic popularity
        pop_60 = set(sorted(title_id_list,
                           key=lambda t: -title_popularity[title_id_list.index(t)])[:60])
        popularity_hits += len(viewer_plays & pop_60)

        # Random baseline
        random_60 = set(np.random.choice(title_id_list, 60, replace=False))
        random_hits += len(viewer_plays & random_60)

recall_svd = recall_svd_hits / recall_svd_total if recall_svd_total > 0 else 0.0
recall_popularity = popularity_hits / recall_svd_total if recall_svd_total > 0 else 0.0
recall_random = random_hits / recall_svd_total if recall_svd_total > 0 else 0.0

print(f"\nRecall@60 metrics:")
print(f"  SVD retriever:      {recall_svd:.4f}")
print(f"  Popularity baseline: {recall_popularity:.4f}")
print(f"  Random baseline:     {recall_random:.4f}")

if recall_svd <= recall_popularity:
    print(f"\n! SVD recall ({recall_svd:.4f}) <= popularity ({recall_popularity:.4f})")
    print(f"  Recommendation: use popularity as baseline arm in production.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Register retriever with feature spec
# COMMAND ----------
# The training set exists to attach the feature spec to the model, so the endpoint
# knows to look vf_0..vf_7 up from Lakebase by viewer_id at request time.
#
# The frame must carry ONLY the lookup key. Passing the vf_* columns as well makes
# create_training_set refuse the lookup:
#   ValueError: DataFrame contains column names that match feature output names
#   specified in FeatureLookups: 'vf_0', ... Either remove these columns from the
#   DataFrame or FeatureLookups.
# top_k rides along because it is part of the request contract.
from pyspark.sql import functions as SF

spec_df = (spark.table(cfg.t("viewer_embedding_current"))
           .select("viewer_id")
           .limit(100)
           .withColumn("top_k", SF.lit(60).cast("bigint")))
# bigint, not int. This one cast decides whether the endpoint can be called at all.
# `cast("int")` is int32, so the derived signature enforced `top_k: integer`, while a
# JSON request integer arrives as int64 -- and MLflow refuses to narrow:
#   MlflowException: Incompatible input types for column top_k.
#                    Can not safely convert int64 to int32.
# which Model Serving then reports as the information-free `Error ''`. Exactly the same
# failure mode as vr_last_click_epoch_s on the rail ranker (verification_log V30/V37):
# an integral width mismatch between the training frame and the request, invisible until
# a live query. The rule that came out of that one applies here: keep request-carried
# numerics at their widest type end to end rather than matching them narrowly.

# viewer_id must STAY in the training set. It is the lookup key, but it is also what
# predict() reads to pick the viewer's factors -- and fe.log_model infers the output
# schema by running the model on an example drawn from this set. Excluding viewer_id
# made that run fail, so no output schema was inferred, and Unity Catalog then
# rejected the registration with "a signature that includes only inputs" -- six times,
# with the real cause three layers away from the error message.
training_set = fe.create_training_set(
    df=spec_df,
    feature_lookups=[
        FeatureLookup(
            table_name=cfg.t("viewer_embedding_current"),
            lookup_key="viewer_id"
        )
    ],
    label=None,
)

# Unity Catalog requires a signature with BOTH inputs and outputs. fe.log_model
# builds the signature itself -- an explicit `signature=` kwarg is ignored -- and it
# infers the output side by running the model on `input_example`. So supply the
# example and let it infer, exactly as notebook 02 does for the ranker.
input_example_pd = spec_df.limit(3).toPandas()

# UC registration of this model is UNRESOLVED. fe.log_model derives the signature
# itself and never produces an output schema for this pyfunc, so Unity Catalog refuses
# it with "a signature that includes only inputs". Seven approaches were tried and are
# recorded in docs/verification_log.md: signature= kwarg, input_example,
# mlflow.models.set_signature, infer_input_example=True, a typed pandas Series, a numpy
# string array, and keeping viewer_id in the training set.
#
# Everything else in this notebook is verified and does not depend on it: the SVD, the
# published viewer_embedding_current feature table and its online mirror, and the
# recall metrics. So registration is attempted and, if it fails, the notebook reports
# that and continues rather than failing the task and blocking the demo.
RETRIEVER_REGISTERED = False
retriever_version = None

with mlflow.start_run(run_name="crunchyroll_retriever") as run:
    # input_example is what lets fe.log_model infer the output schema: it runs the
    # model on the example. Without it the logged signature has inputs only and UC
    # rejects the registration.
    log_kwargs = dict(
        model=CrunchyrollRetriever(),
        artifact_path="cr_retriever",
        flavor=mlflow.pyfunc,
        training_set=training_set,
        input_example=input_example_pd,
        artifacts={
            "item_factors": os.path.join(ARTIFACT_DIR, "item_factors.pkl"),
            "title_ids": os.path.join(ARTIFACT_DIR, "title_ids.pkl"),
            "title_popularity": os.path.join(ARTIFACT_DIR, "title_popularity.pkl"),
        },
    )
    # Two recipes, tried in order -- the same loop notebook 22 uses for the rail
    # ranker. UC needs a signature with BOTH inputs and outputs, and fe.log_model
    # derives the output spec by *running the model* on an example. Which recipe
    # produces that has varied between models:
    #
    #   * explicit `input_example`  -- what notebook 02's ranker registers with, and
    #     the only thing this notebook used to try;
    #   * `infer_input_example=True` -- recorded in docs/verification_log.md as the
    #     resolution after this retriever failed six times with "a signature that
    #     includes only inputs", but never actually wired in here. Notebook 22 has
    #     been carrying both since it was written; this notebook was left behind.
    #
    # Unlike notebook 22 this does not raise when both fail. The SVD, the published
    # embedding table, its online mirror and the recall metrics are all independent of
    # registration, and blocking the whole horizontal pipeline on a retriever endpoint
    # nobody queries would be the wrong trade.
    attempts = [("explicit input_example", dict(input_example=input_example_pd)),
                ("infer_input_example=True", dict(infer_input_example=True))]
    registered_with = None
    REGISTRATION_ERRORS = {}
    for _label, _extra in attempts:
        try:
            fe.log_model(registered_model_name=cfg.t("crunchyroll_retriever"),
                         **{k: v for k, v in log_kwargs.items() if k != "input_example"},
                         **_extra)
            RETRIEVER_REGISTERED = True
            registered_with = _label
            print(f"registered to Unity Catalog with {_label}")
            break
        except Exception as e:
            REGISTRATION_ERRORS[_label] = f"{type(e).__name__}: {str(e)[:400]}"
            print(f"UC registration via {_label} failed: {REGISTRATION_ERRORS[_label]}")
    if registered_with is None:
        # Third recipe: stop asking fe.log_model to infer an output spec, and state it.
        #
        # Both earlier recipes fail with the *same* MlflowException -- "a signature that
        # includes only inputs" -- whether the model returns a numpy string array or a
        # single-column DataFrame. So the output TYPE is not the problem: fe.log_model is
        # not producing an output spec for this model at all. MLflow's own documented
        # remedy for that error is to attach the signature explicitly, so:
        #   1. log through fe.log_model (unregistered) to keep the feature spec attached,
        #   2. set a signature carrying BOTH inputs and outputs on the logged artifact,
        #   3. register that URI with mlflow.register_model.
        # Step 1 has to stay fe.log_model, or the model loses automatic feature lookup
        # and the endpoint would expect vf_0..vf_7 from the caller.
        try:
            from mlflow.models import ModelSignature, set_signature
            from mlflow.types.schema import Schema, ColSpec

            info = fe.log_model(**log_kwargs)
            uri = getattr(info, "model_uri", None) or f"runs:/{run.info.run_id}/cr_retriever"
            sig = ModelSignature(
                inputs=Schema(
                    [ColSpec("string", "viewer_id"), ColSpec("long", "top_k")]
                    + [ColSpec("double", f"vf_{j}", required=False) for j in range(8)]),
                outputs=Schema([ColSpec("string", "candidates")]),
            )
            set_signature(uri, sig)
            mv = mlflow.register_model(uri, cfg.t("crunchyroll_retriever"))
            RETRIEVER_REGISTERED = True
            registered_with = "explicit signature + register_model"
            print(f"registered to Unity Catalog with {registered_with} "
                  f"(version {getattr(mv, 'version', '?')})")
        except Exception as e:
            REGISTRATION_ERRORS["explicit signature + register_model"] = (
                f"{type(e).__name__}: {str(e)[:400]}")
            print("third recipe failed:",
                  REGISTRATION_ERRORS["explicit signature + register_model"])
            print("logging to the run without registering, so the artifacts and metrics "
                  "are still available")
    if registered_with is not None:
        mlflow.log_param("registered_with", registered_with)

    mlflow.log_metric("recall_at_60", recall_svd)
    mlflow.log_metric("recall_popularity_baseline", recall_popularity)
    mlflow.log_metric("recall_random_baseline", recall_random)
    mlflow.log_param("n_components", 8)
    mlflow.log_param("matrix_size", f"{matrix.shape[0]}x{matrix.shape[1]}")
    run_id = run.info.run_id

from mlflow.tracking import MlflowClient

mc = MlflowClient()
_name = cfg.t('crunchyroll_retriever')
_versions = list(mc.search_model_versions("name='%s'" % _name))
_mine = [v for v in _versions if v.run_id == run_id]
# This run's own version first -- a concurrent run must not make this deploy
# someone else's model. Guard the fallback: search_model_versions can come back
# empty immediately after registering, and max() on an empty sequence raises.
if _mine:
    retriever_version = int(_mine[0].version)
elif _versions:
    retriever_version = max(int(v.version) for v in _versions)
else:
    RETRIEVER_REGISTERED = False
    print("registration reported success but no version is queryable yet; "
          "skipping the endpoint deployment")

if RETRIEVER_REGISTERED:
    print(f"registered {cfg.t('crunchyroll_retriever')} version {retriever_version}")

print(f"Registered {cfg.t('crunchyroll_retriever')} | run: {run_id}")

# retriever_version was resolved above from this run's own model version, not the
# registry max, so a concurrent run cannot make this deploy someone else's model.
print(f"Retriever version: {retriever_version}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Deploy retriever to serving endpoint
# COMMAND ----------
if not RETRIEVER_REGISTERED:
    print("retriever is not registered in Unity Catalog, so there is nothing to serve.")
    print("The SVD, the published viewer_embedding_current feature table and its online")
    print("mirror, and the recall metrics above are all verified and unaffected.")
    print("See docs/verification_log.md for the seven registration approaches tried.")
    dbutils.notebook.exit(json.dumps({
        "retriever_registered": False,
        "recall_at_60": float(recall_svd),
        "recall_popularity": float(recall_popularity),
        "recall_random": float(recall_random),
        "embedding_table": cfg.t("viewer_embedding_current"),
        "online_embedding_table": cfg.t("online_viewer_embedding"),
        "note": "endpoint deployment skipped: UC registration unresolved",
        "registration_errors": REGISTRATION_ERRORS,
    }))
# COMMAND ----------
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput, ServedEntityInput,
)

cfg_endpoint = EndpointCoreConfigInput(
    name=cfg.retriever_endpoint,
    served_entities=[ServedEntityInput(
        entity_name=cfg.t("crunchyroll_retriever"),
        entity_version=retriever_version,
        workload_size="Small",
        scale_to_zero_enabled=True,
    )],
)

def with_conflict_retry(fn, what):
    for attempt in range(10):
        try:
            return fn()
        except Exception as e:
            if "ResourceConflict" in type(e).__name__ and attempt < 9:
                print(f"[{attempt}] {what}: entities still updating, retry in 30s")
                time.sleep(30)
            else:
                raise

existing = [e for e in w.serving_endpoints.list() if e.name == cfg.retriever_endpoint]
if not existing:
    print(f"Creating endpoint {cfg.retriever_endpoint}")
    with_conflict_retry(lambda: w.serving_endpoints.create(name=cfg.retriever_endpoint, config=cfg_endpoint), "create")
else:
    print(f"Updating endpoint {cfg.retriever_endpoint}")
    with_conflict_retry(lambda: w.serving_endpoints.update_config(
        name=cfg.retriever_endpoint, served_entities=cfg_endpoint.served_entities), "update")

# Wait for ready
print("Waiting for endpoint to be ready...")
for i in range(90):
    ep = w.serving_endpoints.get(cfg.retriever_endpoint)
    state = ep.state.ready.value if ep.state and ep.state.ready else "UNKNOWN"
    cfg_state = ep.state.config_update.value if ep.state and ep.state.config_update else ""
    if i % 10 == 0:
        print(f"  [{i}] ready={state} config_update={cfg_state}")
    if state == "READY" and str(cfg_state) in ("NOT_UPDATING", ""):
        print(f"  Endpoint ready at [{i}]")
        break
    time.sleep(20)

print(f"Endpoint {cfg.retriever_endpoint} deployed")

# COMMAND ----------
# MAGIC %md
# MAGIC ## End-to-end funnel: retrieval → entitlement → ranking
# MAGIC
# MAGIC Say:
# MAGIC > "Now the full chain. One viewer, retriever narrows 132 down to 60,
# MAGIC > entitlement filters for policy, the ranker orders the rest.
# MAGIC > Same Lakebase features every step of the way."
# COMMAND ----------
# Pick most active viewer
most_active_vid = candidates.most_active_viewer(spark, cfg.fq)
print(f"Demo viewer: {most_active_vid}")

# Stage 1: Retrieval (60 candidates)
t0 = time.perf_counter()
retrieved, retrieval_ms = candidates.query_retriever(w, cfg.retriever_endpoint, most_active_vid, 60)
retrieval_ms_elapsed = (time.perf_counter() - t0) * 1000.0

if isinstance(retrieved, list) and len(retrieved) > 0 and isinstance(retrieved[0], dict):
    retrieved_titles = [r.get("title_id") for r in retrieved]
elif isinstance(retrieved, list):
    retrieved_titles = retrieved
else:
    retrieved_titles = []

print(f"Stage 1 (Retrieval): 132 → {len(retrieved_titles)} | {retrieval_ms_elapsed:.0f} ms")

# Stage 2: Entitlement (policy filter)
entitlement_df = spark.sql(f"""
    SELECT title_id
    FROM {cfg.t('entitlements')}
    WHERE viewer_id = '{most_active_vid}' AND allowed
""").toPandas()
entitled_titles = set(entitlement_df["title_id"].values)
eligible_titles = [t for t in retrieved_titles if t in entitled_titles]

print(f"Stage 2 (Entitlement): {len(retrieved_titles)} → {len(eligible_titles)} | policy filter")

# Stage 3: Ranker (25 top-ranked)
if eligible_titles:
    records = candidates.request_records(most_active_vid, eligible_titles, hour_of_day=21)
    ranked_scores, ranker_ms = candidates.query_ranker(w, cfg.ranker_endpoint, records)

    # Get title names
    title_names = spark.sql(f"""
        SELECT title_id, title_name
        FROM {cfg.t('titles')}
        WHERE title_id IN ({','.join([f"'{t}'" for t in eligible_titles])})
    """).toPandas().set_index("title_id")

    ranked_df = pd.DataFrame({
        "title_id": eligible_titles,
        "rank_score": ranked_scores
    }).sort_values("rank_score", ascending=False).head(25).reset_index(drop=True)

    ranked_df["title_name"] = ranked_df["title_id"].map(lambda t: title_names.loc[t, "title_name"] if t in title_names.index else "?")

    print(f"Stage 3 (Ranker): {len(eligible_titles)} → 25 | {ranker_ms:.0f} ms")
    print(f"\nTop 5 picks:")
    for idx, r in ranked_df.head(5).iterrows():
        print(f"  {idx+1}. {r['title_name']} ({r['title_id']}) - {r['rank_score']:.4f}")

    # Save funnel
    funnel = {
        "catalog_total": 132,
        "retrieved": len(retrieved_titles),
        "entitled": len(eligible_titles),
        "ranked": min(25, len(ranked_df)),
        "retrieval_ms": round(retrieval_ms_elapsed),
        "ranking_ms": round(ranker_ms),
    }

    mlflow.log_dict(funnel, "retrieval_ranker_funnel.json")
else:
    funnel = {
        "catalog_total": 132,
        "retrieved": len(retrieved_titles),
        "entitled": len(eligible_titles),
        "ranked": 0,
        "retrieval_ms": round(retrieval_ms_elapsed),
        "ranking_ms": 0,
    }
    mlflow.log_dict(funnel, "retrieval_ranker_funnel.json")

print(f"\nFunnel: {funnel}")

# COMMAND ----------
dbutils.notebook.exit(json.dumps({
    "retriever_model": cfg.t("crunchyroll_retriever"),
    "retriever_version": retriever_version,
    "recall_at_60": round(recall_svd, 4),
    "recall_popularity": round(recall_popularity, 4),
    "recall_random": round(recall_random, 4),
    "endpoint": cfg.retriever_endpoint,
    "funnel": funnel,
}))
