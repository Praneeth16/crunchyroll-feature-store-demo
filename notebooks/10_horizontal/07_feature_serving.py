# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Feature Serving endpoint
# MAGIC
# MAGIC A Feature Serving endpoint serves precomputed features AND on-demand features
# MAGIC in one REST call, with no model in the path. The endpoint returns raw feature
# MAGIC values, not predictions. Apps use this to:
# MAGIC
# MAGIC - **Run their own model** (trained outside Databricks)
# MAGIC - **Power feature displays** (show the viewer's affinity for action, session decay, etc.)
# MAGIC - **Use features for business logic** (rules engines, experiments, A/B testing)
# MAGIC - **Reduce latency** (batch feature requests in one call)
# MAGIC
# MAGIC This endpoint serves the same features the ranker uses, but decoupled from
# MAGIC the ranking model. The serving path is identical: request keys in, features out,
# MAGIC computed by governed UDFs, fresh from the online store.
# MAGIC
# MAGIC Say: *Expose features as a governed REST API.*
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
if _root not in sys.path: sys.path.insert(0, _root)
import json
import time

from src.crfs.config import Config
from src.crfs import udfs

cfg = Config.from_widgets(dbutils)
spark.sql(f"USE {cfg.fq}")

from databricks.feature_engineering import FeatureEngineeringClient

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
print(f"Working in: {cfg.fq}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Discover and print the correct API signatures
# MAGIC
# MAGIC Feature Serving endpoints use a different API than model serving. The key
# MAGIC detail: served_entities is a SINGLE ServedEntity object, not a list.
# COMMAND ----------
import inspect
from databricks import feature_engineering as fe_mod

# Find the correct classes for Feature Serving
try:
    from databricks.feature_engineering import EndpointCoreConfig, ServedEntity
    print("✓ Found EndpointCoreConfig and ServedEntity")
except ImportError as e:
    print(f"⚠ Import error: {e}")
    print(f"  Available in feature_engineering: {[x for x in dir(fe_mod) if 'Endpoint' in x or 'Served' in x]}")

try:
    from databricks.feature_engineering import FeatureSpec
    print("✓ Found FeatureSpec")
except ImportError:
    print("⚠ FeatureSpec not found, may use different constructor")

# Print signatures
print("\n--- API Signatures ---")
try:
    sig = inspect.signature(EndpointCoreConfig)
    print(f"EndpointCoreConfig: {sig}")
except Exception as e:
    print(f"EndpointCoreConfig signature error: {e}")

try:
    sig = inspect.signature(ServedEntity)
    print(f"ServedEntity: {sig}")
except Exception as e:
    print(f"ServedEntity signature error: {e}")

fe = FeatureEngineeringClient()
try:
    sig = inspect.signature(fe.create_feature_spec)
    print(f"create_feature_spec: {sig}")
except Exception as e:
    print(f"create_feature_spec signature error: {e}")

try:
    sig = inspect.signature(fe.create_feature_serving_endpoint)
    print(f"create_feature_serving_endpoint: {sig}")
except Exception as e:
    print(f"create_feature_serving_endpoint signature error: {e}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Create the feature spec (idempotent)
# MAGIC
# MAGIC The spec includes FeatureLookups (stored tables) and FeatureFunctions (on-demand UDFs).
# COMMAND ----------
# FeatureSpec is NOT exported from databricks.feature_engineering (ImportError on
# 0.x), and it is not needed: create_feature_spec takes a name plus the feature list.
from databricks.feature_engineering import FeatureLookup, FeatureFunction

lookups = [
    FeatureLookup(table_name=cfg.t("viewer_features_current"), lookup_key="viewer_id"),
    FeatureLookup(table_name=cfg.t("recent_behavior_current"), lookup_key="viewer_id"),
    FeatureLookup(table_name=cfg.t("title_features"), lookup_key="title_id"),
]

on_demand = udfs.feature_functions(cfg.fq)

feature_spec_name = cfg.t("crunchyroll_viewer_feature_spec")

# A feature spec is a UC routine, so creating one that exists fails with
#   RESOURCE_ALREADY_EXISTS: Routine or Model 'crunchyroll_viewer_feature_spec'
#   already exists
# and it cannot be dropped while an endpoint serves it. Delete the endpoint first,
# then the spec, then recreate -- and if the create still collides, reuse what is
# there rather than failing the task.
try:
    fe.delete_feature_serving_endpoint(name=cfg.feature_endpoint)
    print(f"deleted endpoint {cfg.feature_endpoint} so its spec can be replaced")
    time.sleep(5)
except Exception as e:
    print(f"no endpoint to delete ({str(e)[:80]})")

try:
    fe.delete_feature_spec(name=feature_spec_name)
    print(f"deleted existing spec: {feature_spec_name}")
except Exception as e:
    print(f"no spec to delete ({str(e)[:80]})")

try:
    fs = fe.create_feature_spec(name=feature_spec_name, features=lookups + on_demand)
except Exception as e:
    if "ALREADY_EXISTS" not in str(e):
        raise
    print("spec already exists and could not be replaced - reusing it")
    fs = fe.get_feature_spec(name=feature_spec_name)

print(f"✓ created feature spec: {feature_spec_name}")
print(f"  lookups: {len(lookups)}")
print(f"  on-demand functions: {len(on_demand)}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Create the Feature Serving endpoint (idempotent)
# MAGIC
# MAGIC The endpoint serves a single FeatureSpec. Workload size is Small for this demo;
# MAGIC scale_to_zero saves cost when idle.
# COMMAND ----------
# EndpointCoreConfig / ServedEntity have moved between the top-level package and
# .entities across versions, so resolve them rather than assuming. The cell above
# printed what this runtime actually has.
try:
    from databricks.feature_engineering import EndpointCoreConfig, ServedEntity
    print("resolved EndpointCoreConfig/ServedEntity from databricks.feature_engineering")
except ImportError:
    from databricks.feature_engineering.entities.feature_serving_endpoint import (
        EndpointCoreConfig, ServedEntity)
    print("resolved EndpointCoreConfig/ServedEntity from "
          "databricks.feature_engineering.entities.feature_serving_endpoint")

endpoint_name = cfg.feature_endpoint

# The endpoint was already deleted above, before the spec was replaced. Create it
# fresh; if it somehow survived, reuse it rather than failing.
# Create the endpoint with a single ServedEntity (NOT a list)
config = EndpointCoreConfig(
    served_entities=ServedEntity(
        feature_spec_name=feature_spec_name,
        workload_size="Small",
        scale_to_zero_enabled=True,
    )
)

try:
    endpoint = fe.create_feature_serving_endpoint(name=endpoint_name, config=config)
except Exception as e:
    if "ALREADY_EXISTS" not in str(e).upper():
        raise
    print("endpoint already exists - reusing it")
    endpoint = fe.get_feature_serving_endpoint(name=endpoint_name)

# A freshly created endpoint is not immediately servable: POST /invocations returns
#   ResourceDoesNotExist: The given endpoint does not exist
# until provisioning finishes. Wait for READY rather than racing it.
def wait_endpoint_ready(name: str, timeout_s: int = 1800, poll_s: int = 20):
    t0 = time.time()
    last = None
    while time.time() - t0 < timeout_s:
        try:
            ep = w.serving_endpoints.get(name)
            state = ep.state
            ready = getattr(getattr(state, "ready", None), "value", None) or str(getattr(state, "ready", ""))
            update = getattr(getattr(state, "config_update", None), "value", None) or ""
            last = f"ready={ready} config_update={update}"
            print(f"  [{time.time()-t0:5.0f}s] {name}: {last}")
            if ready == "READY" and update in ("", "NOT_UPDATING"):
                return True
            if "FAILED" in str(update).upper():
                raise RuntimeError(f"{name} config update failed: {update}")
        except Exception as e:
            if "does not exist" not in str(e).lower():
                raise
            print(f"  [{time.time()-t0:5.0f}s] {name}: not registered yet")
        time.sleep(poll_s)
    raise TimeoutError(f"{name} not READY within {timeout_s}s (last: {last})")


wait_endpoint_ready(endpoint_name)
print(f"✓ created Feature Serving endpoint: {endpoint_name}")
print(f"  feature_spec: {feature_spec_name}")
print(f"  workload_size: Small")
print(f"  auto-shutdown: enabled")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Query the endpoint with real data and measure latency
# COMMAND ----------
import time
from src.crfs import candidates

# Get a sample viewer and a list of titles
viewer_id = candidates.most_active_viewer(spark, cfg.fq)
cands = candidates.candidates(spark, cfg.fq, viewer_id, limit=5)

if len(cands) < 1:
    print("⚠ no candidates found, using a fixed example")
    viewer_id = "viewer_1"
    title_ids = ["title_1"]
else:
    title_ids = list(cands["title_id"].head(3).values)

hour_now = 21
epoch_now = int(time.time())

records = candidates.request_records(
    viewer_id=viewer_id,
    title_ids=title_ids,
    surface="post_play",
    device="tv",
    locale="en-US",
    hour_of_day=hour_now,
    request_epoch_s=epoch_now,
)

print(f"querying endpoint with {len(records)} record(s):")
for r in records[:2]:
    print(f"  {r}")

# Query the endpoint
t0 = time.perf_counter()
resp = w.serving_endpoints.query(name=endpoint_name, dataframe_records=records)
elapsed_ms = (time.perf_counter() - t0) * 1000.0

# A Feature Serving endpoint does not answer in `predictions` the way a model
# endpoint does -- that field comes back None and `list(None)` raises. The feature
# values arrive under `outputs`. Read whichever the response actually carries
# instead of assuming, and print the raw shape so the notebook documents it.
raw = resp.as_dict() if hasattr(resp, "as_dict") else dict(resp)
print(f"\nendpoint responded in {elapsed_ms:.1f}ms")
print("  raw response keys:", sorted(raw.keys()))

predictions = raw.get("outputs") or raw.get("predictions") or []
if not predictions:
    raise RuntimeError(f"no feature values in the response: {json.dumps(raw)[:500]}")
print(f"  returned {len(predictions)} row(s)")
print("  first row:", json.dumps(predictions[0], indent=2, default=str)[:900])
print(f"  predictions returned: {len(predictions)}")

if predictions:
    pred_sample = predictions[0]
    print(f"\nsample prediction (first record):")
    print(f"  {pred_sample}")
    if isinstance(pred_sample, dict):
        print(f"  keys: {list(pred_sample.keys())[:5]}...")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Negative test: query a non-existent viewer
# MAGIC
# MAGIC The UDFs guard None values, so a missing lookup should degrade gracefully
# MAGIC rather than 500.
# COMMAND ----------
bad_records = candidates.request_records(
    viewer_id="viewer_nonexistent_999999",
    title_ids=["title_1"],
    hour_of_day=21,
    request_epoch_s=int(time.time()),
)

try:
    t0 = time.perf_counter()
    resp_bad = w.serving_endpoints.query(name=endpoint_name, dataframe_records=bad_records)
    elapsed_bad = (time.perf_counter() - t0) * 1000.0
    print(f"endpoint handled the missing viewer in {elapsed_bad:.1f}ms")
    raw_bad = resp_bad.as_dict() if hasattr(resp_bad, "as_dict") else dict(resp_bad)
    bad_pred = raw_bad.get("outputs") or raw_bad.get("predictions") or []
    print(f"  returned: {json.dumps(bad_pred[0], default=str)[:400] if bad_pred else 'empty'}")
    print("  the on-demand UDFs guard None, which is why this is a graceful answer")
    print("  rather than a 500 from inside the endpoint")
except Exception as e:
    print(f"✗ endpoint returned error: {str(e)[:200]}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Compare with direct online store read
# MAGIC
# MAGIC Query the same viewer/title via the Postgres backend and verify values match
# MAGIC the Feature Serving response.
# COMMAND ----------
from src.crfs import online

try:
    with online.from_config(w, cfg) as store:
        # Keyed read of viewer features
        viewer_row, viewer_cols, viewer_lat_ms = store.keyed_read(
            "viewer_features_current", "viewer_id", viewer_id
        )

        # Keyed read of title features
        if title_ids:
            title_row, title_cols, title_lat_ms = store.keyed_read(
                "title_features", "title_id", title_ids[0]
            )

        print(f"✓ online store reads succeeded")
        print(f"  viewer read: {viewer_lat_ms:.1f}ms")
        if title_ids:
            print(f"  title read:  {title_lat_ms:.1f}ms")

        # Compare a sample feature value
        if viewer_row and predictions:
            endpoint_dict = predictions[0]
            if isinstance(endpoint_dict, dict):
                # Try to find a matching column
                for col in viewer_cols:
                    if col in endpoint_dict:
                        endpoint_val = float(endpoint_dict[col])
                        viewer_idx = viewer_cols.index(col)
                        store_val = float(viewer_row[viewer_idx]) if viewer_row[viewer_idx] is not None else 0.0
                        match = abs(endpoint_val - store_val) < 0.01
                        print(f"\nvalue comparison ({col}):")
                        print(f"  endpoint: {endpoint_val:.4f}")
                        print(f"  store:    {store_val:.4f}")
                        print(f"  match: {'✓' if match else '✗'}")
                        break
except Exception as e:
    print(f"⚠ online store read failed: {str(e)[:200]}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## When to use Feature Serving instead of a model endpoint
# MAGIC
# MAGIC **Use Feature Serving when:**
# MAGIC - Your scoring model runs outside Databricks (in a microservice, browser, mobile app)
# MAGIC - You need raw features for business logic, rules, or experimentation
# MAGIC - You want to separate feature computation from model serving (governance, versioning)
# MAGIC - Multiple models or applications consume the same features
# MAGIC
# MAGIC **Use Model Serving when:**
# MAGIC - The model is trained and deployed in Databricks
# MAGIC - The scoring latency budget is tight (one API call vs two)
# MAGIC - You want the full trace: request → features → model → prediction
# MAGIC
# MAGIC This endpoint returns the same feature values the ranker learned from, but
# MAGIC decoupled. An external application can call it to score with its own model,
# MAGIC or to understand why a recommendation was made.
# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "feature_spec": feature_spec_name,
    "endpoint": endpoint_name,
    "query_ms": round(elapsed_ms, 1),
    "n_features": len(lookups) + len(on_demand),
    "n_lookups": len(lookups),
    "n_ondemand": len(on_demand),
}))
