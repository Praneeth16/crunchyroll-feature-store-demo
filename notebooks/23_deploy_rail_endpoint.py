# Databricks notebook source
# MAGIC %md
# MAGIC # 23 · The rail ranker as a request-path endpoint
# MAGIC
# MAGIC The watch-next ranker in notebook 03 is configured like a demo: `Small`, scale
# MAGIC to zero on. That is the right choice for something queried a few times during a
# MAGIC presentation and the wrong choice for something in front of the homepage.
# MAGIC
# MAGIC This endpoint is configured for a request path, and every choice is a lever
# MAGIC Crunchyroll will have to set for themselves:
# MAGIC
# MAGIC | Setting | Value here | Why |
# MAGIC |---|---|---|
# MAGIC | `scale_to_zero_enabled` | **false** | Scale-from-zero has no latency SLA and is measured in seconds. A homepage cannot absorb that on the first request after a quiet minute. This is the single most important setting on this page. |
# MAGIC | `min_provisioned_concurrency` / `max_provisioned_concurrency` | **4 / 32** | Explicit floor and ceiling instead of a t-shirt size. The floor is capacity that is always warm; the ceiling is what a spike is allowed to grow into. Sizing rule from the docs: `provisioned_concurrency ≈ QPS × model_execution_seconds`. |
# MAGIC | `route_optimized` | requested **true**, accepted only if the workspace allows it | A lower-overhead network path, for higher QPS and more stable latency. **Create-time only.** Requested on every create and dropped by the fallback chain if rejected — it was rejected on the workspace this was built against. An *existing* endpoint without it is updated in place rather than rebuilt, because rebuilding on every run costs a container build and discards the inference table history. |
# MAGIC | AI Gateway inference table | **enabled** | Captures `execution_time_ms` per request, which is how notebook 25 separates model time from network time instead of arguing about it. |
# MAGIC | AI Gateway usage tracking | **enabled** | Per-endpoint request and cost attribution in system tables. |
# MAGIC
# MAGIC `workload_size` and the provisioned-concurrency pair are **mutually exclusive**
# MAGIC in the API. This notebook asks for the explicit pair and falls back to
# MAGIC `workload_size` if the workspace rejects it, then records which one it actually
# MAGIC got — because the benchmark numbers are meaningless without knowing.
# COMMAND ----------
import os, sys
_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _root not in sys.path:
    sys.path.insert(0, _root)

from src.crfs.config import Config
from src.crfs import loadtest as LT

cfg = Config.from_widgets(dbutils, extra_widgets={
    "model_version": "",              # blank -> the @champion alias
    "min_concurrency": "4",
    "max_concurrency": "32",
    "workload_size_fallback": "Small",
    "route_optimized": "true",
    # Default FALSE on purpose. Route optimization is create-time only, so honouring a
    # mismatch means deleting a live endpoint -- and on a workspace where route
    # optimization is rejected (as it is here) that turns into a delete-and-rebuild on
    # EVERY run: minutes of container build, and the inference table's history is
    # discarded each time. An existing endpoint is now updated in place and the mismatch
    # is reported. Set this true deliberately, once, if you want the rebuild.
    "recreate_for_route_optimization": "false",
    "inference_table_prefix": "cr_rail_inference",
})
spark.sql(f"USE {cfg.fq}")
MODEL = cfg.t("crunchyroll_rail_ranker")
ENDPOINT = cfg.rail_ranker_endpoint
MIN_C = int(cfg.extras["min_concurrency"])
MAX_C = int(cfg.extras["max_concurrency"])
WANT_ROUTE = cfg.extras["route_optimized"].lower() == "true"
MAY_RECREATE = cfg.extras["recreate_for_route_optimization"].lower() == "true"
print(cfg.describe())
print(f"\nmodel {MODEL} -> endpoint {ENDPOINT}")
# COMMAND ----------
import json, time
from databricks.sdk import WorkspaceClient
from mlflow.tracking import MlflowClient
import mlflow

mlflow.set_registry_uri("databricks-uc")
w = WorkspaceClient()
mc = MlflowClient()

VERSION = cfg.extras.get("model_version") or ""
if not VERSION:
    try:
        VERSION = str(mc.get_model_version_by_alias(MODEL, "champion").version)
        print(f"resolved @champion -> version {VERSION}")
    except Exception:
        VERSION = str(max(int(v.version) for v in mc.search_model_versions(f"name='{MODEL}'")))
        print(f"no @champion alias; using latest version {VERSION}")
mv = mc.get_model_version(MODEL, VERSION)
print("status:", mv.status, "| tags:", dict(mv.tags or {}))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Build the served-entity config
# MAGIC
# MAGIC Two shapes are prepared: the explicit provisioned-concurrency one this
# MAGIC architecture wants, and a `workload_size` fallback for a workspace that has not
# MAGIC enabled the former. The endpoint is created with the first that is accepted.
# COMMAND ----------
ENTITY_NAME = f"rail_ranker-{VERSION}"

served_explicit = {
    "name": ENTITY_NAME,
    "entity_name": MODEL,
    "entity_version": VERSION,
    # The reason this endpoint exists in a separate notebook from notebook 03.
    "scale_to_zero_enabled": False,
    "min_provisioned_concurrency": MIN_C,
    "max_provisioned_concurrency": MAX_C,
    "workload_type": "CPU",
}
served_fallback = {
    "name": ENTITY_NAME,
    "entity_name": MODEL,
    "entity_version": VERSION,
    "scale_to_zero_enabled": False,
    "workload_size": cfg.extras["workload_size_fallback"],
    "workload_type": "CPU",
}

AI_GATEWAY = {
    "inference_table_config": {
        "enabled": True,
        "catalog_name": cfg.catalog,
        "schema_name": cfg.schema,
        "table_name_prefix": cfg.extras["inference_table_prefix"],
    },
    "usage_tracking_config": {"enabled": True},
}


def endpoint_body(served):
    return {
        "name": ENDPOINT,
        "config": {
            "served_entities": [served],
            "traffic_config": {"routes": [{"served_entity_name": ENTITY_NAME,
                                           "traffic_percentage": 100}]},
        },
        "ai_gateway": AI_GATEWAY,
    }


print(json.dumps(endpoint_body(served_explicit), indent=2))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Create, update, or recreate
# MAGIC
# MAGIC Route optimization is a create-time property. An existing endpoint without it
# MAGIC cannot be upgraded in place, so when it is wanted and missing the endpoint is
# MAGIC deleted and rebuilt — stated out loud rather than silently serving a
# MAGIC differently-configured endpoint than the one the benchmark claims to measure.
# COMMAND ----------
def get_endpoint():
    """The endpoint, or None if it genuinely does not exist.

    A bare `except: return None` here is what turned one display bug into a broken
    deployment: attempt 0 created the endpoint and then failed on a print, the automatic
    retry could not read it back, concluded it was absent, and tried to CREATE it again
    -- which was rejected because attempt 0 had already created the inference table.
    Only "does not exist" means absent; anything else is re-raised.
    """
    try:
        return w.serving_endpoints.get(ENDPOINT)
    except Exception as e:
        msg = str(e).lower()
        if ("does not exist" in msg or "resource_does_not_exist" in msg
                or "not found" in msg or " 404" in msg):
            return None
        raise


existing = get_endpoint()
action = None

if existing is not None:
    has_route = bool(getattr(existing, "route_optimized", False))
    if WANT_ROUTE and not has_route:
        if MAY_RECREATE:
            print(f"{ENDPOINT} exists WITHOUT route optimization, which cannot be "
                  f"enabled in place. Deleting and recreating.")
            w.serving_endpoints.delete(ENDPOINT)
            for _ in range(60):
                if get_endpoint() is None:
                    break
                time.sleep(5)
            existing = None
        else:
            print(f"NOTE: {ENDPOINT} exists without route optimization, and it cannot be "
                  f"enabled in place. Updating the existing endpoint instead of "
                  f"rebuilding it -- this keeps the inference table's history and saves "
                  f"a container build. The benchmark will report route_optimized=false, "
                  f"which is accurate. To rebuild with route optimization, rerun with "
                  f"recreate_for_route_optimization=true.")
    elif has_route and not WANT_ROUTE:
        print(f"note: {ENDPOINT} is route-optimized but route_optimized=false was "
              f"requested. Leaving it -- turning it off also requires a recreate.")

if existing is None:
    # Ordered most-to-least desirable. Each fallback gives up exactly one thing, and
    # whatever succeeds is recorded in `action` so the benchmark reports the config it
    # actually measured rather than the one that was asked for.
    #
    # Route optimization is last to be given up but it IS given up: a working
    # request-path endpoint without it is worth more than no endpoint, and losing it
    # only costs network overhead, whereas losing scale_to_zero=false would cost
    # correctness for this use case. scale_to_zero is therefore false in every attempt.
    candidates = []
    for served, size_label in ((served_explicit, "provisioned concurrency"),
                               (served_fallback, "workload_size")):
        if WANT_ROUTE:
            candidates.append((served, size_label, True))
    for served, size_label in ((served_explicit, "provisioned concurrency"),
                               (served_fallback, "workload_size")):
        candidates.append((served, size_label, False))

    def clear_orphaned_inference_table():
        """Creating an endpoint whose AI Gateway inference-table prefix already exists is
        rejected outright:

          BadRequest: Table in Unity Catalog ..._payload already exists.
                      Please specify a different table prefix.

        That happens whenever a previous create half-succeeded. Dropping the table is
        only safe if no endpoint owns it, so that is checked first and the drop is
        announced -- silently deleting captured inference logs would be worse than
        failing.
        """
        prefix = cfg.extras["inference_table_prefix"]
        owners = []
        for e in w.serving_endpoints.list():
            gw = getattr(e, "ai_gateway", None)
            inf = getattr(gw, "inference_table_config", None) if gw else None
            if inf and getattr(inf, "table_name_prefix", None) == prefix and e.name != ENDPOINT:
                owners.append(e.name)
        if owners:
            print(f"  inference prefix {prefix} is owned by {owners}; not touching it")
            return False
        dropped = []
        for suffix in ("payload", "payload_ai_gateway"):
            tbl = cfg.t(f"{prefix}_{suffix}")
            if spark.catalog.tableExists(tbl):
                spark.sql(f"DROP TABLE IF EXISTS {tbl}")
                dropped.append(tbl)
        if dropped:
            print(f"  dropped orphaned inference table(s) no endpoint owned: {dropped}")
        return bool(dropped)

    last_error = None
    cleared_orphan = False
    for served, size_label, want_route in candidates:
        body = endpoint_body(served)
        if want_route:
            body["route_optimized"] = True
        label = f"{size_label}{' + route optimization' if want_route else ', no route optimization'}"
        try:
            print(f"creating {ENDPOINT} with {label} ...")
            w.api_client.do("POST", "/api/2.0/serving-endpoints", body=body)
            action = f"created ({label})"
            break
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            print(f"  rejected: {last_error[:400]}")
            if "already exists" in last_error and "table prefix" in last_error \
                    and not cleared_orphan:
                cleared_orphan = True
                if clear_orphaned_inference_table():
                    try:
                        print(f"  retrying {label} after clearing the orphan ...")
                        w.api_client.do("POST", "/api/2.0/serving-endpoints", body=body)
                        action = f"created ({label}, after clearing an orphaned inference table)"
                        break
                    except Exception as e2:
                        last_error = f"{type(e2).__name__}: {e2}"
                        print(f"  still rejected: {last_error[:400]}")
    if action is None:
        raise RuntimeError(
            f"every endpoint configuration was rejected. Last error: {last_error}")
else:
    # Config update: route optimization is untouched, everything else is applied.
    try:
        w.api_client.do("PUT", f"/api/2.0/serving-endpoints/{ENDPOINT}/config",
                        body={"served_entities": [served_explicit],
                              "traffic_config": {"routes": [
                                  {"served_entity_name": ENTITY_NAME,
                                   "traffic_percentage": 100}]}})
        action = "updated (provisioned concurrency)"
    except Exception as e:
        print(f"  provisioned concurrency rejected on update: {str(e)[:300]}")
        w.api_client.do("PUT", f"/api/2.0/serving-endpoints/{ENDPOINT}/config",
                        body={"served_entities": [served_fallback],
                              "traffic_config": {"routes": [
                                  {"served_entity_name": ENTITY_NAME,
                                   "traffic_percentage": 100}]}})
        action = "updated (workload_size)"
    try:
        w.api_client.do("PUT", f"/api/2.0/serving-endpoints/{ENDPOINT}/ai-gateway",
                        body=AI_GATEWAY)
        print("ai gateway config applied")
    except Exception as e:
        print(f"ai gateway update failed: {str(e)[:300]}")

print("action:", action)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Wait for READY
# MAGIC
# MAGIC A model-serving deployment builds a container image, so first deployment of a
# MAGIC version is minutes, not seconds. Polling on the real state beats sleeping and
# MAGIC hoping.
# COMMAND ----------
deadline = time.time() + 45 * 60
last = None
became_ready = False


def _state_value(v):
    """The enum's value, not its repr.

    `"READY" in str(EndpointStateReady.NOT_READY)` is **True** -- a substring test
    treats NOT_READY as ready and breaks the poll early, then the smoke test below
    queries an endpoint that is still building. Compare exact values instead.
    """
    return str(getattr(v, "value", v) or "")


while time.time() < deadline:
    ep = get_endpoint()
    if ep is None:
        time.sleep(10)
        continue
    ready = _state_value(getattr(ep.state, "ready", ""))
    upd = _state_value(getattr(ep.state, "config_update", ""))
    entity_states = [f"{e.name}:{_state_value(getattr(e.state, 'deployment', ''))}"
                     for e in ((ep.config.served_entities if ep.config else None) or [])]
    cur = f"{ready} / {upd} / {entity_states}"
    if cur != last:
        print(f"  {time.strftime('%H:%M:%S')}  {cur}")
        last = cur
    if ready == "READY" and upd == "NOT_UPDATING":
        became_ready = True
        break
    if "FAILED" in upd:
        raise RuntimeError(f"endpoint config update FAILED: {cur}")
    time.sleep(15)
ep = get_endpoint()
print("\nfinal state:", getattr(ep.state, "ready", None), getattr(ep.state, "config_update", None))
if not became_ready:
    # Falling out of the loop silently meant a 45-minute stall surfaced further down as
    # a confusing query error against a half-built endpoint. Say what happened, here.
    raise TimeoutError(
        f"endpoint {ENDPOINT} did not reach READY / NOT_UPDATING within 45 minutes; "
        f"last observed state: {last}. Nothing downstream of this can be trusted, so "
        f"this fails rather than proceeding to the smoke test.")
# COMMAND ----------
# MAGIC %md
# MAGIC ## What we actually got
# MAGIC
# MAGIC Requested config and realised config are not always the same thing. This is
# MAGIC the record the benchmark quotes.
# COMMAND ----------
ep_cfg = LT.endpoint_config_summary(w, ENDPOINT)
# default=str as a second line of defence: endpoint_config_summary now returns only
# primitives, but this print already cost one deployment and is not worth a third.
print(json.dumps(ep_cfg, indent=2, default=str))

realised = (ep_cfg.get("served_entities") or [{}])[0]
warnings = []
if realised.get("scale_to_zero") is not False:
    warnings.append("scale_to_zero is NOT disabled -- cold starts will show up in "
                    "the latency tail and this endpoint is not request-path ready")
if WANT_ROUTE and not ep_cfg.get("route_optimized"):
    warnings.append("route optimization is OFF; latency will carry the standard "
                    "workspace request path overhead")
if realised.get("min_provisioned_concurrency") is None and realised.get("workload_size"):
    warnings.append(f"running on workload_size={realised['workload_size']} rather than "
                    f"explicit provisioned concurrency; the concurrency floor is "
                    f"whatever that size implies")
if not (ep_cfg.get("inference_table") or {}).get("enabled"):
    warnings.append("no inference table -- server-side execution_time_ms will not be "
                    "available and latency cannot be split into model vs network")
for warn in warnings:
    print("\nWARNING:", warn)
if not warnings:
    print("\nrealised configuration matches the request-path intent")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Smoke test: one homepage request
# MAGIC
# MAGIC Eligible rails for a real viewer, one call, ranked rails back. If the endpoint
# MAGIC can do its own feature lookups this returns in one round trip with nothing but
# MAGIC keys and context in the payload.
# COMMAND ----------
from src.crfs import rails as R

viewer = spark.sql(f"""
    SELECT viewer_id FROM {cfg.t('viewer_rail_features_ts')}
    GROUP BY viewer_id ORDER BY SUM(vr_impressions_30d) DESC LIMIT 1
""").first()["viewer_id"]
eligible = R.eligible_rails(spark, cfg.fq, viewer)
print(f"viewer {viewer} is eligible for {len(eligible)} rails")

records = R.rail_request_records(viewer, eligible["rail_id"].tolist(),
                                 device="tv", locale="en-US", hour_of_day=21)
print("\nrequest payload (one row per candidate rail, keys and context only):")
print(json.dumps(records[:2], indent=2))

t0 = time.perf_counter()
resp = w.serving_endpoints.query(name=ENDPOINT, dataframe_records=records)
elapsed_ms = (time.perf_counter() - t0) * 1000.0
preds = list(resp.predictions or [])
print(f"\n{len(preds)} rails scored in {elapsed_ms:.0f} ms (in-region, single request)")
# COMMAND ----------
import pandas as pd

ranked = pd.DataFrame(preds)
if "rail_rank" in ranked.columns:
    ranked = ranked.sort_values("rail_rank")
ranked = ranked.merge(eligible[["rail_id", "rail_name", "rail_type", "editorial_rank"]],
                      on="rail_id", how="left")
# Dense-rank the incumbent within the eligible set before differencing: rail_rank is
# 1..N over eligible rails while editorial_rank is the catalog-wide 1..16, so a raw
# subtraction credits every rail with a gain for each ineligible rail above it.
ranked["moved"] = (ranked["editorial_rank"].astype(int).rank(method="first").astype(int)
                   - ranked["rail_rank"].astype(int))
print(f"the homepage this viewer would get at 21:00 on a TV:\n")
print(ranked[["rail_rank", "rail_name", "rail_type", "engagement_probability",
              "editorial_rank", "moved"]].to_string(index=False))
print("\n'moved' is positions gained against the incumbent editorial order. "
      "A column of zeros would mean the model is agreeing with the old homepage.")
# COMMAND ----------
dbutils.notebook.exit(json.dumps({
    "endpoint": ENDPOINT,
    "model": MODEL,
    "version": VERSION,
    "action": action,
    "endpoint_config": ep_cfg,
    "warnings": warnings,
    "smoke": {"viewer": viewer, "eligible_rails": int(len(eligible)),
              "single_request_ms": round(elapsed_ms, 1),
              "top_3": ranked.head(3)[["rail_rank", "rail_id", "engagement_probability"]]
                              .to_dict(orient="records")},
}, default=str))
