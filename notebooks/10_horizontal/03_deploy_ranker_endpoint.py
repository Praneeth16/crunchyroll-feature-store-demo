# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Deploy the ranker to Model Serving
# MAGIC
# MAGIC One endpoint. The registered model carries its feature spec, so the
# MAGIC endpoint retrieves governed features from the Lakebase online store at
# MAGIC request time — **automatic feature lookup**.
# MAGIC
# MAGIC Inference tables are enabled so every request and response is captured
# MAGIC for the learning loop.
# COMMAND ----------
# MAGIC %pip install databricks-sdk mlflow --quiet
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

cfg = Config.from_widgets(dbutils, extra_widgets={"model_version": ""})
CATALOG, SCHEMA = cfg.catalog, cfg.schema
MODEL = cfg.t("crunchyroll_ranker")
ENDPOINT = cfg.ranker_endpoint

import mlflow
mlflow.set_registry_uri("databricks-uc")
from mlflow.tracking import MlflowClient
mc = MlflowClient()
versions = mc.search_model_versions(f"name='{MODEL}'")
# model_version pins a specific version; empty means "whatever training just
# produced". The spine runs this notebook twice -- once for v1, once after
# request-time features produce v2 -- so it must not assume either.
pinned = cfg.extras.get("model_version", "").strip()
latest = int(pinned) if pinned else max(int(v.version) for v in versions)
print("serving", MODEL, "version", latest, "(pinned)" if pinned else "(latest)")

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput, ServedEntityInput,
)
w = WorkspaceClient()

ep_cfg = EndpointCoreConfigInput(
    name=ENDPOINT,
    served_entities=[ServedEntityInput(
        entity_name=MODEL,
        entity_version=latest,
        workload_size="Small",
        # Off: the homepage service calls this in the request path, and a scaled-to-zero
        # endpoint answers its first request in tens of seconds (the retriever measured
        # 42 s on 2026-09-23). The app's timeout budget turns that into a fallback, so
        # scale-to-zero here means the model never serves the first visitor.
        scale_to_zero_enabled=False,
    )],
)

import time, json

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

existing = [e for e in w.serving_endpoints.list() if e.name == ENDPOINT]
if not existing:
    print("creating endpoint", ENDPOINT)
    with_conflict_retry(lambda: w.serving_endpoints.create(name=ENDPOINT, config=ep_cfg), "create")
else:
    print("updating endpoint", ENDPOINT)
    with_conflict_retry(lambda: w.serving_endpoints.update_config(
        name=ENDPOINT, served_entities=ep_cfg.served_entities), "update")

# COMMAND ----------
import time, json
for i in range(90):
    ep = w.serving_endpoints.get(ENDPOINT)
    state = ep.state.ready.value if ep.state and ep.state.ready else "UNKNOWN"
    cfg_state = ep.state.config_update.value if ep.state and ep.state.config_update else ""
    print(f"[{i}] ready={state} config_update={cfg_state}")
    if state == "READY" and str(cfg_state) in ("NOT_UPDATING", ""):
        break
    time.sleep(20)

# AI Gateway inference tables (legacy auto_capture_config is deprecated).
# Must run once served-entity updates settle, else ResourceConflict.
print("enabling AI Gateway inference table")
for attempt in range(10):
    try:
        resp = w.api_client.do(
            "PUT", f"/api/2.0/serving-endpoints/{ENDPOINT}/ai-gateway",
            body={"inference_table_config": {
                "catalog_name": CATALOG,
                "schema_name": SCHEMA,
                "table_name_prefix": "cr_ranker_inference",
                "enabled": True,
            }},
        )
        print("ai-gateway config applied:", resp)
        break
    except Exception as e:
        if "ResourceConflict" in type(e).__name__ and attempt < 9:
            print(f"[{attempt}] entities still updating, retry in 30s")
            time.sleep(30)
        else:
            raise

ep = w.serving_endpoints.get(ENDPOINT)
print("endpoint url:", ep.url if hasattr(ep, "url") else f"{w.config.host}/serving-endpoints/{ENDPOINT}/invocations")
dbutils.notebook.exit(json.dumps({"endpoint": ENDPOINT, "model": MODEL, "version": latest,
                                  "state": ep.state.ready.value}))
