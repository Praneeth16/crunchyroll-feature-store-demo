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
CATALOG = "serverless_lakebase_praneeth_catalog"
SCHEMA = "crunchyroll_demo"
MODEL = f"{CATALOG}.{SCHEMA}.crunchyroll_ranker"
ENDPOINT = "crunchyroll-watch-next-ranker"

import mlflow
mlflow.set_registry_uri("databricks-uc")
from mlflow.tracking import MlflowClient
mc = MlflowClient()
versions = mc.search_model_versions(f"name='{MODEL}'")
latest = max(int(v.version) for v in versions)
print("serving", MODEL, "version", latest)

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput, ServedEntityInput,
)
w = WorkspaceClient()

cfg = EndpointCoreConfigInput(
    name=ENDPOINT,
    served_entities=[ServedEntityInput(
        entity_name=MODEL,
        entity_version=latest,
        workload_size="Small",
        scale_to_zero_enabled=True,
    )],
)

existing = [e for e in w.serving_endpoints.list() if e.name == ENDPOINT]
if not existing:
    print("creating endpoint", ENDPOINT)
    w.serving_endpoints.create(name=ENDPOINT, config=cfg)
else:
    print("updating endpoint", ENDPOINT)
    w.serving_endpoints.update_config(name=ENDPOINT, served_entities=cfg.served_entities)

# AI Gateway inference tables (legacy auto_capture_config is deprecated)
print("enabling AI Gateway inference table")
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

ep = w.serving_endpoints.get(ENDPOINT)
print("endpoint url:", ep.url if hasattr(ep, "url") else f"{w.config.host}/serving-endpoints/{ENDPOINT}/invocations")
dbutils.notebook.exit(json.dumps({"endpoint": ENDPOINT, "model": MODEL, "version": latest,
                                  "state": ep.state.ready.value}))
