# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Teardown — stop the money
# MAGIC
# MAGIC Money first, data last, and every step tolerates "already gone" so this can
# MAGIC be re-run safely.
# MAGIC
# MAGIC | Widget | Effect |
# MAGIC |---|---|
# MAGIC | `cost_only=true` | Stop after deleting the online store. Endpoints and the store go; every UC table, model and function stays. Use this between rehearsals. |
# MAGIC | `keep_data=true` | Never drop UC tables, models or functions, even in a full teardown. |
# MAGIC
# MAGIC The order matters. Endpoints must go before the online store, or a serving
# MAGIC endpoint is left doing feature lookups against a store that no longer
# MAGIC exists. Synced tables must go before their source tables, or a pipeline is
# MAGIC left pointing at a dropped Delta table.
# MAGIC
# MAGIC `scripts/teardown.sh` does the same thing from a laptop; this notebook exists
# MAGIC so it can be run from the workspace UI with no CLI.
# COMMAND ----------
# MAGIC %pip install databricks-feature-engineering databricks-sdk --quiet
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import os, sys
_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _root not in sys.path:
    sys.path.insert(0, _root)

from src.crfs.config import Config
from src.crfs import ops, udfs

cfg = Config.from_widgets(dbutils, extra_widgets={"cost_only": "true", "keep_data": "true"})
COST_ONLY = cfg.extras.get("cost_only", "true").lower() == "true"
KEEP_DATA = cfg.extras.get("keep_data", "true").lower() == "true"

import json
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
done, skipped, failed = [], [], []


def step(label, fn):
    try:
        result = fn()
        if result is False:
            skipped.append(label)
            print(f"  -  {label}: already gone")
        else:
            done.append(label)
            print(f"  x  {label}")
    except Exception as e:
        msg = str(e)[:160]
        # A missing resource is the expected case on a re-run, not a failure.
        if any(t in msg.upper() for t in ("DOES_NOT_EXIST", "NOT_FOUND", "RESOURCE_DOES_NOT_EXIST", "404")):
            skipped.append(label)
            print(f"  -  {label}: already gone")
        else:
            failed.append((label, msg))
            print(f"  !  {label}: {msg}")


print(f"teardown  cost_only={COST_ONLY}  keep_data={KEEP_DATA}  target={cfg.fq}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 1 · The app
# COMMAND ----------
for app in w.apps.list():
    if app.name and app.name.startswith("crfs-"):
        step(f"app {app.name}", lambda n=app.name: w.apps.delete(name=n))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 2 · Serving endpoints — agent first, it depends on the others
# COMMAND ----------
for ep in [cfg.agent_endpoint, cfg.feature_endpoint, cfg.retriever_endpoint, cfg.ranker_endpoint]:
    step(f"endpoint {ep}", lambda n=ep: w.serving_endpoints.delete(name=n))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 3 · Running streams and their pipelines
# MAGIC
# MAGIC A CONTINUOUS publish holds a streaming pipeline open; cancelling the job run
# MAGIC is not enough on its own.
# COMMAND ----------
for job in w.jobs.list():
    if job.settings and job.settings.name and job.settings.name.startswith("crfs_"):
        for run in w.jobs.list_runs(job_id=job.job_id, active_only=True):
            step(f"cancel run {run.run_id} ({job.settings.name})",
                 lambda r=run.run_id: w.jobs.cancel_run(run_id=r))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 4 · Synced (online) tables
# COMMAND ----------
ONLINE = ["online_viewer_features", "online_title_features", "online_recent_behavior",
          "online_viewer_embedding", "online_session_features"]
for name in ONLINE:
    step(f"synced table {name}", lambda n=name: ops.drop_synced_if_exists(w, cfg.t(n)))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 5 · The online store — this is the always-on bill
# COMMAND ----------
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()
step(f"online store {cfg.online_store}",
     lambda: fe.delete_online_store(name=cfg.online_store))
# COMMAND ----------
if COST_ONLY:
    print("cost_only=true - stopping here. UC tables, models and functions untouched.")
    print("\nConfirm the meter stopped by re-running notebook 13 tomorrow: the")
    print("DATABASE_SERVERLESS line should fall to zero.")
    dbutils.notebook.exit(json.dumps({
        "mode": "cost_only", "deleted": done, "already_gone": skipped, "failed": failed}))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 6 · Feature spec and the request-time UDFs
# COMMAND ----------
step("feature spec", lambda: fe.delete_feature_spec(name=cfg.t("crunchyroll_viewer_feature_spec")))
for stmt in udfs.drop_ddl(cfg.fq):
    step(stmt.split("EXISTS ")[-1], lambda s=stmt: spark.sql(s))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 7 · UC tables and registered models
# COMMAND ----------
if KEEP_DATA:
    print("keep_data=true - leaving UC tables and models in place.")
else:
    TABLES = ["viewer_features_ts", "viewer_features_current", "title_features",
              "recent_behavior_current", "viewer_embedding_current", "session_features_current",
              "engagement_events_stream", "crfs_ops_sync_log",
              "titles", "viewers", "entitlements", "engagement_events"]
    for name in TABLES:
        step(f"table {name}", lambda n=name: spark.sql(f"DROP TABLE IF EXISTS {cfg.t(n)}"))

    for t in spark.sql(f"SHOW TABLES IN {cfg.fq}").collect():
        if t.tableName.startswith(("cr_ranker_inference", "event_log_")):
            step(f"table {t.tableName}",
                 lambda n=t.tableName: spark.sql(f"DROP TABLE IF EXISTS {cfg.t(n)}"))

    for model in ["crunchyroll_ranker", "crunchyroll_retriever", "crunchyroll_explainer_agent"]:
        step(f"model {model}",
             lambda m=model: w.registered_models.delete(full_name=cfg.t(m)))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Result
# MAGIC
# MAGIC `databricks bundle destroy` removes what is left: jobs, the volume, the
# MAGIC dashboard and the app shell. The Lakebase project itself is deleted by step 5;
# MAGIC if it survives, `databricks postgres delete-project` finishes the job.
# COMMAND ----------
print("deleted:", len(done), "| already gone:", len(skipped), "| failed:", len(failed))
for label, msg in failed:
    print("  FAILED", label, "->", msg)

dbutils.notebook.exit(json.dumps({
    "mode": "full" if not COST_ONLY else "cost_only",
    "keep_data": KEEP_DATA,
    "deleted": done, "already_gone": skipped, "failed": failed,
}))
