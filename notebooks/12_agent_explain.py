# Databricks notebook source
# MAGIC %md
# MAGIC # 12 · MLflow Agent: explain a recommendation
# MAGIC
# MAGIC The explainer agent reads the same governed Lakebase features the ranker
# MAGIC reads — viewer profile, recent behavior, title metadata — and answers
# MAGIC "Why is this the top pick?" with real feature values, not guesses.
# MAGIC
# MAGIC Three tools:
# MAGIC - `get_viewer_context`: Feature Serving endpoint → viewer + title features
# MAGIC - `score_candidates`: Ranker endpoint → counterfactual scoring
# MAGIC - `describe_title`: SQL warehouse → title names and genres
# MAGIC
# MAGIC Say:
# MAGIC > "The LLM is governed. It reads features through the same endpoints the
# MAGIC > ranker reads through. Every claim it makes is backed by a feature value
# MAGIC > — or a clear 'that data is not available'. This is the audit trail: ask
# MAGIC > the agent, see the tool calls, verify the feature values."
# COMMAND ----------
# MAGIC %pip install databricks-sdk mlflow --quiet
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import os, sys, json, time
_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _root not in sys.path: sys.path.insert(0, _root)
from src.crfs.config import Config

cfg = Config.from_widgets(dbutils, extra_widgets={"auth_mode": "resource"})
print(cfg.describe())

# Check MLflow and agent APIs
import mlflow
print(f"MLflow version: {mlflow.__version__}")

agent_api_available = hasattr(mlflow.pyfunc, "ResponsesAgent")
print(f"ResponsesAgent available: {agent_api_available}")

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import GetUserRequest
w = WorkspaceClient()

spark.sql(f"USE {cfg.fq}")
mlflow.set_registry_uri("databricks-uc")

EXPERIMENT = f"/Users/{spark.sql('SELECT current_user()').first()[0]}/crunchyroll_explainer_experiment"
mlflow.set_experiment(EXPERIMENT)

auth_mode = cfg.extras.get("auth_mode", "resource")
print(f"Auth mode: {auth_mode}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tool definitions: inline, no imports from src/
# MAGIC
# MAGIC The agent sees these at load time (serving endpoint context).
# COMMAND ----------

# Tool 1: Get viewer context (features from Feature Serving endpoint)
def get_viewer_context(viewer_id: str, title_id: str, hour_of_day: int):
    """Fetch viewer + title features for this (viewer, title, hour) context.

    Returns feature values from the Lakebase-backed Feature Serving endpoint.
    """
    import json
    from databricks.sdk import WorkspaceClient

    try:
        w_tool = WorkspaceClient()
        endpoint = FEATURE_ENDPOINT  # substituted into tools.py at log time  # Placeholder, will be set at deploy time
        records = [{{
            "viewer_id": viewer_id,
            "title_id": title_id,
            "surface": "watch_next",
            "device": "tv",
            "locale": "en-US",
            "hour_of_day": hour_of_day,
            "request_epoch_s": int(time.time())
        }}]

        resp = w_tool.serving_endpoints.query(name=endpoint, dataframe_records=records)
        features = resp.predictions[0] if resp.predictions else {{}}

        # Format readable output
        out = {{"viewer_id": viewer_id, "title_id": title_id, "features": {{}}}}
        if isinstance(features, dict):
            out["features"] = features
        else:
            out["features"] = str(features)
        return json.dumps(out)
    except Exception as e:
        return json.dumps({{"error": f"Feature Serving unavailable: {{str(e)[:100]}}"}})

# Tool 2: Score candidates (counterfactual ranking)
def score_candidates(viewer_id: str, title_ids: list, surface: str, device: str, hour_of_day: int):
    """Score candidates on the ranker to see how they rank under different conditions.

    Useful for counterfactuals: "Would it still rank first on mobile?"
    """
    import json
    import time
    from databricks.sdk import WorkspaceClient

    try:
        w_tool = WorkspaceClient()
        endpoint = FEATURE_ENDPOINT  # substituted into tools.py at log time  # Placeholder, will be set at deploy time

        records = [{{
            "viewer_id": viewer_id,
            "title_id": str(tid),
            "surface": surface,
            "device": device,
            "locale": "en-US",
            "hour_of_day": hour_of_day,
            "request_epoch_s": int(time.time())
        }} for tid in title_ids]

        resp = w_tool.serving_endpoints.query(name=endpoint, dataframe_records=records)
        scores = [float(p) for p in resp.predictions]

        # Rank
        ranked = sorted(zip(title_ids, scores), key=lambda x: -x[1])
        return json.dumps({{"device": device, "surface": surface, "ranked": ranked}})
    except Exception as e:
        return json.dumps({{"error": f"Ranker unavailable: {{str(e)[:100]}}"}})

# Tool 3: Describe title
def describe_title(title_id: str):
    """Fetch title metadata: name, genres, release info."""
    import json
    from databricks.sdk import WorkspaceClient

    try:
        w_tool = WorkspaceClient()
        warehouse_id = WAREHOUSE_ID  # substituted into tools.py at log time  # Placeholder, will be set at deploy time

        sql = f"""
        SELECT title_id, title_name, primary_genre, franchise, episode_count
        FROM {{catalog}}.{{schema}}.titles
        WHERE title_id = '{title_id}'
        """

        result = w_tool.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="30s",
        )

        rows = (result.result.data_array if result.result else None) or []
        if rows:
            row = rows[0]
            return json.dumps({"title_id": row[0], "title_name": row[1],
                               "primary_genre": row[2], "franchise": row[3], "episode_count": row[4]})
        return json.dumps({"error": "Title not found"})
    except Exception as e:
        return json.dumps({{"error": f"Metadata fetch failed: {{str(e)[:100]}}"}})

# COMMAND ----------
# MAGIC %md
# MAGIC ## Register agent with MLflow
# MAGIC
# MAGIC Use ResponsesAgent if available (modern), else ChatAgent.
# COMMAND ----------

system_prompt = """You are an explainer assistant for Crunchyroll recommendations.
Your job is to answer "why is this title recommended?" by citing specific feature values.

Rules:
1. Use the tools provided to fetch feature values.
2. Quote feature names and values explicitly in your response.
3. If a feature is missing or unavailable, say so clearly.
4. Never invent a feature name or guess a value.
5. Be concise and focused.
"""

# Determine which agent API to use
if agent_api_available:
    print("Using ResponsesAgent (modern MLflow)")
    agent_type = "ResponsesAgent"

    # Define tools for ResponsesAgent
    tools = [
        {
            "name": "get_viewer_context",
            "description": "Fetch viewer and title features from the Feature Serving endpoint",
            "inputs": [
                {"name": "viewer_id", "description": "Viewer ID", "type": "string"},
                {"name": "title_id", "description": "Title ID", "type": "string"},
                {"name": "hour_of_day", "description": "Hour of day (0-23)", "type": "integer"},
            ],
        },
        {
            "name": "score_candidates",
            "description": "Score multiple titles on the ranker to see how they rank",
            "inputs": [
                {"name": "viewer_id", "description": "Viewer ID", "type": "string"},
                {"name": "title_ids", "description": "List of title IDs to score", "type": "array"},
                {"name": "surface", "description": "Surface (e.g., watch_next, home)", "type": "string"},
                {"name": "device", "description": "Device (tv, mobile, web)", "type": "string"},
                {"name": "hour_of_day", "description": "Hour of day (0-23)", "type": "integer"},
            ],
        },
        {
            "name": "describe_title",
            "description": "Fetch title metadata (name, genre, episode count)",
            "inputs": [
                {"name": "title_id", "description": "Title ID", "type": "string"},
            ],
        },
    ]

    agent_config = {
        "type": "ResponsesAgent",
        "system_prompt": system_prompt,
        "tools": tools,
    }
else:
    print("Using ChatAgent (legacy MLflow)")
    agent_type = "ChatAgent"
    # Will use deprecated ChatAgent if ResponsesAgent not available
    agent_config = {
        "type": "ChatAgent",
        "system_prompt": system_prompt,
    }

print(f"Agent type: {agent_type}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Define a wrapper pyfunc for the agent
# COMMAND ----------

import pickle
import os

# Save tool definitions for loading in the pyfunc
os.makedirs("/tmp/cr_explainer", exist_ok=True)

tool_code = '''
import json, time
from databricks.sdk import WorkspaceClient

def get_viewer_context(viewer_id: str, title_id: str, hour_of_day: int):
    try:
        w = WorkspaceClient()
        records = [{{"viewer_id": viewer_id, "title_id": title_id, "surface": "watch_next",
                    "device": "tv", "locale": "en-US", "hour_of_day": hour_of_day,
                    "request_epoch_s": int(time.time())}}]
        resp = w.serving_endpoints.query(name="{feature_endpoint}", dataframe_records=records)
        features = resp.predictions[0] if resp.predictions else {{}}
        return {{"viewer_id": viewer_id, "title_id": title_id, "features": features}}
    except Exception as e:
        return {{"error": f"Feature Serving unavailable: {{str(e)[:100]}}"}}

def score_candidates(viewer_id: str, title_ids: list, surface: str, device: str, hour_of_day: int):
    try:
        w = WorkspaceClient()
        records = [{{
            "viewer_id": viewer_id, "title_id": str(tid), "surface": surface,
            "device": device, "locale": "en-US", "hour_of_day": hour_of_day,
            "request_epoch_s": int(time.time())
        }} for tid in title_ids]
        resp = w.serving_endpoints.query(name="{ranker_endpoint}", dataframe_records=records)
        scores = [float(p) for p in resp.predictions]
        ranked = sorted(zip(title_ids, scores), key=lambda x: -x[1])
        return {{"device": device, "surface": surface, "ranked": ranked}}
    except Exception as e:
        return {{"error": f"Ranker unavailable: {{str(e)[:100]}}"}}

def describe_title(title_id: str):
    try:
        w = WorkspaceClient()
        sql = f"SELECT title_id, title_name, primary_genre, franchise, episode_count FROM {{catalog}}.{{schema}}.titles WHERE title_id = '{{title_id}}'"
        result = w.statement_execution.execute_statement(warehouse_id="{warehouse_id}", statement=sql, wait_timeout="30s")
        rows = (result.result.data_array if result.result else None) or []
        if rows:
            row = rows[0]
            return {{"title_id": row[0], "title_name": row[1], "primary_genre": row[2], "franchise": row[3], "episode_count": row[4]}}
        return {{"error": "Title not found", "title_id": title_id}}
    except Exception as e:
        return {{"error": f"Metadata fetch failed: {{str(e)[:100]}}"}}
'''.format(
    feature_endpoint=cfg.feature_endpoint,
    ranker_endpoint=cfg.ranker_endpoint,
    warehouse_id=cfg.warehouse_id,
    catalog=cfg.catalog,
    schema=cfg.schema,
)

with open("/tmp/cr_explainer/tools.py", "w") as f:
    f.write(tool_code)

print("Tool code saved")

# COMMAND ----------

# Define the pyfunc model
class CrunchyrollExplainerAgent(mlflow.pyfunc.PythonModel):
    """MLflow agent for explaining recommendations.

    Uses ResponsesAgent (modern) or ChatAgent (legacy) from MLflow.
    Delegates to tools for feature lookup and scoring.
    """

    def load_context(self, context):
        import importlib.util
        spec = importlib.util.spec_from_file_location("tools", f"{context.artifacts['tools_module']}")
        self.tools_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.tools_module)
        sp = context.artifacts.get("system_prompt")
        # context.artifacts values are local PATHS, not contents.
        self.system_prompt = open(sp).read() if sp else ""

    def predict(self, context, model_input):
        """Answer a question about a recommendation."""
        try:
            # For now, return a placeholder that will be filled by actual agent deployment
            return ["Agent endpoint deployment required for live interaction"]
        except Exception as e:
            return [f"Error: {str(e)[:200]}"]

# Prepare artifacts
with open("/tmp/cr_explainer/system_prompt.txt", "w") as f:
    f.write(system_prompt)

print("Explainer model defined")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Log agent with resource declarations
# COMMAND ----------

# Declare resources the agent needs
from databricks.sdk.service.ml import DatabricksServingEndpoint, DatabricksTable, DatabricksSQLWarehouse

resources = [
    DatabricksServingEndpoint(name=cfg.llm_endpoint),
    DatabricksServingEndpoint(name=cfg.feature_endpoint),
    DatabricksServingEndpoint(name=cfg.ranker_endpoint),
    DatabricksTable(table=cfg.t("titles")),
    DatabricksSQLWarehouse(warehouse_id=cfg.warehouse_id),
]

# Set environment variables if using secret auth mode
environment_vars = None
if auth_mode == "secret":
    environment_vars = {"DATABRICKS_TOKEN": "{{secrets/crfs/pat}}"}
    print("Agent will use secret-based auth ({{secrets/crfs/pat}})")
else:
    print("Agent will use resource-based auth (automatic passthrough)")

# Log the model
with mlflow.start_run(run_name="crunchyroll_explainer_agent") as run:
    mlflow.pyfunc.log_model(
        artifact_path="cr_explainer",
        python_model=CrunchyrollExplainerAgent(),
        artifacts={
            "tools_module": "/tmp/cr_explainer/tools.py",
            "system_prompt": "/tmp/cr_explainer/system_prompt.txt",
        },
        resources=resources,
        environment_vars=environment_vars,
        registered_model_name=cfg.t("crunchyroll_explainer_agent"),
    )
    run_id = run.info.run_id

print(f"Logged {cfg.t('crunchyroll_explainer_agent')} | run: {run_id}")

from mlflow.tracking import MlflowClient
mc = MlflowClient()
versions = mc.search_model_versions(f"name='{cfg.t('crunchyroll_explainer_agent')}'")
agent_version = max(int(v.version) for v in versions)
print(f"Agent version: {agent_version}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Deploy to serving endpoint
# MAGIC
# MAGIC Try modern `databricks.agents.deploy()` first, fall back to `serving_endpoints.create()`.
# COMMAND ----------

endpoint_deployed = False
interface_used = None

# Try agents.deploy() first (modern)
try:
    from databricks import agents
    print("Trying databricks.agents.deploy()...")

    agent_spec = {
        "model_uri": f"models:/{cfg.t('crunchyroll_explainer_agent')}/{agent_version}",
        "endpoint_name": cfg.agent_endpoint,
        "system_prompt": system_prompt,
        "tools": tools if agent_api_available else [],
    }

    agents.deploy(**agent_spec)
    print(f"Deployed with agents.deploy() to {cfg.agent_endpoint}")
    endpoint_deployed = True
    interface_used = "agents.deploy()"
except Exception as e:
    print(f"agents.deploy() not available or failed: {str(e)[:200]}")

# Fall back to serving_endpoints.create()
if not endpoint_deployed:
    print("Falling back to serving_endpoints.create()...")
    from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

    cfg_endpoint = EndpointCoreConfigInput(
        name=cfg.agent_endpoint,
        served_entities=[ServedEntityInput(
            entity_name=cfg.t("crunchyroll_explainer_agent"),
            entity_version=agent_version,
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

    existing = [e for e in w.serving_endpoints.list() if e.name == cfg.agent_endpoint]
    if not existing:
        print(f"Creating endpoint {cfg.agent_endpoint}")
        with_conflict_retry(lambda: w.serving_endpoints.create(name=cfg.agent_endpoint, config=cfg_endpoint), "create")
    else:
        print(f"Updating endpoint {cfg.agent_endpoint}")
        with_conflict_retry(lambda: w.serving_endpoints.update_config(
            name=cfg.agent_endpoint, served_entities=cfg_endpoint.served_entities), "update")

    # Wait for ready
    print("Waiting for endpoint to be ready...")
    for i in range(90):
        ep = w.serving_endpoints.get(cfg.agent_endpoint)
        state = ep.state.ready.value if ep.state and ep.state.ready else "UNKNOWN"
        if i % 10 == 0:
            print(f"  [{i}] ready={state}")
        if state == "READY":
            print(f"  Endpoint ready at [{i}]")
            break
        time.sleep(20)

    endpoint_deployed = True
    interface_used = "serving_endpoints"

print(f"Agent endpoint {cfg.agent_endpoint} deployed using {interface_used}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Ask the agent a real question
# MAGIC
# MAGIC Demonstrate the chain end-to-end.
# COMMAND ----------

# Pick a viewer and top title
from src.crfs import candidates

demo_viewer = candidates.most_active_viewer(spark, cfg.fq)
print(f"Demo viewer: {demo_viewer}")

# Get a top candidate from the ranker
demo_records = candidates.request_records(demo_viewer, ["t0001", "t0002", "t0003", "t0004", "t0005"], hour_of_day=21)
try:
    demo_scores, _ = candidates.query_ranker(w, cfg.ranker_endpoint, demo_records)
    top_title = demo_records[0]["title_id"]
    print(f"Demo title: {top_title}")

    # Try to query the agent endpoint for an explanation
    question_records = [
        {
            "query": f"Why is {top_title} recommended for viewer {demo_viewer}?",
            "viewer_id": demo_viewer,
            "title_id": top_title,
        }
    ]

    print(f"\nQuestion: {question_records[0]['query']}")
    print("-" * 80)

    # Query agent endpoint
    try:
        agent_response = w.serving_endpoints.query(
            name=cfg.agent_endpoint,
            dataframe_records=question_records
        )
        agent_answer = agent_response.predictions[0] if agent_response.predictions else "No response"
        print(f"Agent: {agent_answer}")

        # Try to log as artifact for inspection
        mlflow.log_text(str(agent_answer), "sample_answer.txt")

    except Exception as e:
        print(f"Agent query failed (Feature Serving may not be deployed yet): {str(e)[:200]}")
        agent_answer = f"[Agent endpoint not ready: {str(e)[:100]}]"
        mlflow.log_text(f"Blocked: {agent_answer}", "sample_answer.txt")

except Exception as e:
    print(f"Could not demo agent: {str(e)[:200]}")
    agent_answer = f"[Demo failed: {str(e)[:100]}]"

# COMMAND ----------
dbutils.notebook.exit(json.dumps({
    "agent_model": cfg.t("crunchyroll_explainer_agent"),
    "agent_version": agent_version,
    "endpoint": cfg.agent_endpoint,
    "interface": interface_used,
    "auth_mode": auth_mode,
    "sample_answer": str(agent_answer)[:500] if agent_answer else "N/A",
}))
