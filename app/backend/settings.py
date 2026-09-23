"""Environment, with the same names resources/app.yml has always set."""
import os

env = os.environ.get

CATALOG = env("DATABRICKS_CATALOG", "serverless_lakebase_praneeth_catalog")
SCHEMA = env("DATABRICKS_SCHEMA", "crunchyroll_demo")
WAREHOUSE_ID = env("DATABRICKS_WAREHOUSE_ID", "4d39ac2e32b72a3a")
RANKER_ENDPOINT = env("RANKER_ENDPOINT", "crunchyroll-watch-next-ranker")
RAIL_RANKER_ENDPOINT = env("RAIL_RANKER_ENDPOINT", "crunchyroll-rail-ranker")
RETRIEVER_ENDPOINT = env("RETRIEVER_ENDPOINT", "crunchyroll-candidate-retriever")
# The explainer calls a chat model directly, grounded on the rows this service already
# read. notebooks/50_agent/12's pyfunc returns a placeholder string, so routing "Why?"
# through that endpoint would put fabricated-looking text on screen.
LLM_ENDPOINT = env("LLM_ENDPOINT", "databricks-claude-haiku-4-5")
LAKEBASE_PROJECT = env("LAKEBASE_PROJECT", "crunchyroll-online-store")
LAKEBASE_BRANCH = env("LAKEBASE_BRANCH", "production")
LAKEBASE_ENDPOINT = env("LAKEBASE_ENDPOINT", "primary")
ONLINE_STORE = env("ONLINE_STORE", "crunchyroll-online-store")
BURST_JOB_ID = env("BURST_JOB_ID", "")
BURST_WAIT_S = float(env("BURST_WAIT_S", "480"))

# Request-path budgets. The rail ranker's p95 is 67 ms in the benchmark and 98-129 ms as
# the app measures it (docs/homepage_service.md), so 300 ms is 2-3x headroom before the
# homepage stops waiting and renders a fallback.
RAIL_TIMEOUT_MS = int(env("RAIL_TIMEOUT_MS", "300"))
RANKER_TIMEOUT_MS = int(env("RANKER_TIMEOUT_MS", "400"))
RETRIEVER_TIMEOUT_MS = int(env("RETRIEVER_TIMEOUT_MS", "400"))
BREAKER_FAILURES = int(env("BREAKER_FAILURES", "5"))
BREAKER_COOLDOWN_S = float(env("BREAKER_COOLDOWN_S", "10"))
SNAPSHOT_REFRESH_S = float(env("SNAPSHOT_REFRESH_S", "300"))

FQ = f"{CATALOG}.{SCHEMA}"
ENDPOINT_PATH = (f"projects/{LAKEBASE_PROJECT}/branches/{LAKEBASE_BRANCH}"
                 f"/endpoints/{LAKEBASE_ENDPOINT}")
