# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install databricks-feature-engineering --quiet
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
OUT = []
def log(*a):
    OUT.append(" ".join(str(x) for x in a))

import databricks.feature_engineering as fe_pkg
log("package:", fe_pkg.__file__)

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
import inspect
fe = FeatureEngineeringClient()
methods = [m for m in dir(fe) if not m.startswith("_")]
log("FE client methods:", ", ".join(methods))

for m in ["create_table", "write_table", "create_training_set", "log_model",
          "create_online_store", "get_online_store", "list_online_stores",
          "publish_table", "publish_to_online_store", "score_batch",
          "read_table", "drop_table"]:
    if hasattr(fe, m):
        try:
            log("SIG", m, str(inspect.signature(getattr(fe, m))))
        except Exception as e:
            log("SIG", m, "error:", e)
    else:
        log("MISSING:", m)

try:
    log("FeatureLookup:", str(inspect.signature(FeatureLookup.__init__)))
except Exception as e:
    log("FeatureLookup error:", e)

try:
    from databricks.feature_engineering.entities.online_store import OnlineStore
    log("OnlineStore entity import OK")
except Exception as e:
    log("OnlineStore entity import err:", e)

import databricks.feature_engineering as fe_pkg2, importlib.metadata as md
try:
    log("dfe version:", md.version("databricks-feature-engineering"))
except Exception as e:
    log("version err:", e)

row = spark.sql("SELECT current_catalog() AS cat, current_user() AS usr").first()
log("catalog:", row.cat, "| user:", row.usr)
import sklearn, mlflow
log("sklearn:", sklearn.__version__, "| mlflow:", mlflow.__version__)

dbutils.notebook.exit("\n".join(OUT))
