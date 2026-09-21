# Databricks notebook source
# MAGIC %md
# MAGIC # 29 · Does this workspace have the previews the advanced track needs?
# MAGIC
# MAGIC The three notebooks after this one are built on **Public Preview** APIs:
# MAGIC Feature Views (`fe.create_feature`, `fe.materialize_features`) and AI Runtime
# MAGIC serverless GPU. Preview availability is per workspace and per region, so run
# MAGIC this first: it reports what is actually here rather than what the docs say
# MAGIC should be.
# MAGIC
# MAGIC It changes nothing. No feature is registered, no table is written, no compute
# MAGIC beyond this notebook is started.
# MAGIC
# MAGIC What it answers:
# MAGIC
# MAGIC 1. Is `databricks-feature-engineering` >= 0.16.0 installable here (the floor for
# MAGIC    Feature Views), and does its entity surface match what the code expects?
# MAGIC 2. Do the five client methods exist, with the arguments the notebooks pass?
# MAGIC 3. Does a real Feature over `engagement_events` compute — i.e. is the preview
# MAGIC    enabled for this workspace rather than merely documented?
# MAGIC 4. Is the online store there to materialize into?
# COMMAND ----------
# MAGIC %pip install 'databricks-feature-engineering>=0.16.0' --quiet
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

cfg = Config.from_widgets(dbutils)
spark.sql(f"USE {cfg.fq}")

import inspect
import json

findings = {}
# COMMAND ----------
# MAGIC %md
# MAGIC ## 1 · Package floor and the entity surface
# MAGIC
# MAGIC Feature Views need `databricks-feature-engineering>=0.16.0`. The entity names
# MAGIC below are the whole authoring DSL: sources, one function wrapper, the
# MAGIC aggregation operators, the window shapes, and the two materialization configs.
# MAGIC Anything reported missing is a feature the later notebooks cannot use.
# COMMAND ----------
import databricks.feature_engineering as dfe
from databricks.feature_engineering import FeatureEngineeringClient

version = getattr(dfe, "__version__", "unknown")
print("databricks-feature-engineering:", version)
findings["fe_version"] = version

from databricks.feature_engineering import entities as fe_entities

WANTED = [
    # sources
    "DeltaTableSource", "StreamSource", "RequestSource",
    # the definition and its function wrapper
    "Feature", "AggregationFunction", "ColumnSelection",
    # aggregation operators
    "Sum", "Avg", "Count", "Min", "Max", "First", "Last", "ApproxCountDistinct",
    # window shapes
    "TumblingWindow", "SlidingWindow", "RollingWindow", "SawtoothWindow",
    # materialization
    "OfflineStoreConfig", "OnlineStoreConfig", "CronSchedule", "TableTrigger",
    "StreamingMode",
]
present = [n for n in WANTED if hasattr(fe_entities, n)]
missing = [n for n in WANTED if not hasattr(fe_entities, n)]
print("\npresent :", ", ".join(present))
print("MISSING :", ", ".join(missing) or "(none)")
findings["entities_present"] = present
findings["entities_missing"] = missing

# Everything the module exports, so an operator this list does not know about still
# shows up rather than staying invisible.
exported = sorted(n for n in dir(fe_entities) if n[:1].isupper())
print("\nall exported entities:")
print("  " + "\n  ".join(", ".join(exported[i:i + 6]) for i in range(0, len(exported), 6)))
findings["entities_all"] = exported
# COMMAND ----------
# MAGIC %md
# MAGIC ### What the DSL can actually express
# MAGIC
# MAGIC The docs list `Sum`, `Avg`, `Count` and say "limited list of functions (UDAFs)
# MAGIC supported". The installed package exports considerably more than that, and the
# MAGIC difference decides how much of `src/crfs/features.py` could ever be authored
# MAGIC declaratively. So the operators are read off the package rather than off the
# MAGIC docs, and the two escape hatches — `CustomUDF` and `RowTransformation` — get
# MAGIC their signatures printed, because whether they exist is the whole question for
# MAGIC features like a circular-mean watch hour.
# COMMAND ----------
OPERATORS = [n for n in exported
             if n in ("Sum", "Avg", "Count", "Min", "Max", "First", "Last", "FirstN",
                      "LastN", "FirstDistinct", "LastDistinct", "ApproxCountDistinct",
                      "ApproxPercentile", "PercentileApprox", "StddevSamp", "StddevPop",
                      "VarSamp", "VarPop")]
print("aggregation operators:", ", ".join(OPERATORS))
findings["operators"] = OPERATORS

for name in ["Feature", "CustomUDF", "RowTransformation", "ColumnSelection",
             "RequestSource", "FeatureViewSource", "DataFrameSource", "VolumeSource",
             "SlidingWindow", "TumblingWindow", "RollingWindow", "SawtoothWindow"]:
    cls = getattr(fe_entities, name, None)
    if cls is None:
        print(f"\n{name}: absent")
        continue
    try:
        sig = str(inspect.signature(cls))
    except (TypeError, ValueError):
        sig = "(signature unavailable)"
    doc = (inspect.getdoc(cls) or "").strip().splitlines()
    print(f"\n{name}{sig}")
    for line in doc[:6]:
        print("   ", line)
    findings.setdefault("dsl", {})[name] = sig
# COMMAND ----------
# MAGIC %md
# MAGIC ## 2 · The five client methods, and their real signatures
# MAGIC
# MAGIC Printed rather than trusted. The notebooks that follow pass these arguments by
# MAGIC keyword, so a renamed or absent parameter is the failure this cell catches —
# MAGIC cheaply, and before a 40-minute job hits it.
# COMMAND ----------
fe = FeatureEngineeringClient()

sigs = {}
for name in ["create_feature", "register_feature", "compute_features",
             "create_training_set", "materialize_features", "log_model",
             "score_batch", "publish_table", "create_table"]:
    fn = getattr(fe, name, None)
    if fn is None:
        sigs[name] = None
        print(f"{name}: MISSING")
        continue
    params = list(inspect.signature(fn).parameters)
    sigs[name] = params
    print(f"{name}({', '.join(params)})")
findings["signatures"] = sigs
# COMMAND ----------
# MAGIC %md
# MAGIC ### Registration surface
# MAGIC
# MAGIC A locally-defined `Feature` has no catalog or schema, and anything that needs its
# MAGIC full name raises `ValueError: Feature does not have a catalog and schema`. So the
# MAGIC object a caller keeps matters: `register_feature` **returns** the registered
# MAGIC feature, and that returned object is the one to pass to `create_training_set` and
# MAGIC `materialize_features`. This cell prints every client method so an idempotent
# MAGIC re-registration path can be written against what exists rather than guessed.
# COMMAND ----------
methods = sorted(m for m in dir(fe) if not m.startswith("_") and callable(getattr(fe, m)))
print("FeatureEngineeringClient methods:")
print("  " + "\n  ".join(", ".join(methods[i:i + 4]) for i in range(0, len(methods), 4)))
findings["client_methods"] = methods

for name in ["get_feature", "read_feature", "get_features", "list_features",
             "delete_feature", "get_feature_view", "drop_feature"]:
    print(f"  {name}: {'present' if hasattr(fe, name) else 'absent'}")

# Which of this repo's feature views are already registered in UC -- a re-run has to
# find them rather than fail on ALREADY_EXISTS.
try:
    from src.crfs import feature_views as FV

    local = FV.viewer_features(cfg.catalog, cfg.schema)
    getter = getattr(fe, "get_feature", None)
    for f in local[:2]:
        if getter is None:
            break
        try:
            got = getter(name=f"{cfg.catalog}.{cfg.schema}.{f.name}")
            print(f"  already registered: {f.name} -> {type(got).__name__}")
            findings.setdefault("registered_already", []).append(f.name)
        except Exception as e:
            print(f"  not registered yet: {f.name} ({type(e).__name__}: {str(e)[:80]})")
except Exception as e:
    print("registration check skipped:", type(e).__name__, str(e)[:120])
# COMMAND ----------
# MAGIC %md
# MAGIC ## 3 · Compute one real feature
# MAGIC
# MAGIC The only check that distinguishes *documented* from *enabled here*: define a
# MAGIC 7-day watch-seconds sum over `engagement_events` locally and compute it. Nothing
# MAGIC is registered in Unity Catalog and nothing is materialized, so this leaves no
# MAGIC object behind.
# MAGIC
# MAGIC `SlidingWindow` is used rather than `RollingWindow` deliberately — the docs say
# MAGIC batch rolling-window features cannot be materialized, and the point of this
# MAGIC probe is a definition the next notebook can actually publish.
# COMMAND ----------
from datetime import timedelta

try:
    from databricks.feature_engineering.entities import (
        DeltaTableSource, Feature, AggregationFunction, Sum, SlidingWindow)

    source = DeltaTableSource(
        catalog_name=cfg.catalog,
        schema_name=cfg.schema,
        table_name="engagement_events",
    )
    probe = Feature(
        name="probe_watch_seconds_7d",
        source=source,
        entity=["viewer_id"],
        timeseries_column="event_ts",
        function=AggregationFunction(
            Sum(input="watch_seconds"),
            SlidingWindow(window_duration=timedelta(days=7), slide_duration=timedelta(days=1)),
        ),
    )
    df = fe.compute_features(features=[probe])
    rows = df.limit(5).toPandas()
    print("compute_features returned", len(rows), "sample rows")
    print(rows.to_string(index=False))
    findings["compute_features"] = "ok"
    findings["compute_features_columns"] = list(rows.columns)
except Exception as e:
    # A preview that is off for this workspace fails here, and the message is the
    # thing worth keeping -- it names what to ask an admin to enable.
    findings["compute_features"] = f"FAILED: {type(e).__name__}: {e}"
    print(findings["compute_features"])
# COMMAND ----------
# MAGIC %md
# MAGIC ## 4 · Is there an online store to materialize into?
# MAGIC
# MAGIC The existing demo already owns a Lakebase-backed online store, created by
# MAGIC `fe.create_online_store` in notebook 01. Materializing feature views into that
# MAGIC same store is what keeps this a second authoring path rather than a second
# MAGIC piece of infrastructure to pay for.
# COMMAND ----------
try:
    store = fe.get_online_store(name=cfg.online_store)
    state = getattr(store, "state", None) or (store or {}).get("state") if store else None
    print("online store:", cfg.online_store, "->", state or store)
    findings["online_store"] = str(state or ("present" if store else "absent"))
except Exception as e:
    findings["online_store"] = f"FAILED: {type(e).__name__}: {e}"
    print(findings["online_store"])
# COMMAND ----------
# MAGIC %md
# MAGIC ## Verdict
# COMMAND ----------
ready = (findings.get("compute_features") == "ok" and not findings["entities_missing"])
print("feature views usable here:", ready)
if not ready:
    print("\nwhat to do:")
    if findings["entities_missing"]:
        print("  - entity classes missing:", findings["entities_missing"],
              "-> the installed package predates them, or the DSL moved")
    if findings.get("compute_features") != "ok":
        print("  - compute_features failed -> ask a workspace admin to enable the")
        print("    Feature Views preview on the Previews page, and check the region")

dbutils.notebook.exit(json.dumps({"feature_views_ready": ready, **findings}, default=str))
