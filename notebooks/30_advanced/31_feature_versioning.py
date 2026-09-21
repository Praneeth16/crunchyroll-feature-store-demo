# Databricks notebook source
# MAGIC %md
# MAGIC # 31 · Changing a feature definition without breaking what is serving
# MAGIC
# MAGIC The question: **how do we decouple a feature-definition change for training from
# MAGIC what inference is already using, and how do we version feature definitions?**
# MAGIC
# MAGIC The short answer is that half of it is already structural and half of it is a
# MAGIC discipline you have to impose. This notebook separates the two by measuring them
# MAGIC against a live endpoint rather than describing them.
# MAGIC
# MAGIC | What a model depends on | How the endpoint resolves it | So an in-place change… |
# MAGIC |---|---|---|
# MAGIC | feature **tables** and the columns it looks up | from the **feature spec inside the model version** | …does not change what this model serves |
# MAGIC | on-demand **functions** (`FeatureFunction` → a UC function) | **by name, at request time** | …changes what this model serves, immediately |
# MAGIC
# MAGIC That asymmetry is the whole finding, and §4 demonstrates it: the same request is
# MAGIC scored, a UC function is redefined underneath the live endpoint, and the request is
# MAGIC scored again. The function is restored from `src/crfs/udfs.py` afterwards and the
# MAGIC restoration is verified, not assumed.
# MAGIC
# MAGIC Sections:
# MAGIC
# MAGIC 1. what the deployed version pinned
# MAGIC 2. fingerprint it, so drift becomes detectable
# MAGIC 3. drift report — is anything it depends on already different?
# MAGIC 4. **the experiment** — redefine a function under a live endpoint
# MAGIC 5. the safe pattern: version by name, never in place
# MAGIC 6. canary: two versions behind one endpoint, with a traffic split
# MAGIC 7. the fleet view — which registered models a change would affect
# COMMAND ----------
# MAGIC %pip install databricks-feature-engineering databricks-sdk mlflow --quiet
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
from src.crfs import candidates as C
from src.crfs import rails as R
from src.crfs import udfs as U
from src.crfs import versioning as V

cfg = Config.from_widgets(dbutils, extra_widgets={
    "vers_model": "crunchyroll_rail_ranker",   # the model whose spec is inspected
    "vers_viewer": "v0001",
    "run_udf_experiment": "true",   # redefine a UC function under the live endpoint
    "run_canary": "true",           # add a second served version at 10% traffic
    "canary_percent": "10",
})
spark.sql(f"USE {cfg.fq}")

import json
import time

import mlflow
mlflow.set_registry_uri("databricks-uc")
from mlflow.tracking import MlflowClient

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
mc = MlflowClient()

MODEL = cfg.t(cfg.extras["vers_model"])
ENDPOINT = cfg.rail_ranker_endpoint
VIEWER = cfg.extras["vers_viewer"]
results = {"model": MODEL, "endpoint": ENDPOINT}
# COMMAND ----------
# MAGIC %md
# MAGIC ## 1 · What the deployed version pinned
# MAGIC
# MAGIC Read from the endpoint, not from an alias. `@champion` is where promotion points
# MAGIC *today*; the endpoint serves an immutable version, and that version is the thing
# MAGIC whose feature spec is actually in the request path.
# COMMAND ----------
ep = w.serving_endpoints.get(name=ENDPOINT)
served = [(e.entity_name, e.entity_version, e.name)
          for e in ((ep.config.served_entities if ep.config else None) or [])]
print("endpoint:", ENDPOINT, "| state:", ep.state.ready if ep.state else "?")
for entity, version, name in served:
    print(f"  serving {entity} v{version} as '{name}'")

SERVED_VERSION = served[0][1] if served else None
assert SERVED_VERSION, f"{ENDPOINT} has no served entity -- run notebook 23 first"

alias_version = None
try:
    alias_version = mc.get_model_version_by_alias(MODEL, "champion").version
except Exception:
    pass
print(f"\n@champion -> v{alias_version} | endpoint -> v{SERVED_VERSION}")
if alias_version and alias_version != SERVED_VERSION:
    print("  these differ, which is correct behaviour: promotion moved the alias and")
    print("  nothing reached traffic until a deploy pinned the new version.")
results["served_version"] = SERVED_VERSION
results["champion_version"] = alias_version
# COMMAND ----------
model_uri = f"models:/{MODEL}/{SERVED_VERSION}"
spec = V.feature_spec_of(model_uri)

# Printed once so the spec's own layout is visible -- everything below reads it by
# key rather than by path, but a reader deserves to see the real document.
print("feature spec top-level keys:", sorted(spec.keys()))
print()
print(V.render_spec(spec))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 2 · Fingerprint the definitions, so a later change is detectable
# MAGIC
# MAGIC The spec says *which* objects this model resolves. It does not say what they
# MAGIC contained at training time — so on its own it cannot answer "has anything changed
# MAGIC since?". Two hashes close that gap, and they belong on the model version as tags
# MAGIC because that is the only place that survives a rebuild of everything else:
# MAGIC
# MAGIC * `feature_spec_hash` — the shape: which tables, functions and features.
# MAGIC * `feature_definition_fingerprint` — the content: table schemas and function
# MAGIC   bodies as they are right now.
# MAGIC
# MAGIC A model whose spec hash matches but whose fingerprint does not is the interesting
# MAGIC case: same lookups, redefined underneath.
# COMMAND ----------
tags = V.training_tags(spark, spec)
print("computed:", json.dumps(tags, indent=2))

mv = mc.get_model_version(MODEL, SERVED_VERSION)
existing = dict(mv.tags or {})
for k, val in tags.items():
    if existing.get(k) == val:
        print(f"  {k}: already tagged, unchanged")
        continue
    if k in existing:
        # Do NOT overwrite a fingerprint recorded at training time -- that value is the
        # baseline, and replacing it with today's would erase the very drift this is
        # meant to detect. Retraining is what writes a new one.
        print(f"  {k}: recorded at training as {existing[k]}, keeping it")
        continue
    mc.set_model_version_tag(MODEL, SERVED_VERSION, k, val)
    print(f"  {k}: set to {val} (first time -- this version predates the convention,")
    print("     so it is a baseline from now rather than from training)")

baseline_fp = dict(mc.get_model_version(MODEL, SERVED_VERSION).tags or {}).get(V.TAG_FINGERPRINT)
results["fingerprint_baseline"] = baseline_fp
# COMMAND ----------
# MAGIC %md
# MAGIC ## 3 · Drift report
# MAGIC
# MAGIC Every table and function the served version pins, checked for existence and for
# MAGIC change. `scripts/verify.sh` covers the existence half by name and stays read-only;
# MAGIC the fingerprint comparison needs the model artifact, so it lives here.
# COMMAND ----------
report = V.drift_report(spark, model_uri, recorded_fingerprint=baseline_fp)
print(V.render_drift(report))
results["drift_before"] = report
# COMMAND ----------
# MAGIC %md
# MAGIC ## 4 · The experiment: redefine a function under the live endpoint
# MAGIC
# MAGIC `cr_rail_taste_match` is an on-demand feature: the model's spec names it, and
# MAGIC Model Serving calls it **by name, per request**. Nothing about it is copied into
# MAGIC the model artifact, which means an in-place redefinition is a production change
# MAGIC with no deploy, no version bump and no audit trail on the model.
# MAGIC
# MAGIC Three measurements against the same request:
# MAGIC
# MAGIC 1. baseline — the ranking the endpoint returns now,
# MAGIC 2. after `CREATE OR REPLACE FUNCTION` returns a constant 0.0,
# MAGIC 3. after restoring the definition from `src/crfs/udfs.py`.
# MAGIC
# MAGIC If (2) differs from (1) then on-demand features are **not** pinned, and the rule
# MAGIC in §5 is not style advice. The restore runs in a `finally` and is verified by
# MAGIC comparing (3) against (1).
# COMMAND ----------
rails_for_viewer = R.eligible_rails(spark, cfg.fq, VIEWER)
rail_ids = list(rails_for_viewer["rail_id"])
# A frozen request clock: cr_rail_click_recency and cr_session_decay both decay against
# it, so a moving clock would make the three measurements differ for a reason that has
# nothing to do with the redefinition.
EPOCH = int(time.time())
records = R.rail_request_records(VIEWER, rail_ids, device="tv", hour_of_day=21,
                                request_epoch_s=EPOCH)
print(f"viewer {VIEWER}: {len(records)} eligible rails, request clock frozen at {EPOCH}")


def ranked():
    preds, ms = C.query_ranker(w, ENDPOINT, records)
    order = {p["rail_id"]: (p["rail_rank"], round(float(p["engagement_probability"]), 6))
             for p in preds}
    return order, ms


def compare(a, b):
    moved = [k for k in a if k in b and a[k][0] != b[k][0]]
    dscore = max((abs(a[k][1] - b[k][1]) for k in a if k in b), default=0.0)
    return moved, dscore
# COMMAND ----------
baseline, ms = ranked()
print(f"baseline ({ms:0.0f} ms):")
for rid, (rank, prob) in sorted(baseline.items(), key=lambda kv: kv[1][0])[:6]:
    print(f"  {rank:2d}  {rid:16s} {prob:0.4f}")
results["baseline_top"] = sorted(baseline.items(), key=lambda kv: kv[1][0])[:6]
# COMMAND ----------
ZEROED_DDL = f"""CREATE OR REPLACE FUNCTION {cfg.fq}.cr_rail_taste_match(
  {", ".join(U.AFFINITY_ARGS + ["rail_genre_idx BIGINT"])}
)
RETURNS DOUBLE
LANGUAGE PYTHON
COMMENT 'TEMPORARY -- notebook 31 experiment. Returns a constant so the effect of an
in-place redefinition on a live endpoint is visible. Restored from src/crfs/udfs.py in
the same run.'
AS $$
return 0.0
$$"""

experiment = {"ran": False}
if cfg.extras["run_udf_experiment"].strip().lower() != "true":
    print("run_udf_experiment=false, skipping")
else:
    try:
        spark.sql(ZEROED_DDL)
        print("redefined cr_rail_taste_match to return 0.0 -- no deploy, no version bump")
        # The endpoint resolves the function per request, but give the catalog a moment
        # so a cached plan cannot be mistaken for "the change had no effect".
        time.sleep(15)
        after, ms_after = ranked()
        moved, dscore = compare(baseline, after)
        print(f"\nafter redefinition ({ms_after:0.0f} ms): {len(moved)} of {len(baseline)} "
              f"rails changed rank, max |delta score| = {dscore:0.4f}")
        for rid, (rank, prob) in sorted(after.items(), key=lambda kv: kv[1][0])[:6]:
            was = baseline.get(rid, ("-", float("nan")))
            print(f"  {rank:2d}  {rid:16s} {prob:0.4f}   (was rank {was[0]}, {was[1]:0.4f})")
        experiment.update(ran=True, rails_moved=len(moved), max_score_delta=dscore,
                          n_rails=len(baseline))
    finally:
        for ddl in U.rail_ddl(cfg.fq):
            spark.sql(ddl)
        print("\nrestored the rail UDFs from src/crfs/udfs.py")
        time.sleep(15)
        restored, _ = ranked()
        moved_back, dback = compare(baseline, restored)
        print(f"restored vs baseline: {len(moved_back)} rails differ, "
              f"max |delta score| = {dback:0.6f}")
        # The restore is the part that must not be taken on trust: an experiment that
        # leaves a demo's UDF returning a constant is worse than no experiment.
        assert not moved_back and dback < 1e-6, (
            "restore did not reproduce the baseline ranking -- investigate before "
            "showing this workspace to anyone")
        experiment["restored"] = True
results["udf_experiment"] = experiment
# COMMAND ----------
# MAGIC %md
# MAGIC ### What that means
# MAGIC
# MAGIC * **Table features are decoupled for free.** The spec inside the model version
# MAGIC   names the tables and columns; rebuilding a feature table with new maths does not
# MAGIC   change which columns this model reads, and adding a column changes nothing at
# MAGIC   all. Training can move ahead of serving safely.
# MAGIC * **On-demand functions are shared mutable state.** They are the one place where
# MAGIC   "change a feature definition" and "change what production returns" are the same
# MAGIC   action. Treat a UC function in a feature spec the way you would treat a
# MAGIC   deployed library: additive new names, never an in-place edit.
# COMMAND ----------
# MAGIC %md
# MAGIC ## 5 · The safe pattern: version by name
# MAGIC
# MAGIC Create the new definition beside the old one and let the old models keep
# MAGIC resolving the old name. A retrain binds to the new one, and the two coexist until
# MAGIC nothing references the old — which §7 can tell you.
# MAGIC
# MAGIC The same rule covers the three classes of change:
# MAGIC
# MAGIC | Change | On the GA path | With Feature Views | Who breaks |
# MAGIC |---|---|---|---|
# MAGIC | add a column / a new feature | add to the table; old spec ignores it | register a new `Feature` | nobody |
# MAGIC | change the maths of an existing feature | new **column** `x_v2`, or a new table | new `Feature` name | nobody, if you do not touch the old name |
# MAGIC | change an on-demand function | new **function** `cr_..._v2` | new `CustomUDF` binding | everything serving it, **immediately**, if edited in place |
# MAGIC | remove a feature | drop only once §7 shows no model pins it | same | whatever still pins it |
# COMMAND ----------
# The v2 function: same signature, deliberately different behaviour (mean affinity
# rather than the rail's genre). Creating it affects nothing -- no spec names it yet.
V2_NAME = "cr_rail_taste_match_v2"
V2_DDL = f"""CREATE OR REPLACE FUNCTION {cfg.fq}.{V2_NAME}(
  {", ".join(U.AFFINITY_ARGS + ["rail_genre_idx BIGINT"])}
)
RETURNS DOUBLE
LANGUAGE PYTHON
COMMENT 'v2 of rail taste match: the viewer''s mean affinity across all genres, so a
personalized rail is scored on breadth rather than on one genre. Created beside v1 --
models trained against v1 keep resolving v1, and a retrain binds to whichever name its
FeatureFunction list asks for.'
AS $$
vals = [{", ".join(f"aff_{g}" for g in U.GENRES)}]
clean = [v for v in vals if v is not None]
return 0.0 if not clean else float(sum(clean) / len(clean))
$$"""
spark.sql(V2_DDL)
print(f"created {cfg.t(V2_NAME)}")

both = spark.sql(f"SHOW FUNCTIONS IN {cfg.fq} LIKE 'cr_rail_taste_match*'").collect()
print("\nboth definitions now exist, and the live model still pins v1:")
for r in both:
    print("  ", r[0])

# The live endpoint is unaffected by the new function's existence -- measured, not stated.
after_v2, _ = ranked()
moved_v2, d_v2 = compare(baseline, after_v2)
print(f"\nendpoint after creating v2: {len(moved_v2)} rails moved, max delta {d_v2:0.6f}")
assert not moved_v2, "creating a new function name changed a live ranking -- it must not"
results["v2_created"] = cfg.t(V2_NAME)
# COMMAND ----------
# MAGIC %md
# MAGIC ## 6 · Canary: two versions, one endpoint
# MAGIC
# MAGIC `docs/open_items.md` §3 lists traffic splitting as not demonstrated: the endpoint
# MAGIC pins one immutable version and sends it 100% of traffic. This does the split — the
# MAGIC previous model version served alongside the current one at a small share — reads
# MAGIC the realised config back, and then restores 100% to the served version.
# MAGIC
# MAGIC This is how a *model* is rolled out. A **feature-definition** change cannot be
# MAGIC canaried this way unless it is bound to a new model version, which is the
# MAGIC practical reason §5's rule exists: version the definition by name, retrain, and
# MAGIC the canary mechanism you already have covers the feature change too.
# COMMAND ----------
versions = sorted((int(v.version) for v in mc.search_model_versions(f"name='{MODEL}'")),
                  reverse=True)
CANARY_PCT = int(cfg.extras["canary_percent"])
prev = next((v for v in versions if str(v) != str(SERVED_VERSION)), None)
canary = {"ran": False, "candidate": prev, "percent": CANARY_PCT}

if cfg.extras["run_canary"].strip().lower() != "true":
    print("run_canary=false, skipping")
elif prev is None:
    print(f"{MODEL} has only one version, so there is nothing to canary against")
else:
    live = (ep.config.served_entities or [])[0]


    def sized(name, version):
        """Copy the sizing mode the endpoint actually realised.

        `workload_size` and the provisioned-concurrency pair are mutually exclusive in
        the API, and notebook 23 asks for the explicit pair but falls back to
        `workload_size` if the workspace rejects it. On a workspace that took the
        fallback, the concurrency fields are None -- sending them as null while omitting
        workload_size preserves neither mode, and can reject the update or silently reset
        capacity on the demo's request-path endpoint.
        """
        out = {"name": name, "entity_name": live.entity_name,
               "entity_version": str(version), "scale_to_zero_enabled": False,
               "workload_type": live.workload_type or "CPU"}
        if live.min_provisioned_concurrency is not None:
            out["min_provisioned_concurrency"] = live.min_provisioned_concurrency
            out["max_provisioned_concurrency"] = live.max_provisioned_concurrency
        else:
            out["workload_size"] = live.workload_size
        return out


    base = sized(live.name, live.entity_version)
    cand_name = f"{cfg.extras['vers_model']}-{prev}"
    cand = sized(cand_name, prev)
    body = {
        "served_entities": [base, cand],
        "traffic_config": {"routes": [
            {"served_entity_name": base["name"], "traffic_percentage": 100 - CANARY_PCT},
            {"served_entity_name": cand_name, "traffic_percentage": CANARY_PCT},
        ]},
    }
    print(json.dumps(body, indent=2))
    w.api_client.do("PUT", f"/api/2.0/serving-endpoints/{ENDPOINT}/config", body=body)
    print(f"\nsplit requested: v{SERVED_VERSION} {100 - CANARY_PCT}% / v{prev} {CANARY_PCT}%")
# COMMAND ----------
def wait_config(timeout_s=1800):
    """Wait on the endpoint's own config-update state rather than sleeping.

    A served entity has to be built and brought up, so this is minutes. Reporting the
    state each time makes a slow update distinguishable from a stuck one.
    """
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        e = w.serving_endpoints.get(name=ENDPOINT)
        st = e.state
        cur = (getattr(st, "config_update", None), getattr(st, "ready", None))
        if cur != last:
            print(f"  config_update={cur[0]} ready={cur[1]}")
            last = cur
        if str(cur[0]) in ("EndpointStateConfigUpdate.NOT_UPDATING", "NOT_UPDATING"):
            return e
        time.sleep(20)
    raise TimeoutError(f"{ENDPOINT} config update did not settle in {timeout_s}s")


# Everything after the split PUT runs inside try/finally. If wait_config() times out or
# route handling raises, the endpoint would otherwise be left sending 10% of the
# homepage's traffic to a candidate version with nothing to put it back.
def measure_split():
    e = wait_config()
    routes = [(r.served_entity_name, r.traffic_percentage)
              for r in ((e.config.traffic_config.routes if e.config and e.config.traffic_config else None) or [])]
    print("\nrealised routes:", routes)
    canary.update(ran=True, routes=routes)

    # Fire a handful of requests across the split. The response carries no version, so
    # this proves the endpoint still answers correctly under a split rather than
    # attributing individual requests -- the inference table is where attribution lives.
    oks = 0
    for _ in range(10):
        try:
            preds, _ms = C.query_ranker(w, ENDPOINT, records)
            oks += 1 if preds else 0
        except Exception as ex:
            print("  request failed:", type(ex).__name__, str(ex)[:120])
    print(f"{oks}/10 requests answered while the split was live")
    canary["requests_ok"] = oks


def restore_single_version():
    """Put the endpoint back to one pinned version at 100%. Runs unconditionally."""
    w.api_client.do("PUT", f"/api/2.0/serving-endpoints/{ENDPOINT}/config", body={
        "served_entities": [sized(live.name, live.entity_version)],
        "traffic_config": {"routes": [{"served_entity_name": live.name,
                                       "traffic_percentage": 100}]},
    })
    e = wait_config()
    routes = [(r.served_entity_name, r.traffic_percentage)
              for r in ((e.config.traffic_config.routes if e.config and e.config.traffic_config else None) or [])]
    print("restored routes:", routes)
    assert routes == [(live.name, 100)], f"endpoint left in an unexpected state: {routes}"
    canary["restored"] = True


if canary["candidate"] is not None and cfg.extras["run_canary"].strip().lower() == "true":
    try:
        measure_split()
    finally:
        restore_single_version()
results["canary"] = canary
# COMMAND ----------
# MAGIC %md
# MAGIC ### The endpoint is left exactly as notebook 23 configured it
# MAGIC
# MAGIC The restore above runs in the `finally`, so this holds even if the measurement
# MAGIC failed. Verified by reading the routes back and asserting, not by assuming.
# COMMAND ----------
# MAGIC ## 7 · Which models would a change affect?
# MAGIC
# MAGIC Before touching a definition, this is the question to answer, and Unity Catalog
# MAGIC already holds it: every registered model in this schema, the objects its latest
# MAGIC version pins, and whether those objects still match what was recorded. A feature
# MAGIC nobody pins is safe to delete; one that three models pin is not.
# COMMAND ----------
models = [m.name for m in mc.search_registered_models(filter_string=f"catalog='{cfg.catalog}'")
          if m.name.startswith(f"{cfg.catalog}.{cfg.schema}.")]
print(f"{len(models)} registered models in {cfg.fq}\n")

fleet = []
pins = {}

for name in sorted(models):
    short = name.split(".")[-1]
    try:
        vs = sorted((int(v.version) for v in mc.search_model_versions(f"name='{name}'")),
                    reverse=True)
    except Exception as e:
        fleet.append({"model": short, "version": None, "tables": 0, "functions": 0,
                      "ok": None, "findings": [f"{type(e).__name__}: {str(e)[:100]}"]})
        continue
    if not vs:
        continue
    uri = f"models:/{name}/{vs[0]}"
    try:
        # One download per model. The first version of this cell called
        # feature_spec_of() here and again in a second loop to build the reverse index,
        # which downloaded every model's artifacts twice.
        s = V.feature_spec_of(uri)
    except ValueError as e:
        # A model logged with mlflow.pyfunc rather than fe.log_model has no spec. That is
        # a real distinction -- the caller has to supply features itself -- so it is
        # reported rather than skipped.
        fleet.append({"model": short, "version": vs[0], "tables": 0, "functions": 0,
                      "ok": None, "findings": [str(e)[:120]]})
        continue

    tag = dict(mc.get_model_version(name, str(vs[0])).tags or {}).get(V.TAG_FINGERPRINT)
    fp = V.definition_fingerprint(spark, s)
    missing_t = [k for k, v in fp["tables"].items() if v is None]
    missing_f = [k for k, v in fp["functions"].items() if v is None]
    findings = ([f"BROKEN table {k}" for k in missing_t]
                + [f"BROKEN function {k}" for k in missing_f])
    now = V.fingerprint_hash(fp)
    if tag and tag != now and not findings:
        findings.append(f"CHANGED definitions since training ({tag} -> {now})")
    fleet.append({"model": short, "version": vs[0],
                  "tables": len(V.spec_tables(s)), "functions": len(V.spec_functions(s)),
                  "ok": not findings, "findings": findings})
    for obj in V.spec_tables(s) + V.spec_functions(s):
        pins.setdefault(obj, []).append(short)

print(f"{'model':34s} {'ver':>4s} {'tables':>7s} {'funcs':>6s}  status")
for r in fleet:
    status = ("no feature spec" if r["ok"] is None
              else ("ok" if r["ok"] else r["findings"][0][:60]))
    print(f"{r['model']:34s} {str(r['version']):>4s} {r['tables']:>7d} {r['functions']:>6d}  {status}")

print("\nwho pins what -- this is the answer to 'what breaks if I change this':")
for obj, users in sorted(pins.items(), key=lambda kv: (-len(kv[1]), kv[0])):
    print(f"  {obj.split('.')[-1]:34s} {len(users)}  {', '.join(sorted(set(users)))}")
results["pins"] = {k.split('.')[-1]: sorted(set(v)) for k, v in pins.items()}
results["fleet"] = fleet
results["pins"] = {k.split('.')[-1]: v for k, v in pins.items()}
# COMMAND ----------
dbutils.notebook.exit(json.dumps(results, default=str))
