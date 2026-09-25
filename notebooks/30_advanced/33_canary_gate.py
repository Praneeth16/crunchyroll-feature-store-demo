# Databricks notebook source
# MAGIC %md
# MAGIC # 33 · Canary gate: judge a challenger behind the live endpoint, then promote or roll back
# MAGIC
# MAGIC `docs/open_items.md` §3: notebook 31 showed a **traffic split** works, but a split
# MAGIC is not a rollout. What was missing is the process around it — a metric to judge
# MAGIC the canary on, a gate, and an automatic rollback. This notebook is that process.
# MAGIC
# MAGIC 1. resolve the champion (what `crunchyroll-rail-ranker` serves now) and the
# MAGIC    challenger (`crunchyroll_rail_ranker_gpu@challenger` from notebook 32, else the
# MAGIC    champion's previous version)
# MAGIC 2. put the challenger behind the endpoint at `canary_percent`
# MAGIC 3. score the **same** eligible-rail requests on each served entity directly —
# MAGIC    paired, so error rate, latency and ranking agreement compare like with like
# MAGIC 4. push routed traffic through the split so the inference table has both entities
# MAGIC 5. gate → **PROMOTE** or **ROLLBACK**, with every check and its value, appended to
# MAGIC    `canary_decisions`
# MAGIC 6. `finally`: restore the champion at 100% — unless `apply=true` **and** the gate
# MAGIC    said PROMOTE, in which case the challenger takes 100%
# MAGIC
# MAGIC `apply` defaults to **false**: the demo endpoint stays on its champion and the gate
# MAGIC is a dry run whose decision is still recorded.
# COMMAND ----------
# MAGIC %pip install databricks-sdk mlflow --quiet
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import os, sys
_root = os.getcwd()
while _root != "/" and not os.path.isdir(os.path.join(_root, "src", "crfs")):
    _root = os.path.dirname(_root)
assert os.path.isdir(os.path.join(_root, "src", "crfs")), \
    f"src/crfs not found above {os.getcwd()} -- is the bundle's whole file tree synced?"
if _root not in sys.path:
    sys.path.insert(0, _root)
from src.crfs.config import Config
from src.crfs import canary as K
from src.crfs import rails as R

import datetime as dt
import json
import random
import statistics
import time

from databricks.sdk import WorkspaceClient
from mlflow.tracking import MlflowClient

cfg = Config.from_widgets(dbutils, extra_widgets={
    "challenger_model": "crunchyroll_rail_ranker_gpu",
    "challenger_alias": "challenger",
    "challenger_version": "",        # explicit version overrides the alias
    "canary_percent": "10",
    "n_viewers": "40",
    "routed_requests": "200",
    "max_error_rate": "0.01",
    "max_p95_ratio": "1.25",
    "min_spearman": "0.5",
    "apply": "false",
})
X = cfg.extras
w = WorkspaceClient()
mc = MlflowClient(registry_uri="databricks-uc")
ENDPOINT = cfg.rail_ranker_endpoint
CHAMP_MODEL = cfg.t("crunchyroll_rail_ranker")
PCT = int(X["canary_percent"])
APPLY = X["apply"].strip().lower() == "true"
THRESH = {"max_error_rate": float(X["max_error_rate"]),
          "max_p95_ratio": float(X["max_p95_ratio"]),
          "min_spearman": float(X["min_spearman"])}
print(f"endpoint={ENDPOINT} canary={PCT}% apply={APPLY} thresholds={THRESH}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 1 · Champion and challenger
# COMMAND ----------
ep = w.serving_endpoints.get(name=ENDPOINT)
live = (ep.config.served_entities or [])[0]
assert K.routes(ep) == [(live.name, 100)], \
    f"{ENDPOINT} is not on a single 100% route ({K.routes(ep)}) -- a previous canary leaked?"
print(f"champion: {live.entity_name} v{live.entity_version} as {live.name}")


def resolve_challenger():
    name = cfg.t(X["challenger_model"])
    try:
        if X["challenger_version"].strip():
            return name, X["challenger_version"].strip()
        return name, mc.get_model_version_by_alias(name, X["challenger_alias"]).version
    except Exception as ex:
        print(f"no {name}@{X['challenger_alias']} ({type(ex).__name__}); "
              f"falling back to the champion's previous version")
    versions = sorted((int(v.version) for v in mc.search_model_versions(f"name='{CHAMP_MODEL}'")),
                      reverse=True)
    prev = next((v for v in versions if v < int(live.entity_version)), None)
    assert prev is not None, f"{CHAMP_MODEL} has no version below {live.entity_version}"
    return CHAMP_MODEL, str(prev)


CAND_MODEL, CAND_VERSION = resolve_challenger()
CAND_NAME = f"{CAND_MODEL.split('.')[-1]}-{CAND_VERSION}"
print(f"challenger: {CAND_MODEL} v{CAND_VERSION} as {CAND_NAME}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 2 · The requests: real eligible sets, one frozen context
# MAGIC
# MAGIC Eligibility comes from `rails.eligible_rails_all`, the same rules the homepage and
# MAGIC the batch path use. The context is frozen (21:00 UTC, TV) so a rerun judges the
# MAGIC challenger on identical inputs.
# COMMAND ----------
random.seed(33)
viewers = [r["viewer_id"] for r in spark.sql(f"SELECT viewer_id FROM {cfg.fq}.viewers").collect()]
sample = sorted(random.sample(viewers, min(int(X["n_viewers"]), len(viewers))))
pairs = R.eligible_rails_all(spark, cfg.fq, viewers=sample).toPandas()
at = dt.datetime(2026, 8, 31, 21, tzinfo=dt.timezone.utc)
requests = {
    v: R.rail_request_records(v, sorted(g["rail_id"]), device="tv", locale="en-US",
                              hour_of_day=at.hour, day_of_week=at.weekday(),
                              request_epoch_s=int(at.timestamp()))
    for v, g in pairs.groupby("viewer_id")}
print(f"{len(requests)} viewers, {sum(len(r) for r in requests.values())} rail rows")


def ranks(preds, recs):
    """{rail_id: rank}. The champion returns rail_rank; a challenger that returns only
    probabilities -- as dicts with rail_id, or bare floats in request order -- is ranked
    by score, so it is judged on its ordering rather than failed as a bad response."""
    if preds and isinstance(preds[0], dict) and "rail_rank" in preds[0]:
        return {p["rail_id"]: int(p["rail_rank"]) for p in preds}
    if preds and isinstance(preds[0], dict) and "engagement_probability" in preds[0]:
        scores = {p["rail_id"]: float(p["engagement_probability"]) for p in preds}
    elif preds and not isinstance(preds[0], dict) and len(preds) == len(recs):
        scores = {r["rail_id"]: float(p) for r, p in zip(recs, preds)}
    else:
        return {}
    return {rid: i + 1 for i, rid in enumerate(sorted(scores, key=lambda r: -scores[r]))}
# COMMAND ----------
# MAGIC %md
# MAGIC ## 3 · Split, measure, decide — restore in `finally`
# COMMAND ----------
base = K.sized(live, live.name, live.entity_version)
cand = K.sized(live, CAND_NAME, CAND_VERSION, entity_name=CAND_MODEL)
decision, checks, report = "ROLLBACK", [("split", False, "not reached")], {}
started = dt.datetime.now(dt.timezone.utc)

try:
    K.put_config(w, ENDPOINT, K.split_body(base, cand, PCT))
    print(f"split requested: {base['name']} {100 - PCT}% / {CAND_NAME} {PCT}%")
    e = K.wait_config(w, ENDPOINT)
    report["routes"] = K.routes(e)
    print("realised routes:", report["routes"])

    # Paired, direct-to-entity scoring. Warm each entity first: its first request pays
    # container start-up, which is a deployment cost, not the challenger's latency.
    first = next(iter(requests.values()))
    for name in (base["name"], CAND_NAME):
        K.invoke_served(w, ENDPOINT, name, first)
    samples = {base["name"]: [], CAND_NAME: []}
    rho, top3 = [], []
    for v, recs in requests.items():
        got = {}
        for name in (base["name"], CAND_NAME):
            preds, ms, status = K.invoke_served(w, ENDPOINT, name, recs)
            r = ranks(preds, recs)
            if status == "ok" and len(r) != len(recs):
                status = f"bad_response: {len(r)} ranks for {len(recs)} rails"
            samples[name].append({"ms": ms, "status": status})
            got[name] = r if status == "ok" else None
        if got[base["name"]] and got[CAND_NAME]:
            rho.append(K.spearman(got[base["name"]], got[CAND_NAME]))
            top3.append(K.top_k_overlap(got[base["name"]], got[CAND_NAME], 3))

    champ_s, cand_s = K.summarise(samples[base["name"]]), K.summarise(samples[CAND_NAME])
    agreement = {"pairs": len(rho),
                 "spearman_mean": statistics.fmean(rho) if rho else float("nan"),
                 "top3_overlap_mean": statistics.fmean(top3) if top3 else float("nan")}
    report.update(champion=champ_s, challenger=cand_s, agreement=agreement)
    print(json.dumps(report, indent=2, default=str))

    # Routed traffic, so the inference table records both served entities.
    routed_ok = 0
    vs = list(requests)
    for i in range(int(X["routed_requests"])):
        try:
            w.serving_endpoints.query(name=ENDPOINT, dataframe_records=requests[vs[i % len(vs)]])
            routed_ok += 1
        except Exception as ex:
            if routed_ok == 0 and i < 3:
                print("routed request failed:", type(ex).__name__, str(ex)[:120])
    report["routed_ok"] = routed_ok
    print(f"{routed_ok}/{X['routed_requests']} routed requests answered under the split")

    decision, checks = K.gate(champ_s, cand_s, agreement, THRESH)
finally:
    promote = APPLY and decision == "PROMOTE"
    K.put_config(w, ENDPOINT, K.single_body(cand if promote else base))
    e = K.wait_config(w, ENDPOINT)
    final = K.routes(e)
    expected = [((cand if promote else base)["name"], 100)]
    print("final routes:", final)
    assert final == expected, f"endpoint left in an unexpected state: {final}"
    report["final_routes"] = final

print(f"\nDECISION: {decision}" + ("" if APPLY else "  (apply=false: champion restored)"))
for name, ok, detail in checks:
    print(f"  {'PASS' if ok else 'FAIL'}  {name:15s} {detail}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 4 · Promotion is two steps, as everywhere else in this repo
# MAGIC
# MAGIC Routing 100% to the challenger is deployment. Moving `@champion` is promotion, and
# MAGIC it only makes sense when the challenger is a version of the **same** UC model —
# MAGIC a GPU model is a different model, so its promotion is the route change plus the
# MAGIC decision record, and notebook 23 must be pointed at it explicitly before a redeploy.
# COMMAND ----------
if APPLY and decision == "PROMOTE" and CAND_MODEL == CHAMP_MODEL:
    mc.set_registered_model_alias(CHAMP_MODEL, "champion", CAND_VERSION)
    print(f"{CHAMP_MODEL}@champion -> v{CAND_VERSION}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 5 · Attribution from the inference table, and the decision record
# MAGIC
# MAGIC The inference table lands asynchronously (minutes), so this reads what has arrived
# MAGIC and says so if it is not there yet. It is evidence that the split carried traffic
# MAGIC to both entities, not an input to the gate.
# COMMAND ----------
inf_table = cfg.t("cr_rail_inference_payload")
try:
    attribution = spark.sql(f"""
        SELECT served_entity_id, COUNT(*) AS requests,
               SUM(CASE WHEN status_code = 200 THEN 0 ELSE 1 END) AS errors,
               PERCENTILE(execution_duration_ms, 0.95) AS p95_exec_ms
        FROM {inf_table}
        WHERE request_time >= TIMESTAMP'{started.strftime('%Y-%m-%d %H:%M:%S')}'
        GROUP BY served_entity_id""").toPandas()
    print(attribution.to_string(index=False) if len(attribution)
          else "no inference rows yet for this run (the table lags by minutes); rerun this cell later")
    report["inference_rows"] = int(attribution["requests"].sum()) if len(attribution) else 0
except Exception as ex:
    print(f"inference table unreadable: {type(ex).__name__}: {str(ex)[:200]}")

row = {"decided_at": dt.datetime.now(dt.timezone.utc).isoformat(), "endpoint": ENDPOINT,
       "champion": f"{live.entity_name}:{live.entity_version}",
       "challenger": f"{CAND_MODEL}:{CAND_VERSION}", "canary_percent": PCT,
       "decision": decision, "applied": bool(APPLY and decision == "PROMOTE"),
       "checks": json.dumps([{"check": c, "passed": bool(p), "detail": d} for c, p, d in checks]),
       "report": json.dumps(report, default=str)}
spark.createDataFrame([row]).write.mode("append").option("mergeSchema", "true") \
    .saveAsTable(cfg.t("canary_decisions"))
print(f"appended to {cfg.t('canary_decisions')}")
dbutils.notebook.exit(json.dumps({"decision": decision, "applied": row["applied"],
                                  "checks": json.loads(row["checks"]),
                                  "champion": report.get("champion"),
                                  "challenger": report.get("challenger"),
                                  "agreement": report.get("agreement")}, default=str))
