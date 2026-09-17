# Databricks notebook source
# MAGIC %md
# MAGIC # 25 · What the endpoint does under load
# MAGIC
# MAGIC Vertical ranking sits in the homepage request path, so "it works" is not the
# MAGIC question. The questions are latency, concurrency, autoscaling and spikes, and
# MAGIC each one needs a number with the conditions it was measured under attached.
# MAGIC
# MAGIC This notebook runs **as a job, inside the workspace region**. That is
# MAGIC deliberate: the same benchmark from a laptop measures the laptop's distance to
# MAGIC the region, which on this workspace is 200–250 ms of pure network and would
# MAGIC swamp everything interesting. `make bench-local` runs the identical code from
# MAGIC outside for contrast, and the write-up reports both.
# MAGIC
# MAGIC | Phase | Question it answers |
# MAGIC |---|---|
# MAGIC | `fanout` | How does latency grow with candidate rails per request? Each rail is one more composite-key online lookup inside the same call. |
# MAGIC | `ramp` | Latency percentiles and achieved throughput at rising concurrency, with error codes counted. |
# MAGIC | `spike` | Step from low to high concurrency: what the first second costs, how long recovery takes, whether anything is rejected. |
# MAGIC | `features_only` | The same fanout against the Feature Serving endpoint — features, no model. Subtracting it attributes the online-lookup share of end-to-end latency. |
# MAGIC | `server_side` | `execution_time_ms` from the endpoint's own inference table, which excludes the network. |
# MAGIC
# MAGIC Nothing here is sampled from a marketing number. Every row of the output table
# MAGIC is measured, and the endpoint configuration the numbers belong to is recorded
# MAGIC beside them — because scale-to-zero, provisioned concurrency and route
# MAGIC optimization each move the answer more than any model change would.
# COMMAND ----------
# MAGIC %pip install databricks-feature-engineering --quiet
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import os, sys
_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _root not in sys.path:
    sys.path.insert(0, _root)

from src.crfs.config import Config
from src.crfs import rails as R
from src.crfs import loadtest as LT

cfg = Config.from_widgets(dbutils, extra_widgets={
    "bench_ramp_levels": "1,2,4,8,16,32,64",
    "bench_fanout_sizes": "1,4,8,16,32",
    "bench_level_seconds": "12",
    "bench_spike_to": "48",
    "bench_rails_per_request": "12",
    "bench_sustained_seconds": "600",     # 0 disables the sustained phase
    "bench_sustained_concurrency": "32",
    "bench_write_table": "crfs_serving_benchmark",
    "run_id": "",
})
spark.sql(f"USE {cfg.fq}")
print(cfg.describe())

SUSTAINED_S = float(cfg.extras["bench_sustained_seconds"])
SUSTAINED_C = int(cfg.extras["bench_sustained_concurrency"])
LEVELS = tuple(int(x) for x in cfg.extras["bench_ramp_levels"].split(",") if x.strip())
FANOUT = tuple(int(x) for x in cfg.extras["bench_fanout_sizes"].split(",") if x.strip())
LEVEL_S = float(cfg.extras["bench_level_seconds"])
SPIKE_TO = int(cfg.extras["bench_spike_to"])
RAILS_PER_REQ = int(cfg.extras["bench_rails_per_request"])
BENCH_TABLE = cfg.t(cfg.extras["bench_write_table"])
print(f"\nramp levels {LEVELS} | fanout {FANOUT} | {LEVEL_S}s per level | spike to {SPIKE_TO}")
# COMMAND ----------
import json, time
import pandas as pd
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
WHERE = "in_region_job"
STARTED_MS = int(time.time() * 1000)

# The run id is only a label on the result rows, so it must not be able to fail the job.
# `getContext().currentRunId()` is NOT whitelisted on serverless compute:
#   py4j.security.Py4JSecurityException: Method ... currentRunId() is not whitelisted
# and it killed this benchmark before a single measurement was taken. The supported route
# is the `{{job.run_id}}` task parameter, which resources/jobs.yml now passes; the
# timestamp fallback keeps the notebook runnable by hand from the workspace UI.
RUN_ID = (cfg.extras.get("run_id") or "").strip()
if not RUN_ID or RUN_ID.startswith("{{"):
    RUN_ID = f"manual-{STARTED_MS}"
print("run id label:", RUN_ID)
# COMMAND ----------
# MAGIC %md
# MAGIC ## The configuration these numbers belong to
# COMMAND ----------
ep_cfg = LT.endpoint_config_summary(w, cfg.rail_ranker_endpoint)
print(json.dumps(ep_cfg, indent=2))
# COMMAND ----------
# MAGIC %md
# MAGIC ## The payload: one row per candidate rail
# MAGIC
# MAGIC A homepage request carries a viewer, the request context, and the rails that
# MAGIC viewer is eligible for. The endpoint scores every one of them in a single
# MAGIC call, doing its own feature lookups, and returns them ranked.
# MAGIC
# MAGIC The fanout phase needs request shapes larger than the real eligible set, so
# MAGIC rails are repeated to reach the requested size. That over-states nothing:
# MAGIC repeating a rail still costs a lookup, which is what the phase is measuring.
# COMMAND ----------
viewers = spark.sql(f"""
    SELECT viewer_id FROM {cfg.t('viewer_rail_features_ts')}
    GROUP BY viewer_id ORDER BY SUM(vr_impressions_30d) DESC LIMIT 25
""").toPandas()["viewer_id"].tolist()
all_rails = spark.table(cfg.t("rails")).toPandas()["rail_id"].tolist()
print(f"{len(viewers)} viewers | {len(all_rails)} rails")


def payload_for(n_rails: int, viewer: str = None):
    v = viewer or viewers[0]
    ids = [all_rails[i % len(all_rails)] for i in range(n_rails)]
    return R.rail_request_records(v, ids, device="tv", locale="en-US",
                                  hour_of_day=21, day_of_week=5)


# Cycle viewers across the load phases so every request is not one cached key.
payloads = [R.rail_request_records(v, all_rails[:RAILS_PER_REQ], device="tv",
                                   locale="en-US", hour_of_day=21, day_of_week=5)
            for v in viewers]
print("example request row:", json.dumps(payloads[0][0], indent=2))
# COMMAND ----------
client = LT.EndpointClient(w, cfg.rail_ranker_endpoint, timeout_s=30.0)
print("url:", client.url, "| route_optimized:", client.route_optimized)

warm = LT.warmup(client, payloads[0], n=8)
print("warmup:", warm)
if warm["statuses"] != [200]:
    raise RuntimeError(f"endpoint is not answering cleanly: {warm} "
                       f"-- fix this before trusting any latency number below")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Phase 1 · fanout — the cost of one more candidate rail
# COMMAND ----------
fanout = LT.fanout_phase(client, payload_for, sizes=FANOUT, requests_per_size=40)
print(LT.markdown_table(fanout))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Phase 1b · sustained load, to measure how long capacity takes to arrive
# MAGIC
# MAGIC The ramp gives each level 12 seconds. That measures a steady state at *current*
# MAGIC capacity and is far too short to see capacity being added, which is why the
# MAGIC 2.6x scale-up in this endpoint's throughput was originally found by accident --
# MAGIC two ramp sweeps that happened to run ten minutes apart.
# MAGIC
# MAGIC This holds one concurrency level for ten minutes and reports throughput per
# MAGIC 30-second window. The output is the number capacity planning actually needs:
# MAGIC **how long after load arrives does throughput stop climbing.**
# MAGIC
# MAGIC It runs BEFORE the ramp and the spike, deliberately. The first version ran after
# MAGIC them and reported `scale_up_factor 1.0` from its very first window -- which reads
# MAGIC as "this endpoint never scales" and actually meant "the spike had already scaled
# MAGIC it". A phase measuring time-to-capacity has to be the first load the endpoint
# MAGIC sees in the run, or it measures nothing.
# COMMAND ----------
sustained, sustained_detail = [], {}
if SUSTAINED_S > 0:
    sustained, sustained_detail = LT.sustained_phase(
        client, payloads[0], concurrency=SUSTAINED_C,
        total_s=SUSTAINED_S, window_s=30.0)
    print(LT.markdown_table(sustained))
    print(f"\nfirst 30s window: {sustained_detail.get('first_window_rps')} req/s")
    print(f"best window:      {sustained_detail.get('best_window_rps')} req/s")
    print(f"scale-up factor:  {sustained_detail.get('scale_up_factor')}x")
    print(f"seconds to reach 90% of best: {sustained_detail.get('seconds_to_90pct_of_best')}")
else:
    print("sustained phase disabled (bench_sustained_seconds=0)")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Phase 2 · concurrency ramp
# MAGIC
# MAGIC Each level runs to a steady state with a settle gap before the next, so a
# MAGIC level measures itself rather than the transient left by the level below it.
# MAGIC Watch two things: where p95 starts to separate from p50 (the endpoint is
# MAGIC queueing) and whether achieved req/s keeps climbing (it still has headroom).
# COMMAND ----------
ramp = LT.ramp_phase(client, payloads[0], levels=LEVELS, duration_s=LEVEL_S, settle_s=3.0)
print(LT.markdown_table(ramp))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Phase 3 · a traffic spike
# MAGIC
# MAGIC Baseline, then a step change with no ramp, then back. The averages are the
# MAGIC least interesting part; the per-second p95 and the recovery time are the
# MAGIC answer to "what happens when a simulcast drops".
# COMMAND ----------
spike, spike_detail = LT.spike_phase(
    client, payloads[0], baseline=2, spike=SPIKE_TO,
    baseline_s=10.0, spike_s=25.0, recover_s=20.0)
print(LT.markdown_table(spike))
print("\nper-second p95 during the spike:")
for sec, row in (spike_detail.get("per_second") or {}).items():
    print(f"  t+{sec:>3}s  {row['requests']:>4} requests  p95 {row['p95_ms']} ms")
print("\nfirst second p95:", spike_detail.get("first_second_p95_ms"), "ms")
print("seconds to return within 1.5x baseline p95:",
      spike_detail.get("seconds_to_within_1_5x_baseline_p95"))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Phase 4 · features without a model
# MAGIC
# MAGIC The Feature Serving endpoint returns the same governed feature values with no
# MAGIC model behind it. Its fanout curve is the online-lookup share of the ranker's
# MAGIC latency, measured rather than assumed.
# COMMAND ----------
features_only = []
try:
    fclient = LT.EndpointClient(w, cfg.feature_endpoint, timeout_s=30.0)

    def feature_payload(n):
        v = viewers[0]
        titles = spark.sql(f"SELECT title_id FROM {cfg.t('title_features')} LIMIT {max(n,1)}") \
            .toPandas()["title_id"].tolist()
        titles = [titles[i % len(titles)] for i in range(n)]
        return [{"viewer_id": v, "title_id": t, "hour_of_day": 21,
                 "request_epoch_s": int(time.time())} for t in titles]

    LT.warmup(fclient, feature_payload(4), n=4)
    features_only = LT.fanout_phase(fclient, feature_payload, sizes=FANOUT,
                                    requests_per_size=30)
    for r in features_only:
        r.phase = "features_only"
    print(LT.markdown_table(features_only))
except Exception as e:
    print(f"feature-serving comparison skipped: {type(e).__name__}: {e}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Phase 5 · server-side time, from the endpoint's own inference table
# MAGIC
# MAGIC `execution_time_ms` is the endpoint's measurement of model execution. It
# MAGIC excludes the network between client and endpoint, so client wall time minus
# MAGIC this is the transport share — the part that changes when the caller moves,
# MAGIC and the part route optimization attacks.
# MAGIC
# MAGIC The table is written asynchronously, so this waits for rows from this run
# MAGIC rather than reporting an empty result as if it were a finding.
# COMMAND ----------
inf = (ep_cfg.get("inference_table") or {})
server_side = {}
if inf.get("enabled"):
    inf_table = f"{inf['catalog']}.{inf['schema']}.{inf['prefix']}_payload"
    for attempt in range(20):
        server_side = LT.inference_table_latency(spark, inf_table, STARTED_MS)
        if server_side.get("requests"):
            break
        time.sleep(15)
    print("inference table:", inf_table)
    print(json.dumps(server_side, indent=2))
    client_p50 = (ramp[0].latency or {}).get("p50_ms")
    if client_p50 and server_side.get("p50_ms"):
        print(f"\nat concurrency {ramp[0].concurrency}: client p50 {client_p50} ms "
              f"= server {server_side['p50_ms']} ms + {round(client_p50 - server_side['p50_ms'], 1)} ms transport")
else:
    print("no inference table on this endpoint; server-side attribution unavailable")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Persist the results
# MAGIC
# MAGIC One row per phase, appended, with the endpoint configuration and where the
# MAGIC client was. The dashboard reads this table, and a rerun after a config change
# MAGIC is a comparison rather than a replacement.
# COMMAND ----------
all_results = list(fanout) + list(ramp) + list(sustained) + list(spike) + list(features_only)
rows = []
for r in all_results:
    d = r.as_dict()
    lat = d.pop("latency") or {}
    rows.append({
        "run_id": str(RUN_ID),
        "measured_at": pd.Timestamp.utcnow().tz_localize(None),
        "where": WHERE,
        "endpoint": cfg.rail_ranker_endpoint if r.phase != "features_only" else cfg.feature_endpoint,
        "phase": d["phase"],
        "concurrency": int(d["concurrency"]),
        "rows_per_request": int(d["rows_per_request"]),
        "requests": int(d["requests"]),
        "ok": int(d["ok"]),
        "errors_json": json.dumps(d["errors"]),
        "duration_s": float(d["duration_s"]),
        "rps": float(d["rps"]),
        "p50_ms": float(lat.get("p50_ms") or 0.0),
        "p90_ms": float(lat.get("p90_ms") or 0.0),
        "p95_ms": float(lat.get("p95_ms") or 0.0),
        "p99_ms": float(lat.get("p99_ms") or 0.0),
        "max_ms": float(lat.get("max_ms") or 0.0),
        "note": d["note"],
        "endpoint_config_json": json.dumps(ep_cfg),
        "server_side_json": json.dumps(server_side),
    })
bench_pdf = pd.DataFrame(rows)
(spark.createDataFrame(bench_pdf).write.mode("append")
 .option("mergeSchema", "true").format("delta").saveAsTable(BENCH_TABLE))
spark.sql(f"COMMENT ON TABLE {BENCH_TABLE} IS "
          "'Measured Model Serving latency and throughput per phase. One row per "
          "benchmark phase; endpoint_config_json records the configuration the "
          "numbers belong to.'")
print(f"wrote {len(bench_pdf)} rows to {BENCH_TABLE}")
# COMMAND ----------
# MAGIC %md
# MAGIC ## The write-up
# MAGIC
# MAGIC Written to the ops volume as markdown so `make bench-pull` can drop it into
# MAGIC `docs/serving_benchmark.md`. A benchmark nobody can quote is a benchmark
# MAGIC nobody will act on.
# COMMAND ----------
def fmt_cfg(c):
    e = (c.get("served_entities") or [{}])[0]
    bits = [f"endpoint `{c['endpoint']}`",
            f"route_optimized={c['route_optimized']}",
            f"scale_to_zero={e.get('scale_to_zero')}"]
    if e.get("min_provisioned_concurrency") is not None:
        bits.append(f"provisioned_concurrency={e.get('min_provisioned_concurrency')}"
                    f"-{e.get('max_provisioned_concurrency')}")
    if e.get("workload_size"):
        bits.append(f"workload_size={e['workload_size']}")
    return " · ".join(bits)


md = [
    "# Rail-ranking endpoint — measured serving characteristics",
    "",
    f"Measured {pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M UTC')} from **{WHERE}** "
    f"(client in the same region as the endpoint).",
    "",
    f"Configuration: {fmt_cfg(ep_cfg)}",
    "",
    "Every number below is client-observed wall time unless it says otherwise, and "
    "every latency is reported with the concurrency it was measured at.",
    "",
    "## Fanout — latency vs candidate rails per request",
    "",
    "Each additional rail is one more composite-key read against the Lakebase online "
    "store, inside the same request.",
    "",
    LT.markdown_table(fanout),
    "",
    "## Concurrency ramp",
    "",
    LT.markdown_table(ramp),
    "",
    "## Sustained load — how long capacity takes to arrive",
    "",
    "The ramp above gives each level 12 seconds, which measures a steady state at "
    "current capacity and cannot see capacity being added. This phase holds one level "
    "and slices by wall-clock window.",
    "",
    (LT.markdown_table(sustained) if sustained
     else "_Sustained phase disabled for this run._"),
    "",
] + ([
    f"- concurrency held: **{sustained_detail.get('concurrency')}** for "
    f"{sustained_detail.get('total_s')}s",
    f"- first 30s window: **{sustained_detail.get('first_window_rps')} req/s**",
    f"- best window: **{sustained_detail.get('best_window_rps')} req/s**",
    f"- scale-up factor: **{sustained_detail.get('scale_up_factor')}x**",
    f"- seconds to reach 90% of best throughput: "
    f"**{sustained_detail.get('seconds_to_90pct_of_best')}**",
    "",
    "This is the number to size against: `min_provisioned_concurrency` is what you get "
    "immediately, `max` is what you get after this long.",
    "",
] if sustained_detail else []) + [
    "## Traffic spike",
    "",
    LT.markdown_table(spike),
    "",
    f"- first second of the spike, p95: **{spike_detail.get('first_second_p95_ms')} ms**",
    f"- seconds to return within 1.5x baseline p95: "
    f"**{spike_detail.get('seconds_to_within_1_5x_baseline_p95')}**",
    "",
]
if features_only:
    md += ["## Features without a model (Feature Serving endpoint)", "",
           "The online-lookup share of the ranker's latency, measured directly.", "",
           LT.markdown_table(features_only), ""]
if server_side.get("requests"):
    md += ["## Server-side execution time (from the inference table)", "",
           f"- requests captured: {int(server_side['requests'])}",
           f"- execution_time_ms p50 / p95 / p99: **{server_side.get('p50_ms')} / "
           f"{server_side.get('p95_ms')} / {server_side.get('p99_ms')} ms**",
           f"- non-200 responses: {int(server_side.get('non_200') or 0)}",
           "",
           "`execution_time_ms` excludes the network. Client wall time minus this is "
           "transport.", ""]
md += ["## Endpoint configuration, verbatim", "", "```json",
       json.dumps(ep_cfg, indent=2), "```", ""]
report = "\n".join(md)

vol_dir = f"/Volumes/{cfg.catalog}/{cfg.schema}/{cfg.volume}/benchmark"
os.makedirs(vol_dir, exist_ok=True)
out_path = f"{vol_dir}/serving_benchmark.md"
with open(out_path, "w") as f:
    f.write(report)
print("wrote", out_path)
print()
print(report)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Phase 6 · The online store on its own
# MAGIC
# MAGIC Direct keyed reads against Lakebase over Postgres, the way an application
# MAGIC would issue them. This is the floor under everything above: the endpoint does
# MAGIC these reads for you, so this says what the storage layer costs before any
# MAGIC serving, network or model time is added.
# MAGIC
# MAGIC **This cell is last on purpose.** A direct psycopg read aborted the serverless
# MAGIC kernel with `exit code 134 (SIGABRT)` during development — a native crash that
# MAGIC no `try/except` can contain. Everything above is already written to
# MAGIC `crfs_serving_benchmark` and to the volume by the time this runs, so if it
# MAGIC takes the kernel down it costs a diagnostic and nothing else. `psycopg` is
# MAGIC installed here rather than at the top of the notebook for the same reason.
# COMMAND ----------
# MAGIC %pip install "psycopg[binary]" --quiet
# COMMAND ----------
store_latency = {}
try:
    from src.crfs import online as ONL

    store_pg = ONL.from_config(w, cfg)
    sample = (spark.table(cfg.t("online_viewer_rail"))
              .select("viewer_id", "rail_id").limit(40).toPandas())
    keys = [{"viewer_id": a, "rail_id": b}
            for a, b in zip(sample["viewer_id"], sample["rail_id"])]
    for k in keys[:3]:                              # warm connection and plan
        store_pg.keyed_read_composite("online_viewer_rail", k)
    lat = sorted(store_pg.keyed_read_composite("online_viewer_rail", k)[2] for k in keys)
    n = len(lat)
    store_latency["online_viewer_rail_composite"] = {
        "n": n, "p50_ms": round(lat[n // 2], 2),
        "p95_ms": round(lat[min(n - 1, int(0.95 * n))], 2), "max_ms": round(lat[-1], 2)}
    rail_ids = spark.table(cfg.t("online_rail_features")) \
        .select("rail_id").toPandas()["rail_id"].tolist()
    store_latency["online_rail_features_single"] = store_pg.keyed_read_latency(
        "online_rail_features", "rail_id", rail_ids)
    store_pg.close()
    print(json.dumps(store_latency, indent=2))
    comp = store_latency["online_viewer_rail_composite"]["p50_ms"]
    single = store_latency["online_rail_features_single"].get("p50_ms", 0)
    serial = RAILS_PER_REQ * comp + single
    end_to_end = (ramp[0].latency or {}).get("p50_ms")
    lowest_conc = ramp[0].concurrency
    print(f"\nA {RAILS_PER_REQ}-rail request needs {RAILS_PER_REQ} composite reads plus "
          f"one rail read: {serial:.1f} ms if issued serially.")
    if end_to_end:
        # The comparison was inverted: `not ` was emitted when end_to_end EXCEEDED the
        # serial estimate, i.e. it printed the good news precisely when the measurement
        # was bad. Under the serial cost means the lookups are batched.
        paying = end_to_end >= serial
        print(f"End-to-end p50 at concurrency {lowest_conc} was {end_to_end} ms. The "
              f"endpoint is {'' if paying else 'not '}paying that serial cost, which is "
              f"the useful thing this comparison tells you.")
except Exception as e:
    store_latency = {"error": f"{type(e).__name__}: {e}"[:300]}
    print("direct online-store read unavailable:", store_latency["error"])
    print("The endpoint's own lookups are unaffected; only this attribution is missing.")
# COMMAND ----------
dbutils.notebook.exit(json.dumps({
    "where": WHERE,
    "endpoint": cfg.rail_ranker_endpoint,
    "endpoint_config": ep_cfg,
    "warmup": warm,
    "fanout": [r.as_dict() for r in fanout],
    "ramp": [r.as_dict() for r in ramp],
    "spike": [r.as_dict() for r in spike],
    "spike_detail": {k: v for k, v in spike_detail.items() if k != "per_second"},
    "features_only": [r.as_dict() for r in features_only],
    "sustained": [r.as_dict() for r in sustained],
    "sustained_detail": sustained_detail,
    "server_side": server_side,
    "online_store_direct": store_latency,
    "report_path": out_path,
    "table": BENCH_TABLE,
}, default=str))
