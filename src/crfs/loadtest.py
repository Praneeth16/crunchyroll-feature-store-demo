"""Load and latency measurement for a Model Serving endpoint.

Crunchyroll's question is not "does it work" -- it is "does it hold up in the
homepage request path". That is four separate measurements, and this module makes
each of them a named phase so nobody has to guess which number answers which
question:

  fanout      how latency grows with the number of candidate rails in one
              request. This is the cost of automatic feature lookup: 16 rails is
              16 composite-key reads against the online store, inside one call.
  ramp        latency percentiles and achieved throughput at rising concurrency,
              with error codes counted rather than swallowed.
  spike       a step change from low to high concurrency, to see what autoscaling
              actually does: how long recovery takes and whether anything is
              rejected on the way.
  coldstart   first-request latency from a scaled-to-zero endpoint, measured once
              and only if the endpoint is configured that way.

Two things this module deliberately does:

*   It reports **client-observed wall time**, which includes the network. Run it
    from a laptop and you measure your coffee shop; run it from a job in the same
    region and you measure the platform. `where` in the output records which, and
    the benchmark runs both.
*   It never reports a latency number without the concurrency it was measured
    at. A p99 with no offered load beside it is not a number, it is a mood.

Server-side time comes from the endpoint's inference table (`execution_time_ms`),
pulled separately by `inference_table_latency` -- that is how the network share
of the wall time gets attributed rather than argued about.

Threads, not asyncio: at the concurrencies a homepage ranker needs (tens, not
thousands) an HTTP request is pure I/O wait, and a thread pool keeps the timing
code readable enough to trust.
"""
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, asdict

DEFAULT_TIMEOUT_S = 30.0


# ---------------------------------------------------------------------- results
@dataclass
class Sample:
    started: float
    elapsed_ms: float
    status: int
    n_rows: int
    error: str = ""


@dataclass
class PhaseResult:
    phase: str
    concurrency: int
    rows_per_request: int
    requests: int
    duration_s: float
    ok: int
    errors: dict = field(default_factory=dict)
    latency: dict = field(default_factory=dict)
    rps: float = 0.0
    note: str = ""

    def as_dict(self):
        return asdict(self)


def percentiles(values):
    """p50/p90/p95/p99 plus the shape around them.

    Nearest-rank, not interpolated: with 200 samples an interpolated p99 is a
    number no single request ever experienced, and the whole point of a tail
    percentile is that some real request felt it.
    """
    if not values:
        return {}
    v = sorted(values)
    n = len(v)
    pick = lambda q: v[min(n - 1, max(0, int(round(q * n)) - 1))]
    return {
        "n": n,
        "min_ms": round(v[0], 1),
        "p50_ms": round(pick(0.50), 1),
        "p90_ms": round(pick(0.90), 1),
        "p95_ms": round(pick(0.95), 1),
        "p99_ms": round(pick(0.99), 1),
        "max_ms": round(v[-1], 1),
        "mean_ms": round(statistics.fmean(v), 1),
        "stdev_ms": round(statistics.pstdev(v), 1) if n > 1 else 0.0,
    }


# ------------------------------------------------------------------- the caller
class EndpointClient:
    """One HTTP session per thread, authenticated the way the SDK is.

    A route-optimized endpoint is reached at a different host and requires an
    OAuth token rather than the workspace URL's own auth, so the URL and the
    headers are resolved together, once, from the SDK config -- not assembled by
    hand at each call site.
    """

    def __init__(self, w, endpoint: str, timeout_s: float = DEFAULT_TIMEOUT_S,
                 route_optimized: bool = None):
        import threading
        self.w = w
        self.endpoint = endpoint
        self.timeout_s = timeout_s
        self._local = threading.local()

        ep = w.serving_endpoints.get(endpoint)
        self.route_optimized = bool(getattr(ep, "route_optimized", False)) \
            if route_optimized is None else route_optimized

        # A route-optimized endpoint is reachable on a dedicated, lower-overhead
        # path, and the endpoint object is where that path lives. Verified on this
        # workspace: a NON-route-optimized endpoint returns endpoint_url=None and
        # data_plane_info=None, so both have to be treated as optional and the
        # workspace path is the fallback. data_plane_info.query_info.endpoint_url is
        # the more specific of the two and is preferred when present.
        url = None
        dpi = getattr(ep, "data_plane_info", None)
        query_info = getattr(dpi, "query_info", None) if dpi else None
        if query_info is not None:
            url = getattr(query_info, "endpoint_url", None)
        if not url:
            url = getattr(ep, "endpoint_url", None)
        host = w.config.host.rstrip("/")
        self.url = url or f"{host}/serving-endpoints/{endpoint}/invocations"
        # Say which path is in use rather than leaving it to be inferred: the whole
        # reason route optimization is enabled is that it changes this URL, and a
        # benchmark that silently fell back to the workspace path would be measuring
        # something other than what it claims.
        self.using_direct_path = bool(url)
        if self.route_optimized and not self.using_direct_path:
            print(f"NOTE: {endpoint} reports route_optimized=True but exposes no direct "
                  f"endpoint_url; falling back to the workspace path "
                  f"{self.url}. Latency below is not measuring the optimized route.")

    def _session(self):
        s = getattr(self._local, "session", None)
        if s is None:
            import requests
            from requests.adapters import HTTPAdapter
            s = requests.Session()
            # No urllib3 retry: a silent retry inside the client would fold a
            # 429 or a 503 into the latency of the request that followed it, and
            # those are exactly the events this benchmark exists to count.
            s.mount("https://", HTTPAdapter(pool_connections=4, pool_maxsize=4, max_retries=0))
            self._local.session = s
        return s

    def headers(self):
        h = {"Content-Type": "application/json"}
        h.update(self.w.config.authenticate() or {})
        return h

    def post(self, records) -> Sample:
        body = json.dumps({"dataframe_records": records})
        started = time.time()
        t0 = time.perf_counter()
        try:
            r = self._session().post(self.url, data=body, headers=self.headers(),
                                     timeout=self.timeout_s)
            elapsed = (time.perf_counter() - t0) * 1000.0
            err = "" if r.status_code == 200 else r.text[:200]
            return Sample(started, elapsed, r.status_code, len(records), err)
        except Exception as e:
            elapsed = (time.perf_counter() - t0) * 1000.0
            return Sample(started, elapsed, 0, len(records), f"{type(e).__name__}: {e}"[:200])


# --------------------------------------------------------------------- phases
def _run_phase(client, payloads, concurrency: int, duration_s: float = None,
               requests_total: int = None, phase: str = "ramp", note: str = ""):
    """Drive `concurrency` workers against the endpoint, either for a fixed
    duration or for a fixed number of requests.

    Payloads are cycled rather than randomised per call so two phases at
    different concurrency are comparing the same work, not different work.
    """
    samples = []
    stop_at = (time.perf_counter() + duration_s) if duration_s else None
    counter = {"i": 0}
    import threading
    lock = threading.Lock()

    def next_payload():
        with lock:
            i = counter["i"]
            counter["i"] += 1
        return payloads[i % len(payloads)], i

    def worker():
        local = []
        while True:
            if stop_at is not None and time.perf_counter() >= stop_at:
                break
            if requests_total is not None:
                with lock:
                    if counter["i"] >= requests_total:
                        break
            payload, _ = next_payload()
            local.append(client.post(payload))
        return local

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for chunk in pool.map(lambda _: worker(), range(concurrency)):
            samples.extend(chunk)
    wall = time.perf_counter() - t0

    ok = [s for s in samples if s.status == 200]
    errors = {}
    for s in samples:
        if s.status != 200:
            key = f"http_{s.status}" if s.status else "transport"
            errors[key] = errors.get(key, 0) + 1

    rows = payloads[0] if payloads else []
    return PhaseResult(
        phase=phase,
        concurrency=concurrency,
        rows_per_request=len(rows),
        requests=len(samples),
        duration_s=round(wall, 2),
        ok=len(ok),
        errors=errors,
        latency=percentiles([s.elapsed_ms for s in ok]),
        rps=round(len(ok) / wall, 1) if wall > 0 else 0.0,
        note=note,
    ), samples


def warmup(client, payload, n: int = 8):
    """Absorb TLS handshake, JIT and first-plan cost so they do not land in the
    reported percentiles. Reported separately, because on a scale-to-zero
    endpoint the first of these *is* the cold start."""
    lat = [client.post(payload) for _ in range(n)]
    return {"n": n,
            "first_ms": round(lat[0].elapsed_ms, 1),
            "last_ms": round(lat[-1].elapsed_ms, 1),
            "statuses": sorted({s.status for s in lat})}


def fanout_phase(client, payload_for, sizes=(1, 4, 8, 16, 32), requests_per_size: int = 40):
    """Latency against candidates-per-request, at concurrency 1.

    Concurrency is pinned to 1 deliberately: this phase is asking what one
    request costs as the number of online feature lookups inside it grows, and
    queueing would contaminate that.
    """
    out = []
    for k in sizes:
        payload = payload_for(k)
        client.post(payload)                       # warm this shape
        res, _ = _run_phase(client, [payload], concurrency=1,
                            requests_total=requests_per_size, phase="fanout",
                            note=f"{k} candidate rails per request")
        per_row = (res.latency.get("p50_ms", 0) / k) if k else 0
        res.note += f" | p50 per candidate {per_row:.1f} ms"
        out.append(res)
    return out


def ramp_phase(client, payload, levels=(1, 2, 4, 8, 16, 32, 64), duration_s: float = 12.0,
               settle_s: float = 3.0):
    """Percentiles and achieved throughput at rising concurrency.

    A settle gap between levels lets the endpoint's own autoscaling react, so
    each level measures a steady state rather than the transient left over from
    the level before it.
    """
    out = []
    for c in levels:
        res, _ = _run_phase(client, [payload], concurrency=c, duration_s=duration_s,
                            phase="ramp", note=f"steady state at concurrency {c}")
        out.append(res)
        time.sleep(settle_s)
    return out


def spike_phase(client, payload, baseline: int = 2, spike: int = 48,
                baseline_s: float = 10.0, spike_s: float = 25.0, recover_s: float = 20.0):
    """Step from baseline to spike concurrency and back, then look at the shape.

    The interesting numbers are not the averages. They are: what did the first
    second of the spike cost, how long until latency came back down, and did
    anything get rejected while capacity was still arriving.
    """
    before, _ = _run_phase(client, [payload], concurrency=baseline, duration_s=baseline_s,
                           phase="spike_baseline", note=f"baseline concurrency {baseline}")
    during, samples = _run_phase(client, [payload], concurrency=spike, duration_s=spike_s,
                                 phase="spike_load", note=f"stepped to concurrency {spike}")
    after, _ = _run_phase(client, [payload], concurrency=baseline, duration_s=recover_s,
                          phase="spike_recovery", note=f"back to concurrency {baseline}")

    ok = sorted([s for s in samples if s.status == 200], key=lambda s: s.started)
    detail = {}
    if ok:
        t0 = ok[0].started
        buckets = {}
        for s in ok:
            b = int(s.started - t0)
            buckets.setdefault(b, []).append(s.elapsed_ms)
        by_second = {str(k): {"requests": len(v), "p95_ms": percentiles(v).get("p95_ms")}
                     for k, v in sorted(buckets.items())}
        settled = before.latency.get("p95_ms")
        recovered_at = None
        if settled:
            for sec in sorted(buckets):
                if percentiles(buckets[sec]).get("p95_ms", 1e9) <= settled * 1.5:
                    recovered_at = sec
                    break
        detail = {"per_second": by_second,
                  "first_second_p95_ms": percentiles(buckets.get(0, [])).get("p95_ms"),
                  "seconds_to_within_1_5x_baseline_p95": recovered_at}
    during.note += " | " + json.dumps({k: v for k, v in detail.items() if k != "per_second"})
    return [before, during, after], detail


def coldstart_probe(client, payload):
    """One request into a cold endpoint. Only meaningful once, and only when the
    endpoint is allowed to scale to zero -- otherwise it is just a warm request
    with a misleading name."""
    s = client.post(payload)
    return {"elapsed_ms": round(s.elapsed_ms, 1), "status": s.status, "error": s.error}


# ------------------------------------------------- server-side time attribution
# Column names verified against the deployed AI Gateway inference table
# (`cr_rail_inference_payload`): the timestamp is `request_time TIMESTAMP` and the
# server-side duration is `execution_duration_ms LONG`. The first version of this query
# used `timestamp_ms` and `execution_time_ms`, neither of which exists, so every run
# stored `{"error": "UNRESOLVED_COLUMN ... timestamp_ms"}` instead of a measurement --
# and the server-side attribution these documents promised was never actually made.
INFERENCE_LATENCY_SQL = """
SELECT COUNT(*)                                                        AS requests,
       ROUND(PERCENTILE(execution_duration_ms, 0.50), 1)               AS p50_ms,
       ROUND(PERCENTILE(execution_duration_ms, 0.95), 1)               AS p95_ms,
       ROUND(PERCENTILE(execution_duration_ms, 0.99), 1)               AS p99_ms,
       MAX(execution_duration_ms)                                      AS max_ms,
       COUNT_IF(status_code <> 200)                                    AS non_200
FROM {table}
WHERE request_time >= TIMESTAMP_MILLIS({since_ms})
"""


def inference_table_latency(spark, table: str, since_ms: int):
    """Server-side `execution_time_ms` for the requests this run just made.

    This is the endpoint's own measurement of model execution -- it excludes the
    network between client and endpoint. Subtracting it from the client wall time
    is how the benchmark says what is platform and what is distance, instead of
    reporting one number and letting the room assume.
    """
    try:
        row = spark.sql(INFERENCE_LATENCY_SQL.format(table=table, since_ms=int(since_ms))).first()
        return {k: (float(v) if v is not None else None) for k, v in row.asDict().items()}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"[:300]}


# --------------------------------------------------------------------- reporting
def markdown_table(results):
    """Percentiles as a table, because this ends up in a document a human reads."""
    head = ("| phase | conc | rows/req | requests | ok | errors | p50 ms | p95 ms | p99 ms "
            "| max ms | req/s |")
    sep = "|---" * 11 + "|"
    lines = [head, sep]
    for r in results:
        lat = r.latency or {}
        err = ", ".join(f"{k}×{v}" for k, v in sorted(r.errors.items())) or "-"
        lines.append(
            f"| {r.phase} | {r.concurrency} | {r.rows_per_request} | {r.requests} | {r.ok} "
            f"| {err} | {lat.get('p50_ms','-')} | {lat.get('p95_ms','-')} "
            f"| {lat.get('p99_ms','-')} | {lat.get('max_ms','-')} | {r.rps} |")
    return "\n".join(lines)


def _plain(v):
    """Coerce an SDK value to something json.dumps will accept.

    The serving SDK returns enums (ServingModelWorkloadType, EndpointStateReady, ...).
    `json.dumps` raises `TypeError: Object of type ServingModelWorkloadType is not JSON
    serializable` on them, which failed notebook 23 *after* it had already created the
    endpoint -- a display line taking down a deployment. Everything this function
    returns is a primitive.
    """
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    return getattr(v, "value", None) or str(v)


def endpoint_config_summary(w, endpoint: str) -> dict:
    """The configuration the numbers were measured against, as JSON-safe primitives.

    A latency table without this is unfalsifiable -- scale-to-zero, provisioned
    concurrency and route optimization each move the answer by more than any
    model change would.
    """
    ep = w.serving_endpoints.get(endpoint)
    cfg = ep.config or getattr(ep, "pending_config", None)
    entities = []
    for e in (getattr(cfg, "served_entities", None) or []):
        entities.append({k: _plain(v) for k, v in {
            "name": e.name,
            "entity": e.entity_name,
            "version": e.entity_version,
            "workload_size": getattr(e, "workload_size", None),
            "workload_type": getattr(e, "workload_type", None),
            "scale_to_zero": getattr(e, "scale_to_zero_enabled", None),
            "min_provisioned_concurrency": getattr(e, "min_provisioned_concurrency", None),
            "max_provisioned_concurrency": getattr(e, "max_provisioned_concurrency", None),
            "burst_scaling_enabled": getattr(e, "burst_scaling_enabled", None),
        }.items()})
    gw = getattr(ep, "ai_gateway", None)
    inf = getattr(gw, "inference_table_config", None) if gw else None
    return {
        "endpoint": endpoint,
        "state": {"ready": _plain(getattr(ep.state, "ready", None)),
                  "config_update": _plain(getattr(ep.state, "config_update", None))},
        "route_optimized": bool(getattr(ep, "route_optimized", False)),
        "served_entities": entities,
        "inference_table": ({"catalog": _plain(inf.catalog_name),
                             "schema": _plain(inf.schema_name),
                             "prefix": _plain(inf.table_name_prefix),
                             "enabled": _plain(inf.enabled)}
                            if inf else None),
    }


def sustained_phase(client, payload, concurrency: int = 32, total_s: float = 600.0,
                    window_s: float = 30.0):
    """Hold one concurrency level for minutes and report throughput per window.

    This phase exists because the most consequential finding in this project was
    discovered by accident. Two ramp sweeps happened to run ten minutes apart and the
    second delivered 2.6x the throughput of the first -- same endpoint, same config,
    same payload (verification_log V50). That is autoscaling, and nothing in the
    benchmark was actually measuring it: `ramp_phase` gives each level 12 seconds,
    which is long enough to measure a steady state at current capacity and far too
    short to observe capacity arriving.

    So this holds load steady and slices the result by wall-clock window. The output is
    the shape Crunchyroll needs for capacity planning and that a percentile table
    cannot express: how long after load arrives does throughput stop climbing.

    Reported per window rather than aggregated, because the aggregate over a scale-up
    period is a number that describes no moment of the run.
    """
    samples = []
    t0 = time.perf_counter()
    stop_at = t0 + total_s
    import threading
    lock = threading.Lock()

    def worker():
        local = []
        while time.perf_counter() < stop_at:
            local.append(client.post(payload))
        return local

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for chunk in pool.map(lambda _: worker(), range(concurrency)):
            samples.extend(chunk)
    wall = time.perf_counter() - t0

    # Bucket by window using the wall-clock start each sample recorded.
    if not samples:
        return [], {}
    base = min(s.started for s in samples)
    buckets = {}
    for s in samples:
        w = int((s.started - base) // window_s)
        buckets.setdefault(w, []).append(s)

    windows = []
    for w in sorted(buckets):
        rows = buckets[w]
        ok = [r for r in rows if r.status == 200]
        errs = {}
        for r in rows:
            if r.status != 200:
                key = f"http_{r.status}" if r.status else "transport"
                errs[key] = errs.get(key, 0) + 1
        windows.append(PhaseResult(
            phase="sustained",
            concurrency=concurrency,
            rows_per_request=len(payload),
            requests=len(rows),
            duration_s=round(window_s, 1),
            ok=len(ok),
            errors=errs,
            latency=percentiles([r.elapsed_ms for r in ok]),
            rps=round(len(ok) / window_s, 1),
            note=f"t+{int(w * window_s)}-{int((w + 1) * window_s)}s at concurrency {concurrency}",
        ))

    # Time to plateau: the first window whose throughput is within 10% of the best
    # window seen. That is the number to quote for "how long until capacity arrives".
    best = max((w.rps for w in windows), default=0.0)
    plateau_at = None
    for i, w in enumerate(windows):
        if best and w.rps >= 0.9 * best:
            plateau_at = int(i * window_s)
            break
    first = windows[0].rps if windows else 0.0
    detail = {
        "concurrency": concurrency,
        "total_s": round(wall, 1),
        "window_s": window_s,
        "first_window_rps": first,
        "best_window_rps": best,
        "scale_up_factor": round(best / first, 2) if first else None,
        "seconds_to_90pct_of_best": plateau_at,
        "windows": [{"t_start_s": int(i * window_s), "rps": w.rps,
                     "p50_ms": w.latency.get("p50_ms"), "p95_ms": w.latency.get("p95_ms"),
                     "errors": w.errors}
                    for i, w in enumerate(windows)],
    }
    return windows, detail
