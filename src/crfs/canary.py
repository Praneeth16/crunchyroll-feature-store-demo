"""Canary a model version behind a live endpoint, judge it, and promote or roll back.

docs/open_items.md §3: notebook 31 demonstrated the traffic SPLIT but not the process
around it -- no metric, no gate, no rollback. This module is that process, shared by
notebook 31 (the split demo) and notebook 33 (the gate), so the two cannot drift on the
parts that have already bitten this repo once:

  * SDK enums are not JSON serialisable and the config goes over REST (94eb638)
  * the sizing mode the endpoint REALISED must be copied, not assumed (de3e95c)
  * restore runs in a `finally`, or a timeout leaks 10% of homepage traffic (de3e95c)

The gate judges each served entity by calling it DIRECTLY
(/served-models/<name>/invocations) on the same requests. Routed traffic tells you
nothing about which version answered -- the response carries no version -- and the
inference table that does record it lands minutes later. Direct calls give paired
measurements now; the inference table is read afterwards as attribution evidence.
"""
import json
import time


def plain(v):
    return getattr(v, "value", v)


def sized(live, name: str, version, entity_name: str = None) -> dict:
    """A served-entity spec that copies whichever sizing mode `live` realised.

    `workload_size` and the provisioned-concurrency pair are mutually exclusive, and
    notebook 23 falls back from the pair to workload_size when a workspace rejects it.
    Sending nulls for one while omitting the other preserves neither.
    """
    out = {"name": name, "entity_name": entity_name or live.entity_name,
           "entity_version": str(version), "scale_to_zero_enabled": False,
           "workload_type": plain(live.workload_type) or "CPU"}
    if live.min_provisioned_concurrency is not None:
        out["min_provisioned_concurrency"] = live.min_provisioned_concurrency
        out["max_provisioned_concurrency"] = live.max_provisioned_concurrency
    else:
        out["workload_size"] = plain(live.workload_size)
    return out


def split_body(base: dict, cand: dict, pct: int) -> dict:
    return {"served_entities": [base, cand],
            "traffic_config": {"routes": [
                {"served_entity_name": base["name"], "traffic_percentage": 100 - pct},
                {"served_entity_name": cand["name"], "traffic_percentage": pct}]}}


def single_body(entity: dict) -> dict:
    return {"served_entities": [entity],
            "traffic_config": {"routes": [
                {"served_entity_name": entity["name"], "traffic_percentage": 100}]}}


def put_config(w, endpoint: str, body: dict):
    w.api_client.do("PUT", f"/api/2.0/serving-endpoints/{endpoint}/config",
                    body=json.loads(json.dumps(body, default=plain)))


def wait_config(w, endpoint: str, timeout_s: int = 2400, poll_s: int = 20):
    """Wait on the endpoint's own config-update state, printing each change so a slow
    update is distinguishable from a stuck one."""
    deadline, last = time.time() + timeout_s, None
    while time.time() < deadline:
        e = w.serving_endpoints.get(name=endpoint)
        cur = (str(plain(getattr(e.state, "config_update", None))),
               str(plain(getattr(e.state, "ready", None))))
        if cur != last:
            print(f"  config_update={cur[0]} ready={cur[1]}")
            last = cur
        if cur[0] == "NOT_UPDATING":
            if cur[1] != "READY":
                raise RuntimeError(f"{endpoint} settled NOT READY: {cur}")
            return e
        time.sleep(poll_s)
    raise TimeoutError(f"{endpoint} config update did not settle in {timeout_s}s")


def routes(e) -> list:
    tc = e.config.traffic_config if e.config else None
    return [(r.served_entity_name, r.traffic_percentage) for r in ((tc.routes if tc else None) or [])]


def invoke_served(w, endpoint: str, served: str, records: list):
    """Score `records` on one served entity. Returns (predictions, ms, status)."""
    t0 = time.perf_counter()
    try:
        resp = w.api_client.do(
            "POST", f"/serving-endpoints/{endpoint}/served-models/{served}/invocations",
            body={"dataframe_records": records})
        return resp.get("predictions") or [], (time.perf_counter() - t0) * 1000, "ok"
    except Exception as ex:
        return [], (time.perf_counter() - t0) * 1000, f"{type(ex).__name__}: {str(ex)[:120]}"


def spearman(a: dict, b: dict) -> float:
    """Rank correlation of two {rail_id: rank} orders over their common rails."""
    keys = [k for k in a if k in b]
    n = len(keys)
    if n < 2:
        return float("nan")
    d2 = sum((a[k] - b[k]) ** 2 for k in keys)
    return 1 - 6 * d2 / (n * (n * n - 1))


def top_k_overlap(a: dict, b: dict, k: int = 3) -> float:
    ta = {r for r, v in a.items() if v <= k}
    tb = {r for r, v in b.items() if v <= k}
    return len(ta & tb) / float(k)


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else float("nan")


def summarise(samples: list) -> dict:
    """samples: [{"ms": float, "status": str, "ranks": {rail: rank}}] for one entity."""
    ok = [s for s in samples if s["status"] == "ok"]
    # The first failure's text is kept: the 2026-09-23 run rolled back a challenger that
    # failed 40 of 40 requests and recorded only the count, so the cause had to be
    # rediscovered rather than read off the decision record.
    first_error = next((s["status"] for s in samples if s["status"] != "ok"), None)
    return {"requests": len(samples), "errors": len(samples) - len(ok), "first_error": first_error,
            "error_rate": (len(samples) - len(ok)) / max(len(samples), 1),
            "p50_ms": round(pct([s["ms"] for s in ok], 0.50), 1),
            "p95_ms": round(pct([s["ms"] for s in ok], 0.95), 1)}


def gate(champ: dict, cand: dict, agreement: dict, thresholds: dict):
    """PROMOTE only if every check passes. Returns (decision, [(check, passed, detail)]).

    The agreement check is a GUARDRAIL, not a quality metric: a challenger that
    reorders the homepage completely (Spearman near 0) against the champion is a
    change a human should look at before it takes traffic, whatever its offline NDCG.
    Online quality (clicks by served entity) needs impressions and is out of scope for
    a gate that runs in minutes.
    """
    checks = [
        ("error_rate", cand["error_rate"] <= thresholds["max_error_rate"],
         f"{cand['error_rate']:.3f} <= {thresholds['max_error_rate']}"),
        ("p95_latency", cand["p95_ms"] <= champ["p95_ms"] * thresholds["max_p95_ratio"],
         f"{cand['p95_ms']} ms <= {champ['p95_ms']} x {thresholds['max_p95_ratio']}"),
        ("rank_agreement", agreement["spearman_mean"] >= thresholds["min_spearman"],
         f"spearman {agreement['spearman_mean']:.3f} >= {thresholds['min_spearman']}"),
    ]
    return ("PROMOTE" if all(c[1] for c in checks) else "ROLLBACK"), checks
