#!/usr/bin/env python3
"""Run the serving benchmark from outside the workspace.

Same code as notebook 25, different vantage point -- and the contrast is the
point. From a laptop, every measurement carries the round trip to the region,
which on the workspace this was built against is 200-250 ms. A p95 of 300 ms from
here and 40 ms from a job are the same endpoint; only one of them is a statement
about Databricks.

Use this to answer "what will our own client see", and the in-region job to answer
"what does the platform cost per request". Reporting only one of the two is how
latency conversations go wrong.

    python3 scripts/benchmark_local.py --profile <PROFILE>
    python3 scripts/benchmark_local.py --profile P --levels 1,4,16 --skip-spike

Requires the workspace SDK and requests:  pip install databricks-sdk requests
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.crfs import loadtest as LT           # noqa: E402
from src.crfs import rails as R               # noqa: E402


def read_vars(path=".crfs.vars"):
    """Pick up whatever scripts/bootstrap.sh discovered, so this script needs no
    flags in the common case."""
    out = {}
    if os.path.exists(path):
        for line in open(path):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                out[k] = v
    return out


def main():
    v = read_vars()
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", default=v.get("CRFS_PROFILE"),
                    help="~/.databrickscfg profile")
    ap.add_argument("--endpoint", default="crunchyroll-rail-ranker")
    ap.add_argument("--catalog", default=v.get("catalog"))
    ap.add_argument("--schema", default=v.get("schema", "crunchyroll_demo"))
    ap.add_argument("--viewer", default=None,
                    help="viewer id to score; default is whatever the endpoint accepts")
    ap.add_argument("--rails", type=int, default=12, help="candidate rails per request")
    ap.add_argument("--levels", default="1,2,4,8,16",
                    help="concurrency levels for the ramp")
    ap.add_argument("--fanout", default="1,4,8,16",
                    help="candidates-per-request sizes for the fanout phase")
    ap.add_argument("--seconds", type=float, default=10.0, help="seconds per ramp level")
    ap.add_argument("--skip-spike", action="store_true")
    ap.add_argument("--json-out", default=None, help="write the raw result JSON here")
    args = ap.parse_args()

    if not args.profile:
        ap.error("--profile is required (or run scripts/bootstrap.sh first)")

    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(profile=args.profile)

    cfg = LT.endpoint_config_summary(w, args.endpoint)
    print(json.dumps(cfg, indent=2))
    # `.endswith("READY")` was true for "NOT_READY", so this gate never tripped and the
    # benchmark would happily report a cold or updating endpoint's numbers as latency.
    # str() first: `ready` is None when the SDK returns no state, and None.endswith
    # raised AttributeError instead of this error message.
    if str(cfg["state"]["ready"]) != "READY":
        print(f"\nendpoint is not READY: {cfg['state']}", file=sys.stderr)
        return 1

    # Rails come from the catalog when we can reach it, and from the shipped
    # catalog definition when we cannot -- a laptop without SQL access should still
    # be able to run a load test.
    rail_ids = [r[0] for r in R.RAIL_SPECS]
    viewer = args.viewer
    if viewer is None:
        if args.catalog:
            try:
                wh = w.warehouses.list()
                wid = next((x.id for x in wh if x.state and "RUNNING" in str(x.state)), None) \
                    or next((x.id for x in wh), None)
                res = w.statement_execution.execute_statement(
                    warehouse_id=wid,
                    statement=(f"SELECT viewer_id FROM {args.catalog}.{args.schema}."
                               f"viewer_rail_features_ts GROUP BY viewer_id "
                               f"ORDER BY SUM(vr_impressions_30d) DESC LIMIT 1"),
                    wait_timeout="30s")
                viewer = res.result.data_array[0][0]
                print(f"\nbusiest viewer from the feature table: {viewer}")
            except Exception as e:
                print(f"\ncould not query for a viewer ({type(e).__name__}); "
                      f"falling back to v0001")
                viewer = "v0001"
        else:
            viewer = "v0001"

    def payload_for(n, viewer=viewer):
        ids = [rail_ids[i % len(rail_ids)] for i in range(n)]
        return R.rail_request_records(viewer, ids, device="tv", locale="en-US",
                                      hour_of_day=21, day_of_week=5)

    client = LT.EndpointClient(w, args.endpoint, timeout_s=30.0)
    print(f"\nurl: {client.url}\nroute_optimized: {client.route_optimized}")

    payload = payload_for(args.rails)
    warm = LT.warmup(client, payload, n=6)
    print("warmup:", warm)
    if warm["statuses"] != [200]:
        print("\nendpoint is not answering cleanly; nothing below would be meaningful.",
              file=sys.stderr)
        print(json.dumps(warm, indent=2), file=sys.stderr)
        return 1
    if warm["first_ms"] > 3 * max(warm["last_ms"], 1):
        # Attribute the gap to what actually caused it. This endpoint is configured
        # scale_to_zero=false, so the first request cannot be a container cold start --
        # calling it one would overstate the platform's cold-start cost by ~1 second.
        # What it is: TLS handshake, the SDK's first OAuth token fetch, and the first
        # connection to the endpoint, all of them client-side and one-time.
        s2z = any(e.get("scale_to_zero") for e in (cfg.get("served_entities") or []))
        cause = ("a container cold start, which is what scale_to_zero trades for idle cost"
                 if s2z else
                 "TLS handshake plus the first OAuth token fetch on this client, not a "
                 "container cold start -- this endpoint has scale_to_zero=false")
        print(f"\nnote: first request {warm['first_ms']} ms vs {warm['last_ms']} ms warm. "
              f"That gap is {cause}.")

    print("\n--- fanout: latency vs candidate rails per request ---")
    fanout = LT.fanout_phase(client, payload_for,
                             sizes=tuple(int(x) for x in args.fanout.split(",")),
                             requests_per_size=30)
    print(LT.markdown_table(fanout))

    print("\n--- ramp: percentiles and throughput at rising concurrency ---")
    ramp = LT.ramp_phase(client, payload,
                         levels=tuple(int(x) for x in args.levels.split(",")),
                         duration_s=args.seconds, settle_s=2.0)
    print(LT.markdown_table(ramp))

    spike, spike_detail = [], {}
    if not args.skip_spike:
        print("\n--- spike: step change in offered load ---")
        spike, spike_detail = LT.spike_phase(client, payload, baseline=2, spike=24,
                                             baseline_s=8.0, spike_s=18.0, recover_s=14.0)
        print(LT.markdown_table(spike))
        print("first second p95:", spike_detail.get("first_second_p95_ms"), "ms")
        print("seconds to within 1.5x baseline p95:",
              spike_detail.get("seconds_to_within_1_5x_baseline_p95"))

    print("\n--- where you measured from ---")
    print("This client is outside the workspace region, so every number above "
          "includes your round trip.")
    # Report the fanout size the minimum actually came from. Hardcoding "1-rail"
    # was wrong: the smallest p50 in the last run was at 16 rails, because at this
    # distance transport dominates and the per-rail lookup cost is inside the noise.
    cheapest = min((r for r in fanout if r.latency),
                   key=lambda r: r.latency.get("p50_ms", 1e9), default=None)
    if cheapest is not None:
        print(f"The smallest p50 seen was {cheapest.latency['p50_ms']} ms at "
              f"{cheapest.rows_per_request} rails per request; treat most of that as "
              f"transport and compare against the in-region job (make bench) rather "
              f"than against a target SLA.")

    out = {
        "where": "laptop",
        "measured_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "endpoint": args.endpoint,
        "endpoint_config": cfg,
        "viewer": viewer,
        "warmup": warm,
        "fanout": [r.as_dict() for r in fanout],
        "ramp": [r.as_dict() for r in ramp],
        "spike": [r.as_dict() for r in spike],
        "spike_detail": {k: val for k, val in spike_detail.items() if k != "per_second"},
    }
    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(out, f, indent=2, default=str)
        print(f"\nwrote {args.json_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
