#!/usr/bin/env python3
"""
Measure keyed-read latency from the online feature store (Lakebase Postgres).

This script runs on a laptop (not Databricks) to measure latency as seen from
a real application — network RTT from client to Lakebase endpoint included.

**Important note on pooled hosts:** The Lakebase read_write_pooled_host rejects
OAuth credentials with "SASL authentication failed" (as of 2026-09-07). This
script uses the direct endpoint host (`status.hosts.host`), which accepts OAuth
tokens correctly.

Usage:
    python scripts/measure_online_latency.py \\
        --profile fe-vm-lakebase-praneeth \\
        --catalog serverless_lakebase_praneeth_catalog \\
        --schema crunchyroll_demo \\
        --table online_session_features \\
        --key viewer_id \\
        --n 50

Output:
    p50/p95 latencies in milliseconds, measured from this machine (network included).

Optional:
    --watch        Continuous polling mode (run until interrupted)
    --poll-interval-ms    Sleep between polls (default 1000)
"""

import sys
import argparse
import time
from datetime import datetime

sys.path.insert(0, ".")  # Add current dir to path for src.crfs.online

from databricks.sdk import WorkspaceClient
from src.crfs.online import OnlineStore


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, help="Databricks CLI profile")
    parser.add_argument("--catalog", required=True, help="UC catalog")
    parser.add_argument("--schema", required=True, help="UC schema")
    parser.add_argument("--table", required=True, help="Online table name")
    parser.add_argument("--key", required=True, help="Key column (e.g., viewer_id)")
    parser.add_argument("--n", type=int, default=50, help="Number of keys to sample (default 50)")
    parser.add_argument("--watch", action="store_true", help="Continuous polling mode")
    parser.add_argument("--poll-interval-ms", type=int, default=1000, help="Poll interval in ms (default 1000)")

    args = parser.parse_args()

    # Initialize WorkspaceClient with the specified profile
    w = WorkspaceClient(profile=args.profile)

    # Get the Lakebase endpoint path from the workspace config
    # This is derived from the catalog and schema (adjust as needed for your workspace)
    try:
        endpoint_path = f"projects/crunchyroll-online-store/branches/production/endpoints/primary"
        print(f"[*] Endpoint path: {endpoint_path}")
        print(f"[*] Using direct host (pooled host has auth issues)")
    except Exception as e:
        print(f"Error getting endpoint: {e}", file=sys.stderr)
        sys.exit(1)

    # Create OnlineStore connection
    try:
        store = OnlineStore(w, endpoint_path, pg_database=args.catalog, pg_schema=args.schema)
        print(f"[+] Connected to {args.table}")
    except Exception as e:
        print(f"Error connecting: {e}", file=sys.stderr)
        sys.exit(1)

    # Fetch sample keys
    try:
        query_result = w.sql.execute(
            f"SELECT DISTINCT {args.key} FROM {args.catalog}.{args.schema}.{args.table} LIMIT {args.n}"
        ).result()
        keys = [row[0] for row in query_result.rows]
        print(f"[+] Fetched {len(keys)} sample keys")
    except Exception as e:
        print(f"Error fetching keys: {e}", file=sys.stderr)
        sys.exit(1)

    if args.watch:
        # Continuous polling mode
        print(f"[*] Watch mode: Ctrl+C to stop\n")
        try:
            while True:
                lat = store.keyed_read_latency(args.table, args.key, keys, warmup=0)
                ts = datetime.now().strftime("%H:%M:%S")
                print(f"[{ts}] p50={lat.get('p50_ms', 'N/A')}ms p95={lat.get('p95_ms', 'N/A')}ms " +
                      f"min={lat.get('min_ms', 'N/A')}ms max={lat.get('max_ms', 'N/A')}ms n={lat.get('n', 0)}")
                time.sleep(args.poll_interval_ms / 1000.0)
        except KeyboardInterrupt:
            print(f"\n[*] Stopped")
    else:
        # One-shot measurement
        print(f"[*] Measuring latency over {len(keys)} keys...")
        lat = store.keyed_read_latency(args.table, args.key, keys, warmup=2)
        print(f"\n=== LATENCY FROM LAPTOP (NETWORK INCLUDED) ===")
        print(f"p50: {lat.get('p50_ms', 'N/A')}ms")
        print(f"p95: {lat.get('p95_ms', 'N/A')}ms")
        print(f"min: {lat.get('min_ms', 'N/A')}ms")
        print(f"max: {lat.get('max_ms', 'N/A')}ms")
        print(f"n:   {lat.get('n', 0)}")


if __name__ == "__main__":
    main()
