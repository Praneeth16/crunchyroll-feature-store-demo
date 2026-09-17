# Rail-ranking endpoint — measured serving characteristics

Measured 2026-09-17 09:54 UTC from **in_region_job** (client in the same region as the endpoint).

Configuration: endpoint `crunchyroll-rail-ranker` · route_optimized=False · scale_to_zero=False · provisioned_concurrency=4-32

Every number below is client-observed wall time unless it says otherwise, and every latency is reported with the concurrency it was measured at.

## Fanout — latency vs candidate rails per request

Each additional rail is one more composite-key read against the Lakebase online store, inside the same request.

| phase | conc | rows/req | requests | ok | errors | p50 ms | p95 ms | p99 ms | max ms | req/s |
|---|---|---|---|---|---|---|---|---|---|---|
| fanout | 1 | 1 | 40 | 40 | - | 79.4 | 129.5 | 137.1 | 137.1 | 12.6 |
| fanout | 1 | 4 | 40 | 40 | - | 52.2 | 95.2 | 104.4 | 104.4 | 16.9 |
| fanout | 1 | 8 | 40 | 40 | - | 54.8 | 97.2 | 177.9 | 177.9 | 15.8 |
| fanout | 1 | 16 | 40 | 40 | - | 52.7 | 62.9 | 96.5 | 96.5 | 18.3 |
| fanout | 1 | 32 | 40 | 40 | - | 55.8 | 76.8 | 84.5 | 84.5 | 17.2 |

## Concurrency ramp

| phase | conc | rows/req | requests | ok | errors | p50 ms | p95 ms | p99 ms | max ms | req/s |
|---|---|---|---|---|---|---|---|---|---|---|
| ramp | 1 | 12 | 217 | 217 | - | 52.6 | 66.9 | 74.1 | 284.4 | 18.0 |
| ramp | 2 | 12 | 413 | 413 | - | 54.3 | 71.5 | 105.0 | 330.2 | 34.3 |
| ramp | 4 | 12 | 709 | 709 | - | 63.9 | 96.4 | 121.2 | 275.0 | 58.8 |
| ramp | 8 | 12 | 900 | 900 | - | 101.0 | 171.3 | 209.1 | 294.3 | 74.4 |
| ramp | 16 | 12 | 983 | 983 | - | 182.5 | 359.4 | 468.5 | 562.2 | 80.0 |
| ramp | 32 | 12 | 976 | 976 | - | 369.0 | 685.3 | 773.8 | 828.2 | 77.5 |
| ramp | 64 | 12 | 3118 | 1694 | http_429×1424 | 382.2 | 830.7 | 949.4 | 1187.9 | 134.5 |

## Sustained load — how long capacity takes to arrive

The ramp above gives each level 12 seconds, which measures a steady state at current capacity and cannot see capacity being added. This phase holds one level and slices by wall-clock window.

| phase | conc | rows/req | requests | ok | errors | p50 ms | p95 ms | p99 ms | max ms | req/s |
|---|---|---|---|---|---|---|---|---|---|---|
| sustained | 32 | 12 | 16247 | 6181 | http_429×10066 | 94.9 | 198.8 | 265.5 | 533.7 | 206.0 |
| sustained | 32 | 12 | 16584 | 6022 | http_429×10562 | 88.6 | 195.7 | 267.0 | 579.2 | 200.7 |
| sustained | 32 | 12 | 18043 | 5997 | http_429×12046 | 74.4 | 171.1 | 265.6 | 624.4 | 199.9 |
| sustained | 32 | 12 | 18354 | 5988 | http_429×12366 | 71.7 | 155.6 | 206.5 | 616.3 | 199.6 |
| sustained | 32 | 12 | 18096 | 6018 | http_429×12078 | 70.1 | 159.6 | 284.1 | 699.5 | 200.6 |
| sustained | 32 | 12 | 18089 | 5984 | http_429×12105 | 71.5 | 162.3 | 251.6 | 776.7 | 199.5 |
| sustained | 32 | 12 | 18281 | 6026 | http_429×12255 | 69.7 | 163.4 | 230.3 | 333.9 | 200.9 |
| sustained | 32 | 12 | 18356 | 5960 | http_429×12396 | 71.3 | 159.8 | 220.8 | 574.0 | 198.7 |
| sustained | 32 | 12 | 17949 | 6032 | http_429×11917 | 69.1 | 158.0 | 298.1 | 912.7 | 201.1 |
| sustained | 32 | 12 | 18605 | 5984 | http_429×12621 | 72.3 | 153.7 | 205.8 | 371.8 | 199.5 |
| sustained | 32 | 12 | 18193 | 6004 | http_429×12189 | 71.5 | 157.0 | 250.4 | 505.8 | 200.1 |
| sustained | 32 | 12 | 18245 | 5986 | http_429×12259 | 71.1 | 157.6 | 221.5 | 689.5 | 199.5 |
| sustained | 32 | 12 | 17673 | 6024 | http_429×11649 | 71.0 | 164.3 | 235.0 | 532.6 | 200.8 |
| sustained | 32 | 12 | 17930 | 5998 | http_429×11932 | 72.1 | 157.3 | 231.7 | 637.7 | 199.9 |
| sustained | 32 | 12 | 18104 | 5994 | http_429×12110 | 71.1 | 162.0 | 259.7 | 678.1 | 199.8 |
| sustained | 32 | 12 | 18501 | 5988 | http_429×12513 | 70.9 | 157.5 | 230.1 | 432.2 | 199.6 |
| sustained | 32 | 12 | 18144 | 6005 | http_429×12139 | 71.0 | 159.9 | 279.0 | 743.3 | 200.2 |
| sustained | 32 | 12 | 18477 | 6018 | http_429×12459 | 70.9 | 156.3 | 240.6 | 546.3 | 200.6 |
| sustained | 32 | 12 | 18548 | 5995 | http_429×12553 | 72.3 | 154.0 | 201.3 | 347.6 | 199.8 |
| sustained | 32 | 12 | 17837 | 5989 | http_429×11848 | 75.5 | 168.1 | 249.0 | 593.4 | 199.6 |

- concurrency held: **32** for 600.1s
- first 30s window: **206.0 req/s**
- best window: **206.0 req/s**
- scale-up factor: **1.0x**
- seconds to reach 90% of best throughput: **0**

This is the number to size against: `min_provisioned_concurrency` is what you get immediately, `max` is what you get after this long.

## Traffic spike

| phase | conc | rows/req | requests | ok | errors | p50 ms | p95 ms | p99 ms | max ms | req/s |
|---|---|---|---|---|---|---|---|---|---|---|
| spike_baseline | 2 | 12 | 343 | 343 | - | 54.4 | 71.5 | 108.0 | 517.4 | 34.2 |
| spike_load | 48 | 12 | 13988 | 5218 | http_429×8770 | 152.9 | 293.7 | 373.9 | 479.9 | 207.1 |
| spike_recovery | 2 | 12 | 700 | 696 | http_429×4 | 53.6 | 68.5 | 94.4 | 327.8 | 34.7 |

- first second of the spike, p95: **316.0 ms**
- seconds to return within 1.5x baseline p95: **None**

## Features without a model (Feature Serving endpoint)

The online-lookup share of the ranker's latency, measured directly.

| phase | conc | rows/req | requests | ok | errors | p50 ms | p95 ms | p99 ms | max ms | req/s |
|---|---|---|---|---|---|---|---|---|---|---|
| features_only | 1 | 1 | 30 | 30 | - | 43.5 | 69.9 | 72.9 | 72.9 | 22.0 |
| features_only | 1 | 4 | 30 | 30 | - | 30.9 | 51.4 | 59.7 | 59.7 | 28.5 |
| features_only | 1 | 8 | 30 | 30 | - | 31.0 | 41.1 | 44.8 | 44.8 | 30.8 |
| features_only | 1 | 16 | 30 | 30 | - | 35.1 | 47.4 | 65.1 | 65.1 | 27.6 |
| features_only | 1 | 32 | 30 | 30 | - | 33.0 | 48.7 | 90.1 | 90.1 | 27.3 |

## Endpoint configuration, verbatim

```json
{
  "endpoint": "crunchyroll-rail-ranker",
  "state": {
    "ready": "READY",
    "config_update": "NOT_UPDATING"
  },
  "route_optimized": false,
  "served_entities": [
    {
      "name": "rail_ranker-10",
      "entity": "serverless_lakebase_praneeth_catalog.crunchyroll_demo.crunchyroll_rail_ranker",
      "version": "10",
      "workload_size": null,
      "workload_type": "CPU",
      "scale_to_zero": false,
      "min_provisioned_concurrency": 4,
      "max_provisioned_concurrency": 32,
      "burst_scaling_enabled": null
    }
  ],
  "inference_table": {
    "catalog": "serverless_lakebase_praneeth_catalog",
    "schema": "crunchyroll_demo",
    "prefix": "cr_rail_inference",
    "enabled": true
  }
}
```
