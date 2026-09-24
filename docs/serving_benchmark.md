# Rail-ranking endpoint — measured serving characteristics

Measured 2026-09-24 10:52 UTC from **in_region_job** (client in the same region as the endpoint).

Configuration: endpoint `crunchyroll-rail-ranker` · route_optimized=False · scale_to_zero=False · provisioned_concurrency=4-32

Every number below is client-observed wall time unless it says otherwise, and every latency is reported with the concurrency it was measured at.

## Fanout — latency vs candidate rails per request

Each additional rail is one more composite-key read against the Lakebase online store, inside the same request.

| phase | conc | rows/req | requests | ok | errors | p50 ms | p95 ms | p99 ms | max ms | req/s |
|---|---|---|---|---|---|---|---|---|---|---|
| fanout | 1 | 1 | 40 | 40 | - | 55.1 | 103.1 | 143.0 | 143.0 | 14.4 |
| fanout | 1 | 4 | 40 | 40 | - | 51.4 | 85.2 | 95.9 | 95.9 | 18.2 |
| fanout | 1 | 8 | 40 | 40 | - | 50.6 | 78.5 | 107.6 | 107.6 | 18.2 |
| fanout | 1 | 16 | 40 | 40 | - | 50.5 | 87.5 | 99.2 | 99.2 | 18.3 |
| fanout | 1 | 32 | 40 | 40 | - | 50.0 | 60.4 | 62.1 | 62.1 | 19.7 |

## Concurrency ramp

| phase | conc | rows/req | requests | ok | errors | p50 ms | p95 ms | p99 ms | max ms | req/s |
|---|---|---|---|---|---|---|---|---|---|---|
| ramp | 1 | 12 | 225 | 225 | - | 50.9 | 65.6 | 87.0 | 114.5 | 18.7 |
| ramp | 2 | 12 | 465 | 465 | - | 49.6 | 63.2 | 79.6 | 84.8 | 38.7 |
| ramp | 4 | 12 | 905 | 905 | - | 49.8 | 62.9 | 79.7 | 531.4 | 75.1 |
| ramp | 8 | 12 | 1775 | 1775 | - | 50.6 | 71.8 | 94.4 | 298.0 | 147.2 |
| ramp | 16 | 12 | 4905 | 2595 | http_429×2310 | 56.0 | 85.5 | 112.9 | 276.1 | 214.3 |
| ramp | 32 | 12 | 7371 | 2589 | http_429×4782 | 69.6 | 164.9 | 345.7 | 439.8 | 213.5 |
| ramp | 64 | 12 | 7309 | 2639 | http_429×4670 | 128.0 | 321.4 | 439.1 | 628.7 | 218.1 |

## Sustained load — how long capacity takes to arrive

The ramp above gives each level 12 seconds, which measures a steady state at current capacity and cannot see capacity being added. This phase holds one level and slices by wall-clock window.

| phase | conc | rows/req | requests | ok | errors | p50 ms | p95 ms | p99 ms | max ms | req/s |
|---|---|---|---|---|---|---|---|---|---|---|
| sustained | 32 | 12 | 2517 | 2517 | - | 315.4 | 882.8 | 1065.6 | 1283.8 | 83.9 |
| sustained | 32 | 12 | 8021 | 4802 | http_429×3219 | 117.9 | 535.3 | 910.2 | 1183.0 | 160.1 |
| sustained | 32 | 12 | 14899 | 6017 | http_429×8882 | 94.8 | 213.2 | 281.6 | 1655.4 | 200.6 |
| sustained | 32 | 12 | 15016 | 5989 | http_429×9027 | 89.8 | 214.4 | 294.4 | 569.9 | 199.6 |
| sustained | 32 | 12 | 16964 | 5999 | http_429×10965 | 82.7 | 192.3 | 271.6 | 700.8 | 200.0 |
| sustained | 32 | 12 | 17667 | 6037 | http_429×11630 | 74.7 | 180.2 | 271.7 | 703.7 | 201.2 |
| sustained | 32 | 12 | 18155 | 6002 | http_429×12153 | 69.6 | 165.4 | 264.2 | 558.3 | 200.1 |
| sustained | 32 | 12 | 18286 | 5969 | http_429×12317 | 69.3 | 166.7 | 251.8 | 583.1 | 199.0 |
| sustained | 32 | 12 | 18729 | 5985 | http_429×12744 | 68.4 | 158.8 | 220.5 | 529.5 | 199.5 |
| sustained | 32 | 12 | 17859 | 6029 | http_429×11830 | 68.6 | 163.5 | 274.2 | 681.1 | 201.0 |
| sustained | 32 | 12 | 18805 | 5975 | http_429×12830 | 68.1 | 153.5 | 237.5 | 645.2 | 199.2 |
| sustained | 32 | 12 | 17893 | 6022 | http_429×11871 | 70.0 | 161.2 | 237.7 | 415.3 | 200.7 |
| sustained | 32 | 12 | 18328 | 5999 | http_429×12329 | 70.1 | 169.2 | 260.5 | 1589.4 | 200.0 |
| sustained | 32 | 12 | 18493 | 6001 | http_429×12492 | 69.1 | 160.1 | 260.5 | 768.1 | 200.0 |
| sustained | 32 | 12 | 18849 | 6000 | http_429×12849 | 68.9 | 154.4 | 214.8 | 701.8 | 200.0 |
| sustained | 32 | 12 | 18933 | 5998 | http_429×12935 | 66.8 | 163.2 | 270.9 | 810.4 | 199.9 |
| sustained | 32 | 12 | 19028 | 5986 | http_429×13042 | 67.7 | 160.0 | 257.8 | 588.0 | 199.5 |
| sustained | 32 | 12 | 18750 | 5999 | http_429×12751 | 67.7 | 153.1 | 221.1 | 720.2 | 200.0 |
| sustained | 32 | 12 | 18976 | 6027 | http_429×12949 | 68.5 | 151.5 | 211.5 | 571.9 | 200.9 |
| sustained | 32 | 12 | 19178 | 5982 | http_429×13196 | 66.6 | 155.4 | 232.0 | 706.6 | 199.4 |

- concurrency held: **32** for 600.1s
- first 30s window: **83.9 req/s**
- best window: **201.2 req/s**
- scale-up factor: **2.4x**
- seconds to reach 90% of best throughput: **60**

This is the number to size against: `min_provisioned_concurrency` is what you get immediately, `max` is what you get after this long.

## Traffic spike

| phase | conc | rows/req | requests | ok | errors | p50 ms | p95 ms | p99 ms | max ms | req/s |
|---|---|---|---|---|---|---|---|---|---|---|
| spike_baseline | 2 | 12 | 386 | 386 | - | 50.0 | 65.6 | 71.2 | 78.8 | 38.5 |
| spike_load | 48 | 12 | 15395 | 5238 | http_429×10157 | 103.2 | 254.7 | 348.0 | 921.0 | 208.1 |
| spike_recovery | 2 | 12 | 794 | 779 | http_429×15 | 49.5 | 61.1 | 70.1 | 94.0 | 38.9 |

- first second of the spike, p95: **201.7 ms**
- seconds to return within 1.5x baseline p95: **None**

## Features without a model (Feature Serving endpoint)

The online-lookup share of the ranker's latency, measured directly.

| phase | conc | rows/req | requests | ok | errors | p50 ms | p95 ms | p99 ms | max ms | req/s |
|---|---|---|---|---|---|---|---|---|---|---|
| features_only | 1 | 1 | 30 | 30 | - | 58.6 | 69.0 | 85.4 | 85.4 | 18.3 |
| features_only | 1 | 4 | 30 | 30 | - | 34.0 | 53.2 | 58.2 | 58.2 | 25.7 |
| features_only | 1 | 8 | 30 | 30 | - | 32.9 | 55.7 | 78.1 | 78.1 | 27.1 |
| features_only | 1 | 16 | 30 | 30 | - | 33.6 | 50.8 | 59.0 | 59.0 | 28.1 |
| features_only | 1 | 32 | 30 | 30 | - | 35.4 | 41.0 | 53.1 | 53.1 | 27.7 |

## Server-side execution time (from the inference table)

- requests captured: 8542
- execution_time_ms p50 / p95 / p99: **64.0 / 687.9 / 937.2 ms**
- non-200 responses: 0

`execution_time_ms` excludes the network. Client wall time minus this is transport.

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
      "name": "rail_ranker-12",
      "entity": "serverless_lakebase_praneeth_catalog.crunchyroll_demo.crunchyroll_rail_ranker",
      "version": "12",
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
