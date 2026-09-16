# Rail-ranking endpoint — measured serving characteristics

Measured 2026-09-16 20:02 UTC from **in_region_job** (client in the same region as the endpoint).

Configuration: endpoint `crunchyroll-rail-ranker` · route_optimized=False · scale_to_zero=False · provisioned_concurrency=4-32

Every number below is client-observed wall time unless it says otherwise, and every latency is reported with the concurrency it was measured at.

## Fanout — latency vs candidate rails per request

Each additional rail is one more composite-key read against the Lakebase online store, inside the same request.

| phase | conc | rows/req | requests | ok | errors | p50 ms | p95 ms | p99 ms | max ms | req/s |
|---|---|---|---|---|---|---|---|---|---|---|
| fanout | 1 | 1 | 40 | 40 | - | 86.4 | 121.2 | 152.2 | 152.2 | 11.8 |
| fanout | 1 | 4 | 40 | 40 | - | 55.4 | 109.6 | 184.7 | 184.7 | 15.6 |
| fanout | 1 | 8 | 40 | 40 | - | 54.3 | 103.4 | 126.6 | 126.6 | 15.9 |
| fanout | 1 | 16 | 40 | 40 | - | 54.2 | 76.6 | 88.9 | 88.9 | 17.2 |
| fanout | 1 | 32 | 40 | 40 | - | 57.8 | 75.3 | 84.7 | 84.7 | 16.5 |

## Concurrency ramp

| phase | conc | rows/req | requests | ok | errors | p50 ms | p95 ms | p99 ms | max ms | req/s |
|---|---|---|---|---|---|---|---|---|---|---|
| ramp | 1 | 12 | 228 | 228 | - | 51.6 | 64.0 | 71.9 | 74.0 | 19.0 |
| ramp | 2 | 12 | 428 | 428 | - | 52.4 | 68.2 | 108.8 | 294.9 | 35.5 |
| ramp | 4 | 12 | 795 | 795 | - | 58.7 | 78.9 | 95.9 | 141.8 | 65.9 |
| ramp | 8 | 12 | 925 | 925 | - | 99.5 | 159.3 | 212.4 | 371.3 | 76.0 |
| ramp | 16 | 12 | 977 | 977 | - | 183.9 | 334.8 | 449.0 | 770.4 | 80.0 |
| ramp | 32 | 12 | 1007 | 1007 | - | 359.3 | 674.1 | 781.0 | 872.5 | 80.7 |
| ramp | 64 | 12 | 7952 | 1064 | http_429×6888 | 474.8 | 697.7 | 782.9 | 895.6 | 84.6 |

## Traffic spike

| phase | conc | rows/req | requests | ok | errors | p50 ms | p95 ms | p99 ms | max ms | req/s |
|---|---|---|---|---|---|---|---|---|---|---|
| spike_baseline | 2 | 12 | 344 | 344 | - | 55.3 | 75.6 | 88.0 | 187.4 | 34.2 |
| spike_load | 48 | 12 | 11053 | 5201 | http_429×5852 | 178.4 | 383.4 | 513.1 | 684.7 | 206.0 |
| spike_recovery | 2 | 12 | 722 | 722 | - | 52.6 | 66.7 | 76.7 | 282.3 | 36.1 |

- first second of the spike, p95: **401.6 ms**
- seconds to return within 1.5x baseline p95: **None**

## Features without a model (Feature Serving endpoint)

The online-lookup share of the ranker's latency, measured directly.

| phase | conc | rows/req | requests | ok | errors | p50 ms | p95 ms | p99 ms | max ms | req/s |
|---|---|---|---|---|---|---|---|---|---|---|
| features_only | 1 | 1 | 30 | 30 | - | 54.2 | 72.7 | 83.0 | 83.0 | 18.2 |
| features_only | 1 | 4 | 30 | 30 | - | 34.7 | 67.4 | 111.1 | 111.1 | 22.3 |
| features_only | 1 | 8 | 30 | 30 | - | 33.4 | 50.3 | 55.0 | 55.0 | 26.9 |
| features_only | 1 | 16 | 30 | 30 | - | 34.1 | 57.2 | 79.1 | 79.1 | 26.4 |
| features_only | 1 | 32 | 30 | 30 | - | 35.5 | 40.0 | 51.6 | 51.6 | 27.6 |

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
      "name": "rail_ranker-9",
      "entity": "serverless_lakebase_praneeth_catalog.crunchyroll_demo.crunchyroll_rail_ranker",
      "version": "9",
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
