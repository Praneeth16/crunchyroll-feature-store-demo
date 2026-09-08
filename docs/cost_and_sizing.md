# What this demo costs, and how to make it cost less

All figures are measured from `system.billing.usage` joined to
`system.billing.list_prices`, not estimated. They are **list prices** — a committed
contract will be lower. Reproduce any of them with `make cost`.

## The one always-on cost

A Lakebase Online Feature Store **cannot scale to zero**. A store that sleeps cannot
answer a keyed read in single-digit milliseconds, so the compute stays warm. This is
the only line in the architecture that bills while nobody is watching.

| | Value |
|---|---|
| SKU | `ENTERPRISE_DATABASE_SERVERLESS_COMPUTE_US_EAST_N_VIRGINIA` |
| Unit price | **$0.52 / DBU** (single active price row, verified) |
| At `CU_2` | 30.67 DBU/day = **$15.95/day ≈ $485/month** |
| Backing endpoint at `CU_2` | min 8 / max 16 CU |
| Spend before right-sizing (2026-08-31 → 09-07) | 221.4 DBU ≈ **$115** |

Everything else in the demo scales to zero: the ranker, retriever, feature-serving
and agent endpoints idle at nothing, and the jobs are serverless and per-run
(~45 minutes for a full rebuild).

## The capacity class governs the endpoint floor

Measured directly on 2026-09-07 by changing one thing:

```
PATCH /api/2.0/feature-store/online-stores/crunchyroll-online-store?update_mask=capacity
{"capacity": "CU_1"}
```

Before: `capacity=CU_2`, Lakebase endpoint `min 8 / max 16 CU`.
After:  `capacity=CU_1`, Lakebase endpoint `min 4 / max 8 CU`.

All three published tables stayed `SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE` throughout —
no re-publish was needed.

So the pattern is `CU_N` → a 4N CU endpoint floor, and **the capacity class is the
lever**, not the endpoint's autoscaling bounds. This is why the bundle does not
manage `postgres_endpoints` by default: setting bounds there can only fight the
class. `resources/lakebase.yml` explains the opt-in path if you want it anyway.

The expectation from halving the class is roughly half the DBU rate. Confirm on your
own workspace after 24 hours rather than trusting the arithmetic — the working set,
not the class alone, decides where inside the band the endpoint actually sits.

## Levers, in the order to reach for them

1. **Capacity class.** Size to the working set. This demo serves 300 viewers and
   132 titles; `CU_1` is generous for that and was still 8 CU of Postgres before
   the change.
2. **Read replicas.** `read_replica_count > 0` buys failover and read throughput and
   multiplies the always-on cost. Zero is right until a keyed read is on a
   latency-critical path with real traffic.
3. **Publish mode per table.** `TRIGGERED` costs a pipeline run per refresh;
   `CONTINUOUS` holds a streaming pipeline open. Reserve `CONTINUOUS` for features
   that must change a decision inside the session — see
   [streaming_paths.md](streaming_paths.md).
4. **What goes online at all.** Only serving-critical current values belong there.
   History belongs offline; the online store is not a warehouse.
5. **Serving endpoints scale to zero.** Leave that on except on demo day, where a
   cold start on stage costs more than the compute.

## What the docs say, verbatim

> "Lakebase scale-to-zero is not supported."
> "Online stores continuously incur costs. Delete online stores that are no longer needed."

So this is not a quirk of our setup. Delete the store between rehearsals.

## Stopping the meter

```bash
make teardown-cost      # app, endpoints, synced tables, online store. Data untouched.
make cost               # confirm the DATABASE_SERVERLESS line goes to zero tomorrow
```

`scripts/teardown.sh` is the same thing from a laptop, and `--full` also drops the
UC tables, models and UDFs. It deletes online tables with
`w.feature_store.delete_online_table()`, which the docs call the only recommended
method — `DROP TABLE` and the synced-table delete both leave the Postgres table behind. Order matters and the script encodes it: endpoints before
the store, synced tables before their sources, and unpublishing drops the Postgres
table as well as the UC entry — see the note in `src/crfs/ops.py`, because getting
that wrong leaves a state where UC shows nothing and the next publish still fails.
