# Canary gate: judge a challenger behind the live endpoint, then promote or roll back

`docs/open_items.md` §3 said a traffic split had been demonstrated (notebook 31 §6) but a
split is not a rollout: there was no metric to judge the canary on, no gate, and no
automatic rollback. `notebooks/30_advanced/33_canary_gate.py` is that process.

```bash
make canary PROFILE=<PROFILE>                       # dry run: decide, record, restore champion
databricks bundle run crfs_canary ... --params apply=true   # let a PROMOTE take traffic
```

## What it does

1. **Resolve both sides.** The champion is whatever `crunchyroll-rail-ranker` serves right
   now (asserted to be a single 100% route, so a leaked canary from a previous run is
   caught before a new one starts). The challenger is `crunchyroll_rail_ranker_gpu@challenger`
   from notebook 32, else the champion's previous version.
2. **Split** the endpoint 90/10, copying the sizing mode the endpoint actually realised
   (`src/crfs/canary.py`, shared with notebook 31 so the enum-serialisation and sizing bugs
   that bit this repo once are fixed in one place).
3. **Paired measurement.** For a fixed sample of viewers and their real eligible rails
   (`rails.eligible_rails_all`, one frozen context), score the *same* request on each served
   entity directly — `/served-models/<name>/invocations`. Routed traffic cannot do this: the
   response carries no version, and the inference table that does lands minutes later.
4. **Routed traffic** through the split, so the inference table has both entities.
5. **Gate** — PROMOTE only if every check passes:

   | check | default | why |
   |---|---|---|
   | error rate | ≤ 1% | a challenger that fails requests is out, whatever else it does |
   | p95 latency | ≤ 1.25 × champion | the homepage budget is 300 ms; latency regressions compound |
   | ranking agreement (Spearman vs champion) | ≥ 0.5 | a **guardrail**, not a quality metric: a model that reorders the whole homepage should be looked at by a human before it takes traffic |

6. **`finally`**: restore the champion at 100% — unless `apply=true` *and* PROMOTE, in which
   case the challenger takes 100%. The final routes are read back and asserted.
7. **Record** the decision, every check with its value, and the full report in
   `canary_decisions` (Delta, append-only).

Promotion stays two steps, as everywhere else here: routing is deployment; `@champion` is
moved only if the challenger is a version of the same UC model. The GPU model is a
different UC model, so promoting it is the route change plus the record, and notebook 23
must be pointed at it before any redeploy.

## Measured

First run, 2026-09-23 (`crfs_canary`, run 833952297646682, 35 min end to end — most of it
building the challenger's serving container), challenger `crunchyroll_rail_ranker_gpu` v5
(the torch MLP, `@challenger`) against champion `crunchyroll_rail_ranker` v11:

| | requests | errors | p50 | p95 |
|---|---|---|---|---|
| champion `rail_ranker-11` | 40 | 0 | 74 ms | 134 ms |
| challenger `crunchyroll_rail_ranker_gpu-5` | 40 | **40** | – | – |

Routed through the 90/10 split: 176 of 200 requests answered — the 24 failures are the
challenger's ~10% share. **Decision: ROLLBACK** on all three checks; routes read back as
`[('rail_ranker-11', 100)]`.

So the gate's first real decision was the one it exists for: a registered, aliased model
that trains and logs cleanly but **does not serve**, caught at 10% instead of at 100%. The
GPU model had never been behind an endpoint before — notebook 32 could not score it
off-endpoint either (`verification_log.md` V88) — and this run did not keep the error
text, only the count. `canary.summarise` now records the first error per entity, so the
next run's `canary_decisions` row names the cause. Finding it is the open follow-up.

## What it deliberately does not judge

**Online quality.** Whether the challenger gets more engagement needs impressions and
clicks attributed by served entity — the inference table has the entity, the homepage log
has the clicks, and joining them needs traffic volume and time a minutes-long gate does not
have. That join is the next thing to add, and it is where the canary percentage and duration
should come from.
