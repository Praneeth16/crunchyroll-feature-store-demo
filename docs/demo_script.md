# Demo walkthrough (presenter guide) — vertical ranking

**Runtime:** 45 min demo + 15 min discussion.
**Workspace:** `<workspace>` · catalog `<catalog>` · schema `crunchyroll_demo`.
**App:** `make app-url` prints it.

This guide assumes `./setup.sh --profile <PROFILE>` has completed, so everything below is
already built and running and nothing requires a job to finish live. Current-state numbers
are from the reference run of 2026-09-24 against model version **12**, which is
`@champion` and is the version the endpoint serves. Measurements made on earlier versions
are labelled with their version and date. In your own workspace, read the numbers off your
own run: they move a little with each regeneration of the synthetic data.

---

## The one sentence the demo has to land

The ask: *cover both batch and real-time.*

> **Both are covered, and they are the same model.** The batch path you can ship before an
> online store is available in your serving region, and the real-time path you move to
> afterwards, read the same feature definitions, the same training set and the same
> registered model version. Both were run and compared: **identical collection order, 16
> of 16, Spearman 1.0** (model v10, 2026-09-18). Batch is not a prototype you throw away. It
> is the same pipeline with the last mile swapped.

Say this in the first two minutes, then spend the rest of the demo proving it.

---

## Pre-flight, before the session (10 min)

```bash
cd <repo-root>
make verify       # read-only assertions; expect 0 failures, 1 warn (route_optimized)
make app-url      # confirm the app is ACTIVE, open it, click every panel once
```

Expected state:

| | expected |
|---|---|
| `crunchyroll-rail-ranker` | READY, `rail_ranker-12`, scale_to_zero **false**, concurrency 4–32, route optimization off |
| `crunchyroll-candidate-retriever` | READY, v8 |
| `crunchyroll-watch-next-ranker` | READY, v15 |
| `crunchyroll-online-store` | AVAILABLE, CU_1 |
| app `crfs-watch-next` | ACTIVE |
| `@champion` on `crunchyroll_rail_ranker` | **12** |

Version numbers are whatever your workspace's latest training run produced. The check that
matters is that the served entity equals `@champion`.

**Warm the endpoint.** `scale_to_zero=false`, so there is no cold start, but the first
request of a new payload shape is slower than steady state (~50 ms p50). Load the app once
before the session; its start-up warm-up request covers the rest.

**Do not hand-type `rail_id` values live.** An unknown rail id produces `Error ''` from
the endpoint, not a helpful message: the feature lookup misses and the model gets NULL.
The 16 real ids are `r_continue r_new_eps r_because r_simulcast r_top10 r_watchlist
r_trending r_action r_fantasy r_scifi r_romance r_slice r_drama r_sports r_movies
r_classics`. Use the app's picker.

---

## Act 0 · Frame the two rankings (2 min) — slides, no screen share

* **Horizontal ranking** — ordering anime *within* a collection. Built: `crunchyroll-watch-next-ranker`.
* **Vertical ranking** — ordering the *collections* on the homepage. Built: `crunchyroll-rail-ranker`.

They are different grains, different labels, different models, and **they share the
feature store**. That is the thing worth demoing.

---

## Act 1 · The shared feature store (8 min)

**Open:** notebook `21_rail_features.py`, run output.

Point at the table at the top:

| feature table | grain | new for vertical ranking? |
|---|---|---|
| `viewer_features_current` | viewer | no — reused unchanged |
| `recent_behavior_current` | viewer | no — reused unchanged |
| `session_features_current` | viewer | no — reused unchanged, CONTINUOUS |
| `title_features` | title | no — aggregated to collection grain |
| `rail_features` | collection | **new** |
| `viewer_rail_features_ts` | viewer × collection | **new** |

**The line to say:** adding a whole second ranking model at a new grain cost **two feature
tables and three UDFs**. Four of six inputs are the same governed tables, with no second
pipeline behind them. Nothing forked, nothing copied, no private per-model copy of a
viewer feature.

**Then the time-series table**, which is the part a feature-store team will care about:

`viewer_rail_features_ts` is **one** table, not two. Offline it holds every daily snapshot,
so training does a real point-in-time join. Published online, it deduplicates to the
latest row per key: **463,419 offline rows → 4,681 online rows**, one per (viewer,
collection). So there is no `_current` mirror to keep in sync: training and serving read
the same table through the same `FeatureLookup`.

That is the strongest available form of the training/serving-consistency argument: not
"two tables built from one definition", but one table.

---

## Act 2 · Training and registration (6 min)

**Open:** notebook `22_train_rail_ranker.py`.

Three things to show, in order:

1. **The training set is declarative.** `fe.create_training_set` with 4 `FeatureLookup`s
   and 5 `FeatureFunction`s. No hand-written join anywhere.
2. **Point-in-time is real, not nominal.** `timestamp_lookup_key` against
   `viewer_rail_features_ts`. Snapshots are stamped at the *end* of the day they summarise,
   so a training row for day D reads day D−1 and earlier. It cannot read the clicks that
   came after the impression it is trying to predict.
3. **Position bias is corrected.** Every label in a homepage log was observed at a
   position the *incumbent* chose. Uncorrected, the model learns the incumbent. Inverse
   propensity weights come from `rail_position_propensity`. Without this the lift below
   would be an artifact.

**Registry, live.** Open the model in Unity Catalog:

| | value (v12, 2026-09-24) |
|---|---|
| model | `<catalog>.crunchyroll_demo.crunchyroll_rail_ranker` |
| version | **12**, alias `@champion` |
| NDCG@5 | **0.6978** vs **0.6602** incumbent editorial order = **+5.70%** |
| MRR | 0.7016 vs 0.6590 |
| holdout AUC | 0.7424 all impressions, **0.6319** viewed impressions only |
| data | 80,494 training rows; holdout 9,663 rows, 524 homepage sessions |
| tags | `ndcg5_lift_vs_editorial=+0.0570`, `position_bias_correction=ips` |

**Say the caveat before anyone asks:** this is synthetic data. +5.70% is evidence the
pipeline works end to end. It is **not** a forecast of production lift, and it moves
between regenerations (v11 measured +4.29% on the previous data).

**The ablation.** A ranking team always asks this, so answer it unprompted: "is the lift
personalization, or just a better *fixed* collection order?" Remove all 13
collection-identity features and NDCG@5 goes **0.6978 → 0.7067** (+7.0% over editorial). It
does not drop. Spearman(full, ablated) 0.9309, so the two models genuinely differ. The lift
is personalization.

---

## Act 3 · BATCH — the path that needs no online store (12 min) ⭐

**If the near-term deliverable is batch, this is the act that matters most. Lead with it,
not with the endpoint.**

Open `26_batch_scoring.py`. Frame it:

> Until an online store is available in your serving region, the deliverable is batch. This
> notebook is the batch path, and there is no online store anywhere in it.

**Show the resolve:** `@champion` → a concrete version, the *same* version the endpoint
serves. This is deliberate: if batch and online scored different versions, the comparison
at the end of the notebook would be meaningless.

**Show the one call that is the whole story:**

```python
scored = fe.score_batch(model_uri=MODEL_URI, df=scoring_sdf)
```

No feature values are passed in. The model carries its own feature spec, so this call
resolves 4 feature tables and 5 UC Python UDFs against the **offline** store. The endpoint
uses the same spec, unchanged, against Lakebase.

**Measured (model v10, 2026-09-18):**

| | measured |
|---|---|
| rows scored | **4,628** (300 viewers × 14–16 eligible collections, mean 15.4) |
| `score_batch` + materialisation | **69.9 s** |
| write of the ranked table | **2.9 s** |
| output | `rail_rankings_batch`, 4,628 rows, `model_version` 10 |

**State the limit up front:** 69.9 s for 4,628 rows is dominated by Spark startup on
serverless, not per-row work. It does **not** extrapolate. What transfers is the *shape*:
a full refresh is `O(viewers × eligible collections)`; an incremental refresh is
`O(viewers whose features changed)`. At a minutes cadence only the second is viable.

**Then the incremental mode**, which is what makes a minutes cadence possible:

Change Data Feed is enabled on all four feature tables, so "whose features moved since the
last run" is a query, not a guess. `make batch-incremental` reads each table's change feed
from the version the last run recorded, collects the affected `viewer_id`s, rescores only
those, and writes with `replaceWhere` so the rest of the table stands.

One honest asymmetry to volunteer: `rail_features` is keyed by `rail_id`, so **any** change
to it moves every viewer's ordering and forces a full refresh. The notebook detects that
and says so rather than shipping a half-stale table.

> ⚠️ See "Known gaps" below before you demo incremental live.

**Close the act on the handoff question:** how the homepage service consumes
`rail_rankings_batch` (query the Delta table directly, or populate its own cache from it)
is the platform team's decision, and it changes the freshness contract. Worth settling
early.

---

## Act 4 · ONLINE — the real-time path (10 min)

**Open the app** (FastAPI backend, React frontend). The control bar runs across the top:
viewer, device, hour, clock. Point at the request waterfall first: both rankers and three
Lakebase reads, fanned out in one request. Server-side total **119 ms p50 / 216 ms p95**
on 2026-09-24 (earlier runs 94–109 / 142–176 ms); the panel shows the live p50/p95 for the
session.

**Beat 1 — the collection order.** Pick viewer `v0001`. Show the ranked collections, the
probability per collection, and the incumbent editorial order beside it.

**Beat 2 — what the caller actually sends.** This is the slide-worthy fact:

> **7 request fields in. 45 feature values resolved server-side**, across 4 feature tables
> and 5 UDFs. The model scores on 47 features: 43 of those retrieved values plus the 4
> context fields the caller sends.

The homepage service does not fetch features. It does not know which tables exist. It
sends a viewer, a context and the eligible collections. Everything else is the endpoint's
job, because the feature spec is logged *inside* the model.

**Beat 3 — the online row it looked up.** The app shows the actual `online_viewer_rail`
row behind the score, with the SQL and its latency. Composite key (viewer × collection),
read from Lakebase inside the request.

**Beat 4 — eligibility is a filter, never a feature.** Four collections depend on viewer
state (Continue Watching, Watchlist, Because You Watched, New Episodes), so the eligible
set genuinely varies 14–16 per request. It is applied *before* scoring, exactly as a
production homepage service would.

**Beat 5 — context changes the answer.** Flip hour 09:00 ↔ 21:00 and device mobile ↔ TV.
Nothing in the feature store changes, yet several collections move. Read the count off the
*Context sensitivity* panel, which scores all four contexts in parallel, rather than quoting
a fixed number: two request-time features decay with the clock and the spread moves between
runs.

**Beat 6 — "Why this?".** Click a title. The explanation streams from a Databricks-hosted
foundation model, grounded only on the online rows the service read for this request and
the model's score. Nothing is generated until asked.

**Beat 7 — the homepage survives the endpoint.** In the *Fallback* panel pick "Endpoints
slow". The rail badge turns **cached**: the viewer's last good order, filtered to today's
eligible rails, rendered at the time budget instead of waiting. With "Breakers open" and no
cached order it falls to **editorial**. The endpoint sheds load with 429 past its capacity,
so this tiering is the part of the homepage service a platform team must own
([homepage_service.md](homepage_service.md)). Switch back to "Healthy".

---

## Act 5 · The proof that batch and online agree (5 min) ⭐

**This is the closing argument.** Back to notebook 26, last section.

Same viewer. Same collections. Same context. Same model version. Scored both ways
(model v10, 2026-09-18):

| | result |
|---|---|
| collections compared | **16** |
| identical rank | **16 of 16** |
| Spearman(batch, online) | **1.0** |
| max absolute probability difference | **0.0026** |

**Volunteer the explanation for the 0.0026.** It is not noise and not a bug.
`cr_rail_click_recency` takes `request_epoch_s` as an input, and the batch job and the
endpoint call happened seconds apart. A time-decay feature *should* return a different
value at a different clock. Ranks are unaffected because every collection in the request
shifts by the same clock. **If this number were zero, the recency feature would not be
doing its job.**

**Then the migration, concretely:**

```
Phase 1 — batch, no online store
  notebooks 00,01     feature engineering       → Delta feature tables in UC
  notebooks 20,21     collection features       → Delta feature tables in UC
  notebook  22        PIT training set + model  → registered in Unity Catalog
  notebook  26        score_batch               → rail_rankings_batch (Delta)
                      make batch-incremental    → minutes cadence via CDF

Phase 2 — when the online store is available in the serving region
  notebook  21        publish_table(...)        → the same tables, published online
  notebook  23        deploy the same version   → crunchyroll-rail-ranker endpoint
```

Nothing in the feature definitions, the training set or the model changes. **The version
that was scoring in batch is the version that gets served.**

**Be honest about why Phase 2 is worth doing.** It is *not* primarily latency. It is the
context movement from Beat 5: personalization that a precomputed table structurally cannot
deliver, because 5 of the model's features read `device`, `hour_of_day` and
`request_epoch_s`, none of which exist until a request arrives. Precomputing every context
multiplies the table by the number of contexts, which is the expensive direction at a
minutes cadence.

---

## Act 6 · Production serving numbers (7 min)

Full tables in [serving_benchmark.md](serving_benchmark.md), written by the benchmark job.
In-region client, 2026-09-24, served entity `rail_ranker-12`, `scale_to_zero=false`,
provisioned concurrency 4–32, route optimization off (not enabled on the reference
workspace).

| question | measured |
|---|---|
| latency, low concurrency | **p50 ~50 ms** (12 collections, concurrency 1–8) |
| latency vs collections per request | **flat 1 → 32** (p50 50–55 ms), lookups are batched, not serial |
| feature lookups + UDFs alone | p50 **33–35 ms** at 4–32 rows (Feature Serving endpoint) |
| server-side execution time | p50 **64 ms** (inference table) |
| clean throughput | **147 req/s** at concurrency 8, no errors |
| behaviour past capacity | **HTTP 429**, not unbounded queueing; ~214 req/s ceiling at 16–64 concurrent |
| spike 2 → 48 concurrent | p50 103 ms, p95 255 ms, **10,157 of 15,395 rejected**, 208 req/s served |
| recovery after the spike | immediate: p50 49.5 ms, p95 61 ms, 15 residual 429s |
| sustained concurrency 32 for 10 min | **83.9 → 201.2 req/s** (2.4×), 90% of best after **60 s** |

**The fanout result is the most useful thing here:** 32 collections cost the same as 4.
**A richer homepage is close to free at the ranking layer**: more collections cost no
latency, only a bigger response body.

**Be straight about autoscaling, because it is not one number.** This run showed capacity
arriving over about a minute (2.4× from the first 30 s window to the best). The previous
run (2026-09-17, v10) showed **no** scale-up at all (factor 1.0) because it started at full
capacity. What survives regardless:

> **Provision the floor for peak. Do not size against autoscaling.**
> `min_provisioned_concurrency` is what you get immediately.

**Two vantage points, deliberately.** A laptop saturates at 53.8 req/s (earlier run) and
therefore sees **zero** 429s in the same spike that rejects thousands in region.
Benchmarking a serving endpoint from outside its region overstates headroom by roughly 4×.

---

## Act 7 · What this does not claim (5 min)

Lead with this; it is what makes the rest credible.

| gap | status |
|---|---|
| Scale at production cardinality | **not demonstrated.** 4,681 online keys is not tens of millions. Top follow-up. |
| Training at production volume | **partial.** The PIT join is Spark and scales; the estimator is `toPandas()` + scikit-learn, single-driver. A GPU path exists ([gpu_training.md](gpu_training.md)), and swapping the estimator touches neither the feature layer nor serving. |
| Fallback path | **built as a reference** in the app: time budget, circuit breaker, cached → editorial tiers ([homepage_service.md](homepage_service.md)). Where the last-good ranking lives at fleet scale and the budget itself are for the platform team. |
| Canary / traffic split | **built with a gate**: 90/10 split, paired per-version measurement, promote or roll back ([canary.md](canary.md)). It judges serving safety, not online quality; that needs click attribution by served version. |
| `rail_rankings_batch` → homepage handoff | **the platform team's decision.** Changes the freshness contract. |
| Minutes cadence at production MAU | **unsized.** Mechanism exists, measured at demo scale only. Needs MAU and feature-change rate. |
| Route optimization | not enabled on the reference workspace; the lowest achievable latency was not measured. |

**Then the split** — what Databricks provides vs what the team builds vs what it operates
([vertical_ranking.md](vertical_ranking.md) §5):

* **Provided:** feature plumbing, the PIT join, the online store, automatic feature lookup
  at serving, the registry, the endpoint, batch scoring against the offline store.
* **Built by the team:** feature definitions, **the homepage impression log including
  rendered position** (without it there is no vertical ranking at all), eligibility rules,
  candidate resolution for personalized collections, the production homepage service and
  its fallbacks.
* **Operated by the team:** freshness, capacity, retraining cadence, cost.

---

## Known gaps in the demo itself — read before you present

1. **The batch path has no UI.** The app shows the online path only. The batch story lives
   in notebook 26's output and in a SQL query against `rail_rankings_batch`. Have that
   query open in a tab:
   ```sql
   SELECT viewer_id, rail_id, rail_rank, engagement_probability, model_version
   FROM <catalog>.crunchyroll_demo.rail_rankings_batch
   WHERE viewer_id = 'v0001' ORDER BY rail_rank
   ```
2. **Validate `make batch-incremental` before showing it live.** Two defects in it were
   found and fixed by review (CDF `startingVersion` is inclusive; an empty changed-viewer
   list was treated as "all viewers") — see [verification_log.md](verification_log.md) V62.
   Either run it once beforehand or describe the mechanism from the code.
3. **An incremental run with no feature changes correctly does nothing.** It reports "no
   viewers changed". To demo real incremental work, a feature table has to move first
   (`make burst` writes events).
4. **`route_optimized` is off** on the reference workspace. If asked about the lowest
   achievable latency, that is the lever that was not pulled here, not a platform limit.

---

## Questions to expect, with the answer ready

**"Can we serve both rankings from one endpoint?"**
Different grains, different request shapes, different labels, so two endpoints. What is
shared is the feature layer, which is where the duplication cost would otherwise be.

**"How fresh are the features at serving time?"**
Three cadences, deliberately: `session_features_current` publishes CONTINUOUS (streaming),
`recent_behavior_current` TRIGGERED, the rest daily. `make burst` (or the app's freshness
panel) demonstrates an event changing the next ranking. Freshness is a per-table decision,
not a global one.

**"What happens if the online store or the endpoint is down?"**
The endpoint call fails or blows its time budget, the circuit breaker opens after 5
consecutive failures, and the homepage renders the viewer's last good order, then the
editorial order. Every response names the tier that served it. In Phase 1 the question
does not arise: batch has no online dependency, which is a genuine argument for starting
there.

**"Why is the online store the only always-on cost?"**
A store that sleeps cannot answer a keyed read in single-digit milliseconds. Measured
**$15.95/day at CU_2** list; this demo runs CU_1 ([cost_and_sizing.md](cost_and_sizing.md)).
The request-path endpoints also run with `scale_to_zero=false` so the numbers are not
first-request numbers; that is a latency choice with a cost, not a requirement.

**"Is 45 or 47 features the right number?"**
Both, and they mean different things. **45** feature values are resolved server-side; the
model scores on **47** = 43 of those retrieved values + the 4 context fields the caller
sends. Two of the retrieved values are UDF inputs only and never reach the estimator.
