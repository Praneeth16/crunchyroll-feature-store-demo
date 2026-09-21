# Demo script — Crunchyroll vertical ranking

**Audience:** Mohit Kukkar + Crunchyroll ranking/platform.
**Runtime:** 45 min demo + 15 min discussion.
**Workspace:** `fevm-serverless-lakebase-praneeth` · catalog `serverless_lakebase_praneeth_catalog` · schema `crunchyroll_demo`.
**App:** https://crfs-watch-next-7474654904882204.aws.databricksapps.com

Everything below is already built and running. Nothing in the script requires a job to
finish live. Every number quoted is measured on this workspace against model version
**10**, which is `@champion` and is the version the endpoint serves.

---

## The one sentence the demo has to land

Mohit's question was *"hopefully we are covering both batch & realtime???"*

> **Yes — and the point is that they are the same model.** The batch path you ship in
> November and the real-time path you move to when Lakebase reaches GCP us-west1 read the
> same feature definitions, the same training set and the same registered model version.
> We ran both on this workspace and compared them: **identical collection order, 16 of 16,
> Spearman 1.0.** Batch is not a prototype you throw away — it is the same pipeline with
> the last mile swapped.

Say this in the first two minutes, then spend the rest of the demo proving it.

---

## Pre-flight, the morning of (10 min, do it before the call)

```bash
cd ~/Documents/Code/01-accounts/crunchyroll/crunchyroll_feature_store
make verify                      # 31 assertions; expect 0 failures, 1 warn (route_optimized)
make app-url                     # confirm the app is ACTIVE, open it, click every panel once
python3 /tmp/crfs_ctx_probe.py   # 4 live endpoint calls, proves the endpoint answers
```

Expected state:

| | expected |
|---|---|
| `crunchyroll-rail-ranker` | READY, `rail_ranker-10`, scale_to_zero **false**, concurrency 4–32 |
| `crunchyroll-candidate-retriever` | READY, v5 |
| `crunchyroll-watch-next-ranker` | READY, v9 |
| `crunchyroll-online-store` | AVAILABLE, CU_1 |
| app `crfs-watch-next` | ACTIVE |
| `@champion` | **10** |

**Warm the endpoint.** `scale_to_zero=false`, so there is no cold start — but the *first
request of a new payload shape* costs ~85 ms instead of ~52 ms. Send one 12-rail request
before the call so the shape is warm.

**Do not hand-type `rail_id` values live.** An unknown rail id produces `Error ''` from
the endpoint, not a helpful message — the feature lookup misses and the model gets NULL.
The 16 real ids are `r_continue r_new_eps r_because r_simulcast r_top10 r_watchlist
r_trending r_action r_fantasy r_scifi r_romance r_slice r_drama r_sports r_movies
r_classics`. Use the app's picker or the prepared script.

---

## Act 0 · Frame the two rankings (2 min) — slides, no screen share

Mohit's own vocabulary, back to him:

* **Horizontal ranking** — ordering anime *within* a collection. Built: `crunchyroll-watch-next-ranker`.
* **Vertical ranking** — ordering the *collections* on the homepage. Built: `crunchyroll-rail-ranker`.

They are different grains, different labels, different models — and **they share the
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
collection). So there is no `_current` mirror to keep in sync — training and serving read
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
   `viewer_rail_features_ts`. Snapshots are stamped at *end* of the day they summarise, so
   a training row for day D reads day D−1 and earlier — it cannot read the clicks that
   came after the impression it is trying to predict.
3. **Position bias is corrected.** Every label in a homepage log was observed at a
   position the *incumbent* chose. Uncorrected, the model learns the incumbent. Inverse
   propensity weights from `rail_position_propensity`. Without this the lift below would
   be an artifact.

**Registry, live** — open the model in UC:

| | value |
|---|---|
| model | `serverless_lakebase_praneeth_catalog.crunchyroll_demo.crunchyroll_rail_ranker` |
| version | **10**, alias `@champion` |
| NDCG@5 | **0.7157** vs **0.6791** incumbent editorial order = **+5.39%** |
| MRR | 0.7147 vs 0.6775 |
| holdout AUC (viewed impressions) | 0.6345 |
| holdout | 537 homepage sessions, 9,903 rows |
| tags | `ndcg5_lift_vs_editorial=+0.0539`, `position_bias_correction=ips` |

**Say the caveat before they ask it:** this is synthetic data. +5.39% is evidence the
pipeline works end to end. It is **not** a forecast of Crunchyroll's lift.

**The ablation** — this is the question a ranking team always asks, so answer it unprompted:
"is the lift personalization, or just a better *fixed* collection order?" Remove all 13
collection-identity features and NDCG@5 goes **0.7157 → 0.7161** — it does not drop.
Spearman(full, ablated) 0.9735. The lift is personalization.

---

## Act 3 · BATCH — what Crunchyroll ships in November (12 min) ⭐

**This is the act that matters most. Lead with it, not with the endpoint.**

Open `26_batch_scoring.py`. Frame it:

> Lakebase is not in GCP us-west1 until November, so your deliverable is batch. This
> notebook is the batch path, and there is no online store anywhere in it.

**Show the resolve:** `@champion` → concrete version 10. The *same* version the endpoint
serves. This is deliberate — if batch and online scored different versions the comparison
at the end of the notebook would be meaningless.

**Show the one call that is the whole story:**

```python
scored = fe.score_batch(model_uri=MODEL_URI, df=scoring_sdf)
```

No feature values are passed in. The model carries its own feature spec, so this call
resolves 4 feature tables and 5 UC Python UDFs against the **offline** store. Same spec,
unchanged, that the endpoint uses against Lakebase.

**Measured, this workspace:**

| | measured |
|---|---|
| rows scored | **4,628** (300 viewers × 14–16 eligible collections, mean 15.4) |
| `score_batch` + materialisation | **69.9 s** |
| write of the ranked table | **2.9 s** |
| output | `rail_rankings_batch`, 4,628 rows, `model_version` 10 |

**State the limit before they do:** 69.9 s for 4,628 rows is dominated by Spark startup on
serverless, not per-row work. It does **not** extrapolate. What transfers is the *shape*:
a full refresh is `O(viewers × eligible collections)`; an incremental refresh is
`O(viewers whose features changed)`. At a minutes cadence only the second is viable.

**Then the incremental mode**, which is how a minutes cadence is possible:

Change Data Feed is enabled on all four feature tables, so "whose features moved since the
last run" is a query, not a guess. `make batch-incremental` reads each table's change feed
from the version the last run recorded, collects the affected `viewer_id`s, rescores only
those, and writes with `replaceWhere` so the rest of the table stands.

One honest asymmetry to volunteer: `rail_features` is keyed by `rail_id`, so **any** change
to it moves every viewer's ordering and forces a full refresh. The notebook detects that
and says so rather than shipping a half-stale table.

> ⚠️ **See "Known gaps" below before you demo incremental live.** The CDF path was fixed
> today and has not yet been executed end to end on this workspace.

**Close the act on the handoff question, because they will ask it:** how their homepage
service consumes `rail_rankings_batch` — query the Delta table directly, or populate their
own cache from it — is theirs to decide, and it changes the freshness contract. Worth
settling before November.

---

## Act 4 · ONLINE — where they are going (10 min)

**Open the app.** Left sidebar: viewer, device, hour, clock.

**Beat 1 — the collection order.** Pick viewer `v0001`. Show the ranked collections, the
probability per collection, and the incumbent editorial order beside it.

**Beat 2 — what the caller actually sends.** This is the slide-worthy fact:

> **7 request fields in. 45 feature values resolved server-side**, across 4 feature tables
> and 5 UDFs. The model scores on 47 features — 43 of those retrieved values plus the 4
> context fields the caller sends.

The homepage service does not fetch features. It does not know which tables exist. It
sends a viewer, a context and the eligible collections. Everything else is the endpoint's
job, because the feature spec is logged *inside* the model.

**Beat 3 — the online row it looked up.** The app shows the actual `online_viewer_rail`
row behind the score. Composite key (viewer × collection), read from Lakebase inside the
request.

**Beat 4 — eligibility is a filter, never a feature.** Four collections depend on viewer
state (Continue Watching, Watchlist, Because You Watched, New Episodes), so the eligible
set genuinely varies 14–16 per request. It is applied *before* scoring, exactly as their
service would.

**Beat 5 — context changes the answer.** Flip hour 09:00 ↔ 21:00 and device mobile ↔ TV.
Nothing in the feature store changes. **5 to 12 of 16 collections move** — read the count
off the app rather than quoting a fixed number, because two request-time features decay with
the clock and the spread moves between runs. (Re-verified live
today with four direct endpoint calls.)

---

## Act 5 · The proof that batch and online agree (5 min) ⭐

**This is the closing argument of the whole POC.** Back to notebook 26, last section.

Same viewer. Same collections. Same context. Same model version. Scored both ways:

| | result |
|---|---|
| collections compared | **16** |
| identical rank | **16 of 16** |
| Spearman(batch, online) | **1.0** |
| max absolute probability difference | **0.0026** |

**Volunteer the explanation for the 0.0026 — do not let them find it.** It is not noise
and not a bug. `cr_rail_click_recency` takes `request_epoch_s` as an input, and the batch
job and the endpoint call happened seconds apart. A time-decay feature *should* return a
different value at a different clock. Ranks are unaffected because every collection in the
request shifts by the same clock. **If this number were zero, the recency feature would
not be doing its job.**

**Then the migration, concretely:**

```
Phase 1 — now, GCP us-west1, no online store
  notebooks 00,01     feature engineering       → Delta feature tables in UC
  notebooks 20,21     collection features       → Delta feature tables in UC
  notebook  22        PIT training set + model  → registered in Unity Catalog
  notebook  26        score_batch               → rail_rankings_batch (Delta)
                      make batch-incremental    → minutes cadence via CDF

Phase 2 — when Lakebase lands in us-west1
  notebook  21        publish_table(...)        → the same tables, published online
  notebook  23        deploy the same version   → crunchyroll-rail-ranker endpoint
```

Nothing in the feature definitions, the training set or the model changes. **The version
that was scoring in batch is the version that gets served.**

**And be honest about why Phase 2 is worth doing.** It is *not* primarily latency. It is
the up-to-9-of-16 movement: personalization that a precomputed table structurally cannot
deliver, because 5 of the model's features read `device`, `hour_of_day` and
`request_epoch_s`, none of which exist until a request arrives. Precomputing every context
multiplies the table by the number of contexts — the expensive direction at a minutes
cadence.

---

## Act 6 · Production serving numbers (7 min)

Full tables in `docs/serving_benchmark.md`. In-region job, model v10, `scale_to_zero=false`,
concurrency 4–32, route optimization off (this workspace rejected it).

| question | measured |
|---|---|
| latency, low concurrency | **p50 52 ms, p95 67 ms** (12 collections) |
| latency vs collections per request | **flat 4 → 32** — lookups are batched, not serial |
| feature lookups + UDFs alone | p50 **31–35 ms** (Feature Serving endpoint) |
| model + ranking share | **~17 ms** |
| behaviour past capacity | **HTTP 429**, not unbounded queueing |
| spike 2 → 48 concurrent | p50 153 ms, p95 294 ms, **8,770 of 13,988 rejected**, 207 req/s served |
| recovery after the spike | immediate — p50 54 ms, p95 69 ms, 4 residual 429s |

**The fanout result is the most useful thing here for them:** 32 collections cost the same
as 4. Three independent runs agree, including one from a completely different network
position. **A richer homepage is close to free at the ranking layer** — 12 → 30 collections
costs no latency, only a bigger response body.

**Be straight about throughput, because it is not one number.** At concurrency 32 we
measured **~80 req/s** in a closed-loop ramp (p50 369 ms, no rejections) and **~206 req/s**
under open-loop overload (p50 71 ms, excess shed as 429). Same endpoint, same config,
2.6× apart. A 10-minute sustained hold showed **no scale-up at all** (factor 1.0), so
"capacity arriving over minutes" is *not* supported by the current run and we are not
claiming it. What survives regardless:

> **Provision the floor for peak. Do not size against autoscaling.**
> `min_provisioned_concurrency` is what you actually get.

**Two vantage points, deliberately.** A laptop saturates at 53.8 req/s and therefore sees
**zero** 429s in the same spike that rejects thousands in region. Benchmarking a serving
endpoint from outside its region overstates headroom by roughly 4×.

---

## Act 7 · What we are not claiming (5 min)

Lead with this, do not let them extract it. It is what makes the rest credible.

| gap | status |
|---|---|
| Scale at their cardinality | **not demonstrated.** 4,681 online keys is not 50 million. Top follow-up. |
| Training at their volume | **partial.** The PIT join is Spark and scales; the estimator is `toPandas()` + scikit-learn, single-driver. Substitution named (Spark ML / XGBoost on Spark) and it touches neither the feature layer nor serving. |
| Fallback path | **not built.** No cached previous ranking, no editorial default on timeout, no circuit breaker. Given the endpoint sheds load with 429, **this is the most important thing Crunchyroll must build.** |
| Canary / traffic split | **not built.** Model Serving supports it; this POC sends 100% to one version. |
| `rail_rankings_batch` → homepage handoff | **theirs to decide.** Changes the freshness contract. |
| Minutes cadence at their MAU | **unsized.** Mechanism exists, measured at demo scale only. Needs their MAU and feature-change rate. |
| Route optimization | rejected by this workspace; escalation is ours, not theirs. |

**Then the split they asked for** — what Databricks provides vs what they build vs what
they operate (`vertical_ranking.md` §5):

* **Provided:** feature plumbing, the PIT join, the online store, automatic feature lookup
  at serving, the registry, the endpoint, batch scoring against the offline store.
* **They build:** feature definitions, **the homepage impression log including rendered
  position** (without it there is no vertical ranking at all), eligibility rules, candidate
  resolution for personalized collections, the homepage service and its fallbacks.
* **They operate:** freshness, capacity, retraining cadence, cost.

---

## Known gaps in the demo itself — read before you present

1. **The batch path has no UI.** The app shows the online path only. The batch story lives
   in notebook 26's output and in a SQL query against `rail_rankings_batch`. Have that
   query open in a tab before the call:
   ```sql
   SELECT viewer_id, rail_id, rail_rank, engagement_probability, model_version
   FROM serverless_lakebase_praneeth_catalog.crunchyroll_demo.rail_rankings_batch
   WHERE viewer_id = 'v0001' ORDER BY rail_rank
   ```
2. **`make batch-incremental` has never completed a run.** Two defects were found and fixed
   today: Delta's CDF `startingVersion` is **inclusive**, so reading from the last-scored
   version re-reported that run's own writes and forced a full refresh every time; and an
   empty changed-viewer list was treated as "all viewers". Both fixed, neither yet
   exercised. **Either validate it before the call or describe the mechanism from the code
   without running it live.**
3. **Nothing has changed in the feature tables since the last batch run**, so an
   incremental run right now would correctly report "no viewers changed" and touch nothing.
   To demo real incremental work, a feature table has to move first.
4. **`route_optimized` is off** and this workspace rejects it. If they ask about the lowest
   achievable latency, that is the lever we could not pull here — not a platform limit.

---

## Questions to expect, with the answer ready

**"Can we serve both rankings from one endpoint?"**
Different grains, different request shapes, different labels — two endpoints. What is
shared is the feature layer, which is where the duplication cost would otherwise be.

**"How fresh are the features at serving time?"**
Three cadences, deliberately: `session_features_current` publishes CONTINUOUS (streaming),
`recent_behavior_current` TRIGGERED, the rest daily. `make burst` demonstrates an event
changing the next ranking. Freshness is a per-table decision, not a global one.

**"What happens if the online store is down?"**
Today: the request fails. That is the fallback gap in Act 7 and we are not pretending
otherwise. In Phase 1 the question does not arise — batch has no online dependency, which
is a genuine argument for starting there.

**"Why is the online store the only always-on cost?"**
A store that sleeps cannot answer a keyed read in single-digit milliseconds. Measured
**$15.95/day at CU_2** list; this demo runs CU_1. The rail-ranker endpoint also has
`scale_to_zero=false` **for the demo**, so latency numbers are not first-request numbers —
that is a demo-day choice, not a requirement.

**"Is 45 or 47 features the right number?"**
Both, and they mean different things. **45** feature values are resolved server-side; the
model scores on **47** = 43 of those retrieved values + the 4 context fields the caller
sends. Two of the retrieved values are UDF inputs only and never reach the estimator.
