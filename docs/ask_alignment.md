# Alignment check against the Crunchyroll ask

A line-by-line audit of ["Write-up — Vertical Ranking Demo Ask"](https://docs.google.com/document/d/1HwZEDqRA_PNzRPSZbQS0sxZuxyItzSoUgl2rxxfhdLU/edit)
against what the POC actually does. Every **met** row names the artifact that proves
it, so any row can be checked rather than taken on trust.

Status vocabulary is deliberately narrow: **met** = built and verified in a live run;
**partial** = built, with a named limitation; **not met** = absent, and said so.

---

## The lifecycle the ask names

> Feature Engineering → Feature Store → Model Training/Registration → Model Serving → Ranked Rails

| Stage | Status | Where |
|---|---|---|
| Feature Engineering | met | `notebooks/21_rail_features.py`, `src/crfs/rails.py` |
| Feature Store | met | 7 UC feature tables, all published to Lakebase; `verify.sh` asserts row counts and dedup |
| Model Training | met | `notebooks/22_train_rail_ranker.py`, trained from `fe.create_training_set` |
| Model Registration | met | UC model `crunchyroll_rail_ranker` v9, `@champion`, tagged and described |
| Model Serving | met | `crunchyroll-rail-ranker`, READY, serving v9, `scale_to_zero=false`, concurrency 4–32 |
| Ranked Rails | met | `notebooks/24_homepage_assembly.py` — 15 eligible rails ranked in one 151 ms call |

All five stages run from one command (`make up`), on a workspace whose ids are
discovered rather than hardcoded.

---

## Ask 1 — Shared Feature Store

> User, title, rail and contextual features that can be reused across Horizontal and Vertical Ranking models, including offline/online availability and training-serving consistency.

| Element of the ask | Status | Evidence |
|---|---|---|
| **User** features | met | `viewer_features_current`, `recent_behavior_current` — both read by **both** rankers, unchanged, one pipeline |
| **Title** features | **partial** | `title_features` is consumed directly by the horizontal ranker. The vertical ranker uses title content aggregated to rail grain, which is the correct shape for a rail-grain model — **but it reads the raw `titles` table, not the `title_features` feature table**, so that signal is shared at source-data level rather than through the feature store. `title_features` has a governed analogue for all four stats, so this is a fixable gap, not a design limit. See `vertical_ranking.md` §1 |
| **Rail** features | met | `rail_features` (rail grain) and `viewer_rail_features_ts` (viewer × rail grain) |
| **Contextual** features | met | 5 request-time UC Python UDFs, **2 of them shared** with the horizontal ranker |
| Reused across both models | met | `notebooks/24` resolves the overlap from Unity Catalog at runtime and prints it — it cannot drift from this document |
| Offline availability | met | every feature table has an offline Delta table; `viewer_rail_features_ts` keeps **every daily snapshot** for point-in-time joins |
| Online availability | met | all 7 published to Lakebase; `verify.sh` asserts `online_viewer_rail` holds exactly one row per key (4,681 rows from 421,290 offline) |
| Training-serving consistency | met, structurally | the feature spec is logged **inside** the model, so the endpoint performs the same lookups and the same UDFs as training. Not a convention — there is no second code path to keep in sync |

**Cost of adding a second ranking model at a new grain: two feature tables and three
UDFs.** Nothing forked, nothing copied, no private per-model copy of a viewer feature.

---

## Ask 2 — Model lifecycle

> Building the training dataset from stored features, model/version management, and deployment.

| Element of the ask | Status | Evidence |
|---|---|---|
| Training dataset from stored features | met | `fe.create_training_set` with 4 `FeatureLookup`s and 5 `FeatureFunction`s; **no hand-written join** |
| Point-in-time correctness | met | `timestamp_lookup_key` against `viewer_rail_features_ts`; verified with an isolated probe job before the architecture was chosen |
| Model management | met | registered in **Unity Catalog** (not the workspace registry), so it is a securable with grants and lineage |
| Version management | met | integer versions, `@champion` alias, three tags incl. `ndcg5_lift_vs_editorial=+0.0514`, full description, lineage to `run_id` |
| Deployment | met | notebook 23 resolves `@champion` → version 9 and pins the **immutable version**; promotion and deployment stay two separate steps |
| Rollback | met | set the `model_version` widget to a previous version and rerun; in-place update, no rebuild |
| Training at production volume | **partial** | the PIT join is Spark and scales; the **estimator does not** — `toPandas()` + scikit-learn is single-driver. Documented, with the substitution named (Spark ML / XGBoost on Spark from `load_df()`), and it does not touch the feature layer or serving path |
| Canary / traffic splitting | **not met** | 100% of traffic goes to one version. Model Serving supports splitting; this POC does not use it |

---

## Ask 3 — Online inference

> A serving endpoint that takes a user, context and eligible rails and returns personalized rail rankings, including how online features are retrieved at inference time.

| Element of the ask | Status | Evidence |
|---|---|---|
| Takes a user | met | `viewer_id` in the request |
| Takes context | met | `device`, `locale`, `hour_of_day`, `day_of_week`, `request_epoch_s` |
| Takes eligible rails | met | one row per candidate rail; eligibility is a **hard filter applied before scoring**, never a feature |
| Returns personalized rankings | met | `rail_id` / `engagement_probability` / `rail_rank`, ranked within `viewer_id` |
| **How online features are retrieved** | met | the endpoint does it, not the caller: **7 request fields in, 45 feature values resolved server-side** across 4 tables and 5 UDFs (the model scores on 47 features: 43 of those retrieved values plus the 4 context fields the caller sends) |
| Context actually changes the answer | met | notebook 24 scores the same viewer at 09:00/21:00 × TV/mobile; **9 of 15 rails move** between contexts with nothing in the feature store changing. A zero would fail the run loudly |
| Personalization is the source of the lift | met | ablation removing all 13 rail-identity features loses nothing (NDCG@5 0.7140 → 0.7169), across two independent runs |

---

## Ask 4 — Production serving

> Expected end-to-end latency, concurrency/throughput characteristics, autoscaling behavior, and how the system performs under traffic spikes.

| Element of the ask | Status | Measured |
|---|---|---|
| End-to-end latency | met | **p50 52 ms / p95 64 ms** in region, 12 rails; decomposed into ~35 ms feature layer + ~17 ms model |
| Latency vs request size | met | **flat 4 → 32 rails** — lookups are batched, not serial |
| Concurrency / throughput | met | **not a single number**: ~80 req/s shortly after a version update, ~212 req/s once scaled — a 2.6x spread on identical config |
| Behaviour past the ceiling | met | **HTTP 429**, not unbounded queueing — undocumented publicly, so worth having measured |
| Autoscaling behaviour | met, and the finding is negative | scale-up takes **minutes, not seconds** — a 12-second burst gets no new capacity, while ten minutes of load bought 2.6x throughput. **Provision the floor for peak; do not rely on scale-up** |
| Traffic spikes | met | 2 → 48 concurrent: p50 178 ms, p95 383 ms, **5,852 of 11,053 rejected**, 206 req/s served; recovery to baseline **immediate**, zero errors |
| Cold start | n/a by design | `scale_to_zero=false`, so there is none. The 1,454 ms first request from a laptop is TLS + first OAuth token fetch, client-side |
| Scalability at real cardinality | **not met** | 4,681 online keys is not 50 million. Named as the top follow-up |
| Fallback path | **not met** | no cached previous ranking, no editorial default on timeout, no circuit breaker. Given that the endpoint sheds load with 429, this is the most important thing Crunchyroll must build |

Two vantage points are reported (`make bench` in region, `make bench-local` from a
laptop) because a laptop saturates at 53.8 req/s and therefore sees **zero** 429s in
the same spike that rejects 10,097 in region — i.e. benchmarking from outside the
region overstates headroom by roughly 4×.

---

## What we want to learn

> How much of this workflow Databricks can provide out of the box, what Crunchyroll would need to build/operate.

met — `vertical_ranking.md` §5 splits this three ways (provided / build / operate) with
no hedging. Short version: the feature plumbing, the PIT join, the online store, the
automatic lookup at serving, the registry and the endpoint are provided. Crunchyroll
builds feature definitions, **the homepage impression log including rendered position**,
eligibility rules, candidate resolution for personalized rails, the homepage service and
its fallbacks. Crunchyroll operates freshness, capacity, retraining cadence and cost.

> Whether the proposed approach can meet the latency, concurrency and scalability requirements of homepage ranking.

Answered directly in `vertical_ranking.md` §0: **latency yes with room; concurrency yes
but provisioned for peak rather than autoscaled into; scalability not yet demonstrated at
Crunchyroll's cardinality or training volume** — with both gaps measurable in a follow-up
rather than being design flaws.

> Synthetic or representative data/modeling is sufficient.

Honoured — 300 viewers, 132 titles, 363k rail impressions, generated in-pipeline. The POC
says explicitly that the NDCG lift is evidence the pipeline works and **not** a forecast
of Crunchyroll's lift.

---

## Nothing in the ask is unaddressed

Every bullet in the ask has a row above. The three **not met** rows — traffic
splitting, a scale test at real cardinality, and a fallback path — are deliberate
scope decisions stated in `vertical_ranking.md` §6, not oversights.

## Where we went beyond the ask

Not padding — each one exists because the ask could not be answered honestly without it.

* **Position-bias correction (IPS).** The ask says the ranking algorithm is not the
  point. But every label in a homepage log was observed at a position the incumbent
  chose, so an uncorrected model learns the incumbent and scores well for it. Without
  this the reported lift would be an artifact.
* **A load-test harness, not a latency screenshot.** "Expected latency" is unanswerable
  without stating concurrency, so `src/crfs/loadtest.py` reports percentiles, achieved
  throughput and error codes together, from two vantage points.
* **An ablation.** "Is the lift personalization or a better fixed rail order" is the
  question a ranking team will ask, and only an ablation answers it.
* **A verification log.** `docs/verification_log.md` records 38 checks, 20 of which are
  defects that only a live run surfaced — including four corrections to our own earlier
  claims.
