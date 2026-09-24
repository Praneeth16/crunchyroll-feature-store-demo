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
| Feature Engineering | met | `notebooks/20_vertical/21_rail_features.py`, `src/crfs/rails.py` |
| Feature Store | met | 7 UC feature tables, all published to Lakebase; `verify.sh` asserts row counts and dedup |
| Model Training | met | `notebooks/20_vertical/22_train_rail_ranker.py`, trained from `fe.create_training_set` |
| Model Registration | met | UC model `crunchyroll_rail_ranker` v12, `@champion`, tagged and described (2026-09-24 run) |
| Model Serving | met | `crunchyroll-rail-ranker`, READY, serving v12 (`rail_ranker-12`), `scale_to_zero=false`, concurrency 4–32 |
| Ranked Rails | met | `notebooks/20_vertical/24_homepage_assembly.py` — 14–16 eligible rails per viewer (mean 15.4), ranked in one call |

All five stages run from one command (`make up`), on a workspace whose ids are
discovered rather than hardcoded.

---

## Ask 1 — Shared Feature Store

> User, title, rail and contextual features that can be reused across Horizontal and Vertical Ranking models, including offline/online availability and training-serving consistency.

| Element of the ask | Status | Evidence |
|---|---|---|
| **User** features | met | `viewer_features_current`, `recent_behavior_current` — both read by **both** rankers, unchanged, one pipeline |
| **Title** features | met | `title_features` is looked up directly by the horizontal ranker and **aggregated to rail grain** for the vertical one (`rail_avg_popularity`, `rail_avg_rating`, `rail_content_age_days`, `rail_simulcast_share`). A rail is not a title, so aggregation is the correct reuse shape. This read the raw `titles` table until an audit caught it — see `verification_log.md` V57 |
| **Rail** features | met | `rail_features` (rail grain) and `viewer_rail_features_ts` (viewer × rail grain) |
| **Contextual** features | met | 5 request-time UC Python UDFs, **2 of them shared** with the horizontal ranker |
| Reused across both models | met | `notebooks/24` resolves the overlap from Unity Catalog at runtime and prints it — it cannot drift from this document |
| Offline availability | met | every feature table has an offline Delta table; `viewer_rail_features_ts` keeps **every daily snapshot** for point-in-time joins |
| Online availability | met | all 7 published to Lakebase; `verify.sh` asserts `online_viewer_rail` holds exactly one row per key (4,681 rows from 463,419 offline) |
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
| Version management | met | integer versions, `@champion` alias, three tags incl. `ndcg5_lift_vs_editorial=+0.0570`, full description, lineage to `run_id` |
| Deployment | met | notebook 23 resolves `@champion` → version 11 and pins the **immutable version**; promotion and deployment stay two separate steps |
| Rollback | met | set the `model_version` widget to a previous version and rerun; in-place update, no rebuild |
| Training at production volume | **partial** | the PIT join is Spark and scales; the **estimator does not** — `toPandas()` + scikit-learn is single-driver. Documented, with the substitution named (Spark ML / XGBoost on Spark from `load_df()`), and it does not touch the feature layer or serving path |
| Canary / traffic splitting | met, with a gate | `notebooks/30_advanced/33_canary_gate.py` (`make canary`): challenger at 10%, paired per-entity error / p95 / Spearman checks, PROMOTE or ROLLBACK recorded in `canary_decisions`, champion restored in a `finally`. First run **rolled back** the GPU challenger, which failed 40/40 requests when served: its signature required the point-in-time `ts` column. Fixed in notebook 32; the retrained v6 then **passed** the gate (0/40 errors, p95 139 ms, Spearman 0.61). No online-quality metric yet — `docs/canary.md` |

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
| Context actually changes the answer | met | notebook 24 and the app score the same viewer at 09:00/21:00 × TV/mobile; **5 to 12 of 16 rails move** between contexts (live, 2026-09-18, two runs against the 21:00 TV order: 09:00 TV 6 then 5, 21:00 mobile 12 then 7, 09:00 mobile 9 both times) with nothing in the feature store changing. A zero would fail the run loudly |
| Personalization is the source of the lift | met | ablation removing all 13 rail-identity features loses nothing (v12: NDCG@5 0.6978 → 0.7067; v11: 0.6984 → 0.7026), across three independent runs |

---

## Ask 4 — Production serving

> Expected end-to-end latency, concurrency/throughput characteristics, autoscaling behavior, and how the system performs under traffic spikes.

| Element of the ask | Status | Measured |
|---|---|---|
| End-to-end latency | met | **p50 51 ms / p95 66 ms** in region, 12 rails (v12, 2026-09-24); decomposed into ~34 ms feature layer + ~17 ms model |
| Latency vs request size | met | **flat 4 → 32 rails** — lookups are batched, not serial |
| Concurrency / throughput | met | **not a single number**: at concurrency 32, ~80 req/s while the endpoint queues, ~200–218 req/s admitted once it sheds the excess as 429. A 2.4–2.6x spread on identical config; the mechanism is not cleanly established — see `vertical_ranking.md` §4 |
| Behaviour past the ceiling | met | **HTTP 429**, not unbounded queueing — undocumented publicly, so worth having measured |
| Autoscaling behaviour | met, and the finding is negative | scale-up is **neither instant nor predictable** — the latest run reached 90% of best throughput after **~60 s** of sustained load (2.4x), the run before saw no scale-up at all, an earlier pair of sweeps needed ten minutes, and a 12-second burst never gets new capacity. **Provision the floor for peak; do not rely on scale-up** |
| Traffic spikes | met | 2 → 48 concurrent: p50 103 ms, p95 255 ms, **10,157 of 15,395 rejected**, 208 req/s served; recovery to baseline **immediate** (p50 49.5 ms, p95 61 ms) with 15 residual 429s (v12, 2026-09-24) |
| Cold start | n/a by design | `scale_to_zero=false`, so there is none. The 1,454 ms first request from a laptop is TLS + first OAuth token fetch, client-side |
| Scalability at real cardinality | **not met** | 4,681 online keys is not 50 million. Named as the top follow-up |
| Fallback path | met, in the reference homepage service | timeout budget (300 ms) + circuit breaker per endpoint; rails fall back to the viewer's last good order (filtered to the current eligible set), then editorial; every response names its tier. Demonstrable from the app. `docs/homepage_service.md` |
| End-to-end homepage latency (both rankers + online rows) | met | **94–119 ms p50 / 142–216 ms p95** server-side in region over three runs, every request served by both models each time — `scripts/bench_app.py`. The slowest run was the one straight after the 2026-09-24 fresh deploy. The previous app spent ~2.4 s per view in two SQL-warehouse statements before calling either model |

Two vantage points are reported (`make bench` in region, `make bench-local` from a
laptop) because a laptop saturates at 53.8 req/s and therefore sees **zero** 429s in
the same spike that rejects 10,097 in region (10,157 on 2026-09-24) — i.e. benchmarking from outside the
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

## Added after the ask: the batch path

Crunchyroll's November deliverable turned out to be **batch**, because Lakebase is not yet
available in their region (GCP us-west1), with real-time as the end goal. That is not in
the original document, so it is recorded here as an addition rather than an ask item.

| Element | Status | Evidence |
|---|---|---|
| Batch scoring from the same feature store | met | `notebooks/20_vertical/26_batch_scoring.py`, `fe.score_batch` against the **offline** store — no online store involved |
| Same model serves both paths | met | v11 `@champion` scored both ways (measured 2026-09-18; not rerun against v12); **16 of 16 collections at identical rank**, Spearman 1.0 |
| Minutes-level refresh cadence | met, at demo scale | CDF-driven incremental mode; `make batch-incremental`. Unsized at their MAU |
| What batch gives up | measured | **up to 12 of 16 collections** move across four contexts (5-12 across runs) — personalization a precomputed table cannot deliver |
| Migration path documented | met | `docs/batch_and_online.md` — what changes is one API call and where features are read from |

---

## Nothing in the ask is unaddressed

Every bullet in the ask has a row above. Of the three rows first marked **not met**,
the fallback path is now built (`docs/homepage_service.md`) and traffic splitting has a
gate around it (`docs/canary.md`); the scale test at real cardinality remains a
deliberate scope decision stated in `vertical_ranking.md` §6.

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
* **A verification log.** `docs/verification_log.md` records 66 checks, and most of them are
  defects that only a live run surfaced — including several corrections to earlier
  claims.

---

## Added after the readout: the four follow-up questions

The team came back with three requests for boilerplate and one design question. Same
standard as everything above: each row names the artifact that proves it, and anything
that stands on a Public Preview API says so.

| Element of the ask | Status | Evidence |
|---|---|---|
| **How to use Feature Views for training** | met | `notebooks/30_advanced/30_feature_views.py` + `src/crfs/feature_views.py`. Seven viewer aggregates declared, registered in UC, and `fe.create_training_set(df=labels, features=[...])` — **no table name and no join in the training code**. `make feature-views` |
| **Job for GPU based training** | met | `resources/jobs_advanced.yml` → `crfs_gpu_train`: a notebook task with `compute.hardware_accelerator: GPU_1xA10` and a paired `environment_key`. `make gpu-train` |
| **AI Runtime for serverless GPU** | met | `notebooks/30_advanced/32_gpu_train.py` trains through `serverless_gpu.distributed`; `ai/train.yaml` + `ai/train_entrypoint.py` submit the **same module** from a laptop with the `air` CLI. Measured here: NVIDIA A10G, 23 GB, torch 2.7.1+cu126 |
| **Decoupling a feature-definition change between training and inference** | met, and the measurement corrected our expectation | `notebooks/30_advanced/31_feature_versioning.py` measures it against the live endpoint. Table lookups are **pinned** inside the model version, as expected. On-demand UC functions turned out to be pinned in practice too: redefining one changed nothing over five minutes of polling (0/16 rails, delta 0.000000), so they are resolved at deploy rather than per request. `docs/feature_versioning.md` records both the result and the limits of concluding from one endpoint |
| **Versioning for feature definitions** | met, as a discipline rather than a platform feature | Neither feature tables nor Feature Views carry a version number. What exists: the feature spec inside each model version, plus two tags this repo adds (`feature_spec_hash`, `feature_definition_fingerprint`) and `src/crfs/versioning.py` to read, fingerprint and diff them. Version **by name**, additively |
| A repo the team can navigate | met | `QUICKSTART.md`, `docs/README.md` as an index, notebooks grouped into seven tracks, and a README that is a landing page rather than a manual |
| One-click deploy with DABs | met | `make deploy` is the whole deploy — jobs, volume, dashboard **and the app**, which became possible on CLI v1.17.0 (`docs/risks.md` §8b). `./setup.sh --profile <P>` still does empty-workspace-to-demo in one command |

### What we did not do, and why

* **The pipeline was not migrated to Feature Views.** The DSL covers the viewer-grain
  aggregates and, with `CustomUDF` / `RowTransformation` / `FeatureViewSource` chaining,
  more than the published limitations suggest — but `rail_features`,
  `viewer_rail_features_ts` and the propensity model are procedural, and rewriting a
  working feature table to prove a point is not an improvement.
  `docs/feature_views.md` has the feature-by-feature table.
* **Neither preview is available in GCP us-west1.** Feature Views and AI Runtime are both
  AWS-region-limited today, which is the same constraint as Lakebase. For Crunchyroll's
  own region these are roadmap, and `make probe` is how any workspace answers the question
  for itself.
* **The GPU model was not promoted.** It is registered and aliased `@challenger`, not
  `@champion`. It exists to show that the estimator can be swapped without touching the
  feature layer or the serving contract, not to claim a better ranker on synthetic labels.
