# Vertical Ranking on Databricks — what the POC shows, and what it does not

This answers the four things the Crunchyroll DSML ask named — a shared feature
store, model lifecycle, online inference, and production serving — plus the
question underneath them: **how much of this is out of the box, and how much would
Crunchyroll build and operate?**

Everything marked *measured* comes with the command that produced it. Anything not
yet measured says so. The serving numbers live in
[`serving_benchmark.md`](serving_benchmark.md), written by the benchmark job rather
than by hand.

---

## 0 · The shape of it

Two ranking models, one feature layer.

```
                    ┌─────────────────── Feature Engineering in Unity Catalog ──────────────────┐
engagement_events ──┤ viewer_features_current   ▸ viewer_id                                      │
rail_impressions ───┤ recent_behavior_current   ▸ viewer_id      SHARED, unchanged, one pipeline │
titles ─────────────┤ session_features_current  ▸ viewer_id      ───────────────────────────────  │
rails ──────────────┤ title_features            ▸ title_id                                       │
                    │ rail_features             ▸ rail_id            new for vertical            │
                    │ viewer_rail_features_ts   ▸ viewer_id+rail_id  new for vertical            │
                    │   ├── offline: every daily snapshot  → point-in-time training              │
                    │   └── online:  latest row per key    → keyed reads at request time         │
                    │ 5 request-time UC Python UDFs (2 of them shared)                           │
                    └────────────────────────┬─────────────────────────────────────────────────┘
                                             │ published (TRIGGERED / CONTINUOUS)
                              ┌──────────────▼──────────────┐
                              │  Online Feature Store        │  one Lakebase Postgres project
                              │  (Lakebase)                  │  one always-on capacity bill
                              └───────┬──────────────┬───────┘
                automatic feature     │              │    automatic feature lookup
                lookup                │              │
                    ┌─────────────────▼───┐    ┌─────▼──────────────────┐
                    │ crunchyroll-rail-    │    │ crunchyroll-watch-next-│
                    │ ranker  (VERTICAL)   │    │ ranker  (HORIZONTAL)   │
                    │ which rails, ordered │    │ which titles, ordered  │
                    │ no scale-to-zero     │    │ demo config            │
                    │ concurrency 4-32     │    │                        │
                    └──────────┬───────────┘    └───────────┬────────────┘
                               └──────────► homepage ◄──────┘
```

The homepage is one vertical call plus one horizontal call per rendered rail. The
horizontal calls are independent and issue in parallel, so wall time is the
vertical call plus the slowest horizontal one — not their sum.

### The direct answer to the closing question

The ask ends by asking whether this approach can meet the latency, concurrency and
scalability requirements of homepage ranking. Taking those one at a time, against
measurements rather than positioning:

* **Latency — yes, with room.** p50 **52 ms** in region for a 12-rail request, p95
  **64 ms**, and **flat from 4 to 32 rails**. About 35 ms of that is the feature layer
  and 17 ms the model. A homepage budget in the low hundreds of milliseconds has room
  for this plus the horizontal call plus Crunchyroll's own service hops.
* **Concurrency — yes, but you provision for peak; you do not autoscale into it.**
  Measured on one configuration (provisioned concurrency 4–32), the same endpoint
  served **~80 req/s shortly after a version update and ~212 req/s once it had spent
  ten minutes under load** — a 2.6× difference from scale-up alone. Past available
  capacity it returns **429** rather than queueing. So `min_provisioned_concurrency`
  must be sized for peak, a retry/fallback path is mandatory rather than optional, and
  **no single throughput number describes this endpoint** without stating how long it
  had been warm.
* **Scalability — not demonstrated at Crunchyroll's scale, and two specific things
  would have to change.** The measured ceiling here is a *configuration* ceiling, not
  a platform one; the sizing rule is `concurrency ≈ QPS × execution_seconds`, so at
  52 ms each unit buys roughly 19 req/s, and public documentation puts Model Serving
  over 25K QPS. What this POC has **not** shown is (a) online-store read latency at
  real cardinality — 4,681 keys is not 50 million — and (b) training at real volume,
  where `toPandas()` + scikit-learn must be replaced by a Spark estimator. Neither
  changes the architecture; both need measuring before a commitment.

**The honest summary: the architecture is right and the request path is fast enough.
The two open risks are online-store cardinality and training volume, and both are
measurable in a follow-up rather than being design flaws.**

---

## 1 · Shared feature store

**The ask:** user, title, rail and contextual features reusable across horizontal
and vertical ranking, with offline/online availability and training-serving
consistency.

### What is actually shared

| Feature table | Grain | Vertical | Horizontal | Online |
|---|---|---|---|---|
| `viewer_features_current` | viewer | ✅ | ✅ | ✅ |
| `recent_behavior_current` | viewer | ✅ | ✅ | ✅ |
| `session_features_current` | viewer | available | ✅ | ✅ CONTINUOUS |
| `title_features` | title | ✅ aggregated to rail grain | ✅ | ✅ |
| `viewer_embedding_current` | viewer | available | retriever | ✅ |
| `rail_features` | rail | ✅ | — | ✅ |
| `viewer_rail_features_ts` | viewer × rail | ✅ | — | ✅ latest per key |
| `cr_hour_affinity_delta` (UDF) | request | ✅ | ✅ | request-time |
| `cr_session_decay` (UDF) | request | ✅ | ✅ | request-time |
| `cr_rail_taste_match` (UDF) | request | ✅ | — | request-time |
| `cr_rail_click_recency` (UDF) | request | ✅ | — | request-time |
| `cr_device_rail_fit` (UDF) | request | ✅ | — | request-time |

**One row of that table was a caveat until recently, and the fix is worth describing
because it is the difference between real reuse and nominal reuse.** The vertical
ranker's four rail content stats — `rail_avg_popularity`, `rail_avg_rating`,
`rail_content_age_days`, `rail_simulcast_share` — were originally computed from the
**raw `titles` Delta table**. Title signal was therefore shared at source-data level,
not through the feature store, which is materially weaker: those stats inherited none of
the feature table's definitions, and `rail_content_age_days` recomputed content age from
`release_year` against a July-1 approximation while `title_features.days_since_release`
already defined it. Two definitions of one concept is the drift this architecture exists
to remove.

They now aggregate `title_features` — the same table the watch-next ranker looks up. Two
further improvements fell out of it:

* `rail_avg_popularity` now averages the observed `popularity_30d` instead of the
  generator's latent `intrinsic_popularity`. That removes a mild **leakage**: intrinsic
  popularity is a parameter that produced the engagement the model is trained to predict.
  Both columns are on a 0–1 scale, so the change moves the values without changing the
  feature's range.
* `avg_rating` and `is_simulcast` come from the table the other ranker reads, so the two
  models cannot disagree about what those mean.

This is recorded rather than quietly fixed because an earlier version of this document
claimed `title_features` fed these stats when it did not (`verification_log.md` V57). A
rail is still not a title, so aggregating to rail grain remains the correct shape — the
change is *where the title values come from*, not what grain they are used at.

Adding a whole second ranking model at a different grain cost **two feature tables
and three UDFs**. Nothing was forked, nothing was copied, and neither model owns a
private version of a viewer feature.

Notebook 24 prints this overlap at runtime: the **table list** comes from Unity Catalog
(`SHOW TABLES LIKE 'online_*'`) and the **reader mapping** is derived from the
`FeatureLookup` declarations in `src/crfs/config.py` — the same declarations notebooks 02
and 22 train against. Measured there: **2 of 7** published online tables are read by both
rankers.

An earlier version of this document said that overlap was "resolved from Unity Catalog …
so it cannot drift from reality". That was an overstatement worth correcting: only the
table list was resolved, while the mapping that constitutes the sharing claim was a
hand-typed dict inside the reporting notebook. It is now derived from one declaration
instead of two, which removes the drift between the notebook and the trainers — but it is
still a code declaration, **not** the deployed models' own feature specs, so it can drift
if someone retrains with different lookups. The notebook prints which source it used.

### Contextual features: the part that is easy to get wrong

Device, hour of day and day of week do not exist anywhere until a request arrives,
so they cannot be a table. They are **Unity Catalog Python UDFs** registered as
`FeatureFunction`s in the feature spec, and the serving endpoint evaluates them
per request, after its online lookups, inside the same call.

That matters because it is the difference between a feature that is *governed* and
one that lives in application code. `cr_device_rail_fit` is a function in Unity
Catalog with a comment, lineage and a single definition. If Crunchyroll computed
the same cross in the homepage service instead, the training pipeline would have to
reimplement it, and the two would drift — silently, and in production only.

Notebook 24 scores the same viewer in four contexts (21:00/09:00 × TV/mobile) and
prints how many rails move. If that number were zero, this whole layer would be
unjustifiable.

### Training-serving consistency, structurally

Three mechanisms, in increasing order of strength:

1. **One definition, imported once.** Every feature computation lives in
   `src/crfs/features.py` or `src/crfs/rails.py` and is imported by the batch
   build, the triggered recompute and the streaming path. The first version of this
   demo re-derived the recent-behaviour maths in two notebooks — the exact skew the
   architecture argues against, committed in its own source. That is why the shared
   module exists.

2. **The feature spec travels inside the model.** `fe.log_model(training_set=...)`
   embeds the lookups and the UDF bindings in the registered model. The endpoint
   then retrieves features itself. The homepage service sends **seven fields**; **45
   feature values** are resolved server-side (15 viewer + 5 recent-behaviour + 13
   rail + 7 viewer×rail + 5 request-time UDF outputs). A caller cannot send the
   wrong feature because a caller does not send features.

   Two counts appear in this repo and they are **not** the same number, so both are
   reconciled here once: **45** is what the endpoint retrieves or computes, while
   **47** is what the model scores on. The difference is that two retrieved values
   are deliberately not features (`last_event_epoch_s` and `vr_last_click_epoch_s`
   are UDF inputs only, so 43 of the 45 are used) and four features are carried on
   the request rather than looked up (`device`, `locale`, `hour_of_day`,
   `day_of_week`). 43 + 4 = 47. An earlier edit in this project substituted one
   count for the other across three files; both are correct for their own question.

3. **One table for offline and online.** `viewer_rail_features_ts` is a time series
   feature table. Offline it holds every daily snapshot and the training join is
   point-in-time. Published online it **deduplicates to the latest row per
   `(viewer_id, rail_id)`** — verified on this workspace: 16 offline rows across 4
   keys became 4 online rows, and the synced table reported
   `primary_key_columns=[viewer_id, rail_id]`, `timeseries_key=ts`. Training and
   serving read the same table through the same `FeatureLookup`.

> **Recommendation for the horizontal path.** `viewer_features_ts` +
> `viewer_features_current` predates this and keeps two tables where one would do.
> Collapsing it to a single published time series table would remove a
> recompute step and a class of possible divergence. Not done here, because
> rebuilding the working horizontal path was not in scope for this POC — but it is
> the pattern to adopt going forward.

### Point-in-time correctness

`viewer_rail_features_ts` snapshots are stamped at the **end** of the day they
summarise, so an as-of lookup for an impression on day D resolves to the snapshot
built from D−1 and earlier. Stamping at day-start would let a training row read
clicks that happened after it: offline AUC goes up, production performance does
not, and the gap is very hard to find later.

Notebook 22 prints the same viewer × rail key as of an old impression and as of
now, side by side. If those columns matched, the as-of join would be decoration.

---

## 2 · Model lifecycle

**The ask:** building the training dataset from stored features, model and version
management, and deployment.

### Training dataset

`fe.create_training_set(df=labels, feature_lookups=[...])` — labels in, joined
point-in-time feature frame out. Four lookups and five feature functions, declared
once in notebook 22 and reused verbatim for the holdout set. There is no join code
to review, which is the point: a hand-written training join is where skew is
introduced.

**What this step costs, and where it stops scaling.** The as-of join is the expensive
part of training here — not the fit — and it is worth being blunt about it. Measured on
this workspace: joining **363k labels against a 421k-row time series table** and
collecting to pandas twice **did not finish inside a 60-minute task timeout, on two
separate runs.** A point-in-time join is a range join, and range joins degrade with the
product of the two sides.

The demo therefore fits on **25% of homepage sessions** by default
(`label_sample_frac`, sampled by whole session so per-session ranking metrics stay
intact). The feature tables and the propensity table are still built from the full log
— the sample only affects what the model is fit on, and the notebook prints both counts
so the metrics are never read without that context.

At Crunchyroll's scale that shape does not hold. `toPandas()` plus scikit-learn puts
the whole training frame on one driver, and the ceiling arrives long before billions
of impressions. The join itself is fine — it is Spark, and it is the part Databricks
provides. What would change is the estimator: Spark ML, or XGBoost/LightGBM on Spark,
fed from `training_set.load_df()` directly without the collect. That is a
straightforward substitution and it does not touch the feature layer, the feature
spec, or the serving path — which is the useful property of this architecture and
worth stating explicitly rather than leaving as an exercise.

### Position bias — the part a homepage ranker cannot skip

Every label in a homepage log was observed at a position the *incumbent* policy
chose. Rails at the top get engagement because they are at the top. Fit that raw
and the model learns the old policy and reports a good number for doing so.

This POC handles it three ways, and it is worth saying which is which:

* `rail_position_propensity` measures empirical `P(viewport | position)` — measured
  on this data, position 1 ≈ 0.97 falling to ≈ 0.09 at position 16.
* Training weights observed engagements by `1 / P(viewport | position)`, clipped at
  10×. Unclipped inverse propensity on a long tail is unbounded variance.
* **The rendered position is never a feature.** At request time it does not exist —
  it is what the model is computing.

Reported metrics are AUC on all impressions *and* on viewed impressions only. The
viewed-only number is the honest one: engagement on a rail nobody scrolled to
carries no evidence either way.

### Ranking metrics, not just AUC

Nobody ships a homepage because AUC moved 0.004. Notebook 22 evaluates NDCG@3,
NDCG@5 and MRR per homepage session against three baselines — the incumbent
editorial order, global rail CTR, and random — restricted to sessions with at least
one engagement and at least four viewed rails.

Measured on **537 holdout homepage sessions** (81,109 training rows, 9,903 holdout rows),
model version 9:

| Scorer | NDCG@3 | NDCG@5 | MRR |
|---|---|---|---|
| **Vertical ranker** | **0.6151** | **0.7140** | **0.7153** |
| Vertical ranker, no rail-identity features | 0.6209 | 0.7169 | 0.7196 |
| Incumbent editorial order | 0.5618 | 0.6791 | 0.6775 |
| Rail popularity (global CTR) | 0.5707 | 0.6814 | 0.6803 |
| Random | 0.4550 | 0.5956 | 0.5759 |

**+5.1% NDCG@5** against the order the homepage ships today. Holdout AUC is 0.7363 across
all impressions and **0.6347 on viewed impressions only** — the second number is the one
to quote, because engagement on a rail nobody scrolled to is not a preference.

**These numbers are a rerun, and that is the point.** An earlier run of the identical
pipeline on independently regenerated data gave NDCG@5 0.7065 vs 0.6751 incumbent
(+4.7%), AUC-viewed 0.6228, on 511 sessions. This run gives 0.7140 vs 0.6791 (+5.1%),
AUC-viewed 0.6347, on 537 sessions. Different data, same conclusion, same order of
magnitude — which is a **reproducibility** result, and worth more than either run alone.

Read the lift as evidence the pipeline works, **not as a forecast**. The labels come from
a latent utility the model can recover, a few hundred sessions is a small evaluation set,
and the honest measurement is an interleaving or bucket test, which this POC does not have.

**Where the lift comes from — and a result worth pausing on.** The ablation drops all 13
rail-identity features (44 numeric features down to 31), leaving only viewer × rail
history, request context and the on-demand crosses. It does not lose anything: NDCG@5 goes
from 0.7140 to **0.7169**, very slightly *up*, with Spearman 0.9755 between the two
models' scores confirming they genuinely differ. The previous run showed the same thing
(0.7065 → 0.7073, Spearman 0.971).

**So the entire lift is personalization** — not a better fixed order of rails. That
conclusion rests on the ablation, which is a direct measurement of the thing in question,
and it has now held across two independent runs.

Permutation importance, grouped by source, is the weaker evidence and is reported here
with its instability rather than without:

| Source | run A | run B | run C (v9) |
|---|---|---|---|
| `viewer_rail_features_ts` (new) | 0.0514 | 0.0419 | **0.0821** |
| `rail_features` (new) | 0.0488 | 0.0465 | 0.0218 |
| request-time UDFs | 0.0257 | 0.0297 | 0.0134 |
| shared viewer tables (reused) | −0.0024 | +0.0021 | 0.0004 |
| request context | 0.0005 | 0.0006 | −0.0006 |

Runs A and B were two runs of `permutation_importance` at `n_repeats=2` on 8,000 rows
against identically-trained models, and **the ordering of the top two swaps between
them.** Run C, on the rebuilt data, separates them cleanly by nearly 4×. So the honest
statement is that the *ranking* of those two sources is not reliably measurable at this
sample size — do not read "viewer × rail is 4× more important than rail features" from
run C any more than you would read the reverse from run B.

What **is** stable across all three runs, and consistent with the ablation:

* the **two new tables dominate** the signal, together several times any other source;
* the **shared viewer tables sit at approximately zero** for rail ranking, either sign;
* the request-time UDFs contribute real but smaller signal.

The reconciliation still holds and is the useful part: permutation importance here is
measured against **AUC**, a global classification metric, while NDCG measures **ordering
within one session**. Rail-level features are constant per rail, so they help predict how
often a rail is engaged with in general — genuine AUC contribution — but cannot
differentiate rails *for a particular viewer*, because within a session every viewer sees
the same rail-level priors. That is why they score on importance and contribute nothing to
the ablation.

The practical consequence for Crunchyroll is unchanged, because it follows from the
ablation rather than from the importance table: **for vertical ranking the viewer × rail
interaction signal is what moves the homepage order.** Rail-level aggregates are worth
having for calibration and cold start.

**An honest note on the shared tables.** The shared viewer features contribute essentially
nothing to *rail* ranking — −0.0024, +0.0021 and +0.0004 across three runs, i.e.
indistinguishable from zero every time. Sharing paid off **operationally** here (one
pipeline, one online store, one always-on capacity bill, no second copy to keep
consistent), not predictively for this model. A viewer's genre affinity matters much more
for *which title* than for *which row*. Better to say that than to imply every shared
feature earns its place in every model.

**The ablation was wrong the first time, and only a check caught it.** The original version
dropped `rail_editorial_rank` alone and returned an NDCG identical to four decimal places.
That is easy to accept as coincidence. Adding an unrounded comparison and a rank
correlation showed **Spearman 1.0** — the test was inert, because all 13 `rail_features`
columns are constant per rail and with 16 rails the model reconstructs rail identity from
any of the other twelve. Both documents and the registered model's description claimed a
result the ablation did not support, and both were corrected.

### Model and version management

The ask names this explicitly, so here is the whole mechanism rather than a claim that a
registry exists.

Notebook 22 logs with `fe.log_model(registered_model_name=...)`, which registers into
**Unity Catalog** — not the workspace registry. The model is
`serverless_lakebase_praneeth_catalog.crunchyroll_demo.crunchyroll_rail_ranker`, so it is
a UC securable: the same grants, lineage and cross-workspace visibility as a table. Each
training run produces a new integer version; nothing is overwritten.

What the notebook attaches to the version it just created, all of it read back from the
API rather than asserted here:

| | value on the current version |
|---|---|
| version | **9** |
| alias | **`@champion`** |
| `position_bias_correction` | `ips` |
| `ndcg5_lift_vs_editorial` | `+0.0514` — relative, i.e. +5.14% over the incumbent order, not an absolute NDCG delta |
| `serves` | `vertical rail ranking, homepage request path` |
| description | model purpose, the four feature tables it reads, the five UDFs, the IPS weighting, holdout AUC on viewed impressions, NDCG lift, and the ablation result |
| lineage | `run_id db31fcb9f1114c20ad41bd058b6913d5`, with 14 metrics logged against it |

The tags matter more than they look. `ndcg5_lift_vs_editorial` on the version means the
question "which model is in front of the homepage and what did it actually beat" is
answerable from the registry, without opening a notebook or finding the run. That is the
minimum for a model in a request path.

**One API wrinkle worth knowing before you go looking.** Aliases are not returned by the
UC tables-style API for a model version — `GET /api/2.1/unity-catalog/models/<name>/versions/9`
reports `aliases: null` even when an alias is set. They come back from the MLflow UC
endpoint, `GET /api/2.0/mlflow/unity-catalog/registered-models/alias?name=...&alias=champion`,
which correctly resolves `champion → 9`. Checking the wrong one is an easy way to conclude
a promotion silently failed when it did not.

### Deployment, and what the endpoint is pinned to

Notebook 23 resolves `@champion` to a concrete version and serves **that number**:

```
model_version widget blank  →  get_model_version_by_alias(MODEL, "champion")  →  9
served entity name          →  rail_ranker-9
```

This is deliberate and it is the part most worth copying. The endpoint is pinned to an
immutable version, and the alias is used only to *decide* which version at deploy time.
Serving the alias directly would mean a `set_registered_model_alias` call in a training
notebook silently changes what production answers with — a promotion with no deploy, no
review and no rollback point. Here promotion and deployment are two steps: retrain moves
`@champion`, and nothing reaches traffic until notebook 23 runs.

Rollback is therefore a version number: set the `model_version` widget to the previous
version and rerun. The endpoint updates in place, the served entity is renamed to match,
and there is no rebuild.

The endpoint keeps the old served entity serving while the new one builds, so a version
change is not an outage. What it is *not* is a canary: this POC sends 100% of traffic to
one version. Model Serving supports traffic splitting across served entities, and a real
rollout of a ranking model should use it — see §6.

## 3 · Online inference

The ask: an endpoint that takes a user, a context and the eligible rails, returns them
ranked, and retrieves its own online features on the way. Section 1 covered why the
features are consistent; this is the request path itself.

### `exclude_columns` is the request contract

Worth calling out because it is not obvious and it fails at serving time rather than at
training time: **every column left in the training set becomes an input on the logged
signature, and label-side ones become required.** A live request carrying only the seven
legitimate keys was rejected with

```
Failed to enforce schema ... Model is missing inputs
  ['rail_position', 'was_viewport', 'sample_weight']
```

while all 45 looked-up features were correctly marked `(optional)` — automatic feature
lookup working exactly as intended. Those three are properties of the logged impression
(a rendered position, a viewport flag, an IPS weight); the homepage has no business
supplying them. They are now in `exclude_columns` and re-attached in pandas on
`(viewer_id, rail_id, request_epoch_s)` for the fit and the evaluation.

Dropping them from `input_example` was **not** sufficient — the schema comes from the
training set, not the example.

**And the dtypes are part of that contract too.** A point-in-time join returns NULL
wherever a viewer × rail snapshot does not exist yet, pandas widens the column to
float64, and the inferred signature says `double` for something that is `LONG` in Delta.
At serving the online lookup returns a real int64 and MLflow refuses to narrow it:

```
Incompatible input types for column vr_last_click_epoch_s.
Can not safely convert int64 to float64.
```

The caller sees only `Error ''`. Notebook 22 now reads the integral columns out of the
lookup tables' Delta schemas, restores int64 before logging, and **asserts** at training
time that no integral column reached the signature as a float. Anyone building this on
sparse features will hit the same thing.

> **Debugging note.** A serving 400 of this kind is opaque from the client. The real
> traceback is only in
> `databricks serving-endpoints logs <endpoint> <served-entity-name>` — and the entity
> name is `config.served_entities[0].name`, not the endpoint name.

### How features are retrieved

Per request, the endpoint:

1. issues one keyed read against `online_rail_features` per candidate rail
   (`rail_id`);
2. issues one **composite-key** read against `online_viewer_rail`
   (`viewer_id`, `rail_id`) per candidate rail;
3. issues one keyed read per viewer table (`online_viewer_features`,
   `online_recent_behavior`);
4. evaluates the five request-time UDFs on the joined row;
5. scores and ranks.

A 12-rail request implies up to 26 logical keyed reads inside one HTTP call — 2 per rail
plus 2 for the viewer. Whether the platform issues those serially or batches them is not
documented, so the benchmark's `fanout` phase sweeps candidates-per-request to find out
rather than assuming.

**Measured answer: they are batched.** Latency is flat from 1 to 32 candidate rails
(p50 ~55–57 ms), so the serial reading of that arithmetic is wrong and a bigger homepage
is close to free at this layer. See § 4.

In-region keyed reads against Lakebase, measured directly through Postgres in
notebook 21, are single-digit milliseconds — but a serial estimate from that is an
upper bound, not a prediction. The endpoint's actual behaviour is what
`serving_benchmark.md` measures.

### Feature Serving, for the callers that are not a model

`crunchyroll-viewer-features` is a Feature Serving endpoint: the same governed
feature values over REST with no model behind it. Two uses in this POC — the
explainer agent reads it as a tool, and the benchmark runs the same fanout sweep
against it so the online-lookup share of the ranker's latency is measured rather
than assumed.

---

## 4 · Production serving

**The ask:** expected end-to-end latency, concurrency and throughput, autoscaling
behaviour, and behaviour under traffic spikes.

**Full tables in [`serving_benchmark.md`](serving_benchmark.md).** All measurements from
an in-region job against `crunchyroll-rail-ranker` serving **version 9**
(`scale_to_zero=false`, provisioned concurrency 4–32, route optimization **off** because
the workspace rejected it), 12 candidate rails per request unless stated.

| Question | Measured |
|---|---|
| Latency, low concurrency | **p50 52 ms, p95 64–68 ms** (concurrency 1–2) |
| Latency vs candidates per request | **flat**: p50 54–58 ms from 4 to 32 rails |
| Throughput | **~80 req/s on a recently-updated endpoint, ~212 req/s once it has scaled** — see below, this is not one number |
| Behaviour past capacity | **HTTP 429**, not unbounded queueing |
| Spike 2 → 48 concurrent | p50 178 ms, p95 383 ms, 5,852 of 11,053 rejected, 206 req/s served |
| Recovery after the spike | **immediate** — p50 53 ms, p95 67 ms, zero errors |
| Feature lookups + UDFs alone | **p50 33–35 ms** (Feature Serving, also flat 4→32) |
| Model + ranking share | **~17 ms** (52 − 35) |

### The fanout result

Latency is **flat from 4 to 32 candidate rails** — 32 rails cost the same as four. So the
endpoint's automatic feature lookup **batches** the per-rail reads rather than issuing
them serially. Two independent runs agree, and so does the laptop run from a completely
different network position, which is what makes this the most solid conclusion here.

The 1-rail measurement (86.4 ms) is *higher* than the 4-rail one, in both runs. That is
first-shape cost, not a fanout effect — it is the first request of a new payload shape,
and the warm-up request the harness sends does not cover every shape.

Practical consequence for Crunchyroll: **a richer homepage is close to free at the ranking
layer.** Going from 12 rails to 30 costs no latency, only a slightly larger response body.

**A correction to an earlier version of this document.** It stated that a 12-rail request
"implies up to 26 keyed reads" and framed the fanout slope as the test of whether batching
happens. The test ran and the serial model was simply wrong: the slope is zero.

### Throughput is not a single number, and this is the most important finding here

**Corrected after a deliberate measurement.** A ten-minute sustained-load phase was added
specifically to measure how long capacity takes to arrive, and it reported no scale-up at
all — flat ~200 req/s from its first 30-second window. That was a measurement artifact:
the phase ran *after* the spike, so the endpoint was already at full capacity before it
started. What the same run does show, at the **same** concurrency 32:

| regime | throughput | p50 | 429s |
|---|---|---|---|
| ramp (early in the run) | 77.5 req/s | 369 ms | none |
| sustained (after the spike) | ~200 req/s | 71 ms | ~12,000 per 30 s |

Both are real and they are different *modes*, not different capacities: early on the
endpoint queues excess load, and once pushed it sheds it with 429s instead. Shedding
gives higher successful throughput **and** lower latency for the requests that get
through. The phase has been moved ahead of the ramp so a future run measures capacity
arriving rather than capacity already arrived; until then, treat "how long does scale-up
take" as **not yet cleanly measured**, and see `verification_log.md` V50 and V59.

An earlier version of this document reported a **"~212 req/s ceiling, reached at
concurrency 16"**. That number is real but it was **half of the evidence**, and reporting
it alone flattered the platform. Corrected, with both halves:

| Ramp sweep | conc 4 | conc 8 | conc 16 | conc 32 | conc 64 |
|---|---|---|---|---|---|
| **A** — endpoint recently updated | 66 req/s | 76 | **80** | 81 | 85 |
| **B** — same endpoint, ~10 min later | 68 req/s | 131 | **212** | 213 | 216 |

Sweep A is p50 59 → 100 → 184 → 359 ms as concurrency rises; sweep B holds p50 56 → 58 →
63 → 86 ms over the same range. **Same endpoint, same configuration, same payload, 2.6×
the throughput.** The difference is that B ran after the endpoint had spent ten minutes
under sustained load and had scaled toward its `max_provisioned_concurrency` of 32; A ran
while it was still near the floor of 4.

The newest run reproduces this *within a single run*: its ramp plateaus at **80 req/s**,
and the spike phase that follows — after the ramp has warmed the endpoint — serves
**206 req/s** at concurrency 48.

Three consequences, and they matter more than any single latency number:

* **Provision the floor for peak.** `min_provisioned_concurrency` is what you get
  immediately; `max` is what you get several minutes later. For homepage traffic, size
  the floor for peak rather than relying on scale-up.
* **Autoscaling operates in minutes, not seconds.** A 12-second burst gets no new
  capacity. A ten-minute load ramp gets 2.6×.
* **Never quote this endpoint's throughput without saying how long it had been under
  load.** Any benchmark short enough to be convenient measures the floor, not the ceiling.

### Concurrency, and what happens at capacity

Past available capacity the endpoint returns **HTTP 429** rather than queueing — worth
stating plainly because current public documentation does not specify this behaviour. A
caller needs retry-with-backoff and a fallback, not a longer timeout.

In the newest run 429s appear only at concurrency **64** in the ramp (6,888 rejected while
1,064 succeeded), and in the spike at 48. In the earlier, already-scaled run they began at
concurrency 16. Which is to say: the concurrency at which rejection starts is a function
of provisioned capacity at that moment, not a fixed property of the endpoint.

For sizing, the documented rule is
`provisioned_concurrency ≈ QPS × model_execution_seconds`. At ~52 ms per request that is
roughly 19 req/s per unit of concurrency — and the measured 80 req/s near a floor of 4,
and 212 near a ceiling of 32, both sit in the range that rule predicts.

### Spike behaviour

Baseline at concurrency 2 is p50 55 ms / p95 76 ms. Stepping straight to 48 concurrent
pushes p50 to 178 ms and p95 to 383 ms, serves 206 req/s, and **rejects 5,852 of 11,053
offered requests with 429**. First second of the spike: p95 **402 ms**.

When load drops the endpoint returns to baseline immediately — p50 **53 ms**, p95
**67 ms**, **zero** errors in the recovery phase.

The reassuring half is that there is no lasting damage and no queue to drain. The half to
design around is that the excess is *rejected*, so the homepage needs a fallback order to
render when that happens. This POC does not implement one.

One reporting detail, because it looks like a contradiction: the report's
"seconds to return within 1.5× baseline p95" is **None**. That statistic is computed over
the per-second buckets *inside* the spike, so it is saying the endpoint never recovered
**while the spike was still running** — which is expected. Recovery after the load stops
is the `spike_recovery` row, and it is immediate.

### Where the time goes

The same fanout sweep against the Feature Serving endpoint — same governed features, no
model — returns **p50 33–35 ms, also flat** across 4 to 32 rows. So of the ~52 ms at low
concurrency:

* **~35 ms** is the online lookup and on-demand UDF layer,
* **~17 ms** is the model and the ranking,
* and both are flat in the number of candidates.

That decomposition is measured, not apportioned, and it says where optimisation effort
would go: the feature layer, not the model.

### Server-side time, and what it can and cannot be compared with

The endpoint's own `execution_duration_ms`, from the AI Gateway inference table, over the
benchmark window: **11,911 requests, p50 98 ms, p95 403 ms, p99 570 ms, zero non-200**.

**This number must not be subtracted from the concurrency-1 client latency.** The window
spans concurrency 1 through 64 *and* a 48-way spike, so it is dominated by requests made
under heavy load — which is why the server-side p50 (98 ms) is *higher* than the
client-observed p50 at concurrency 1 (52 ms). Those are different populations of requests,
not a transport measurement.

This is also a correction: earlier versions of this document said client wall time minus
`execution_time_ms` "is printed rather than inferred". It was not. The query referenced
columns that do not exist on the inference table (`timestamp_ms`, `execution_time_ms`;
the real ones are `request_time` and `execution_duration_ms`), so every run stored an
`UNRESOLVED_COLUMN` error where a measurement should have been, and nobody read the field.
Fixed, and the numbers above are the first real reading. The defensible attribution of the
lookup share remains the Feature Serving comparison, which is a direct measurement.

### Where the client sits changes the answer

The benchmark runs **twice**:

* `make bench` — as a job inside the workspace region. This measures the platform.
* `make bench-local` — the same code from a laptop.

Both were run against the same endpoint and the same 12-rail payload. The in-region
column is the newest run (model version 9); the laptop column is the run made from here.
The laptop run was made against version 8, so treat the pairing as network-position
evidence rather than a controlled A/B — the latency gap is two orders of magnitude larger
than any difference between those two model versions.

| | in-region job | this laptop |
|---|---|---|
| fanout p50, 1 rail | 86.4 ms | 410.9 ms |
| fanout p50, 16 rails | 54.2 ms | 374.2 ms |
| ramp p50 @ conc 1 | 51.6 ms | 378.9 ms |
| ramp p50 @ conc 16 | 183.9 ms | 398.4 ms |
| peak achieved req/s | **80 → 212** (see above) | **53.8** |
| 429s observed | 6,888 @ conc 64 | **none, at any level** |

So the distance is **roughly 320–340 ms**, not the 200–250 ms this document previously
estimated — the estimate was replaced with the measurement.

Two things fall out of that table which are worth more than the latency delta:

* **Fanout is flat from both vantage points.** 4 rails to 16 costs nothing extra, at
  54 ms or at 380 ms. The same conclusion from two very different network positions,
  which is what makes it believable.
* **A laptop cannot find the throughput ceiling.** From here the client saturates at
  53.8 req/s — its own round trip is the bottleneck long before the endpoint's is — so
  the local run sails through concurrency 16 and a 24-way spike with **zero rejections**,
  while the in-region run rejects thousands in the same spike shape. Anyone benchmarking
  this endpoint from outside the region will conclude it has far more headroom than it
  has. That is the single most misleading result in this whole exercise, and it is why
  both runs are reported.

A p95 of 400 ms from a laptop and 64 ms from a job are the same endpoint; only one of them
is a statement about Databricks. For Crunchyroll's own sizing, the number that matters is
the in-region one plus their own service's distance to the endpoint.

One more measured detail: the first request from a cold client was **1454 ms vs 406 ms
warm**. That is *not* a container cold start — this endpoint has `scale_to_zero=false` and
never scales down. It is TLS handshake plus the SDK's first OAuth token fetch, both
client-side and one-time per process. The local benchmark originally labelled it a cold
start, which would have overstated the platform's cold-start cost by about a second; it
now reports the cause it can justify from the endpoint config.

Server-side time and why it cannot simply be differenced against these numbers is
covered above.

### Documented platform limits

From current Databricks documentation, not from this POC's measurements:

* Model Serving supports **over 25K QPS** with **under 50 ms overhead latency** at
  that scale.
* Request timeout is **597 seconds** — irrelevant for ranking, but it bounds the
  worst case.
* Scale-up on rising traffic is described as near-immediate; scale-down happens on
  a five-minute cadence.
* Scale-to-zero is explicitly **not recommended for production workloads requiring
  consistent uptime**.

Not documented publicly, and therefore measured here or flagged as unknown:
per-endpoint QPS ceilings, queueing versus 429 behaviour when concurrency is
exceeded, payload size limits, and feature-lookup miss behaviour. These are worth
confirming with the Databricks account team before a production sizing commitment
— this POC measures what this endpoint did, which is evidence but not an SLA.

### Online store sizing

The Lakebase online store is the one component that **cannot scale to zero**. At
`CU_1` the backing endpoint floor is 4–8 CU; `CU_2` gives 8–16 — both measured on this
workspace by changing the capacity class and reading the endpoint back. Also measured
here: at `CU_2`, 30.67 DBU/day ≈ $15.95/day; at `CU_1`, ≈ $11/day.

Per the docs (not measured here), up to three read replicas can be added for
read-heavy serving, which distributes read traffic and multiplies the compute cost by
the replica count. This POC runs `read_replica_count=0`.

Sizing lever order, most to least effective: capacity class → what goes online at
all (current values only, history stays offline) → publish mode per table
(TRIGGERED per refresh vs CONTINUOUS always-on) → read replicas.

---

## 5 · Out of the box vs. build and operate

The question the ask is really about.

### Databricks provides, no code

* Feature tables in Unity Catalog with primary keys, lineage and Change Data Feed.
* Point-in-time correct training joins (`create_training_set` with
  `timestamp_lookup_key`).
* An online store on managed Lakebase Postgres, and a managed sync pipeline per
  published table (TRIGGERED or CONTINUOUS).
* Automatic feature lookup at serving: the feature spec travels in the model, the
  endpoint does the reads.
* Request-time features as governed Unity Catalog Python UDFs.
* Model registry with versions, aliases, tags and lineage back to the run.
* Model Serving with autoscaling, provisioned concurrency, route optimization,
  traffic splitting between versions, and inference tables.
* Feature Serving endpoints — features over REST with no model.
* Monitoring: inference tables, system billing tables, sync status APIs.

### Crunchyroll builds

* **Feature definitions.** The maths of what a feature means is domain work, and
  always will be. `src/crfs/features.py` and `src/crfs/rails.py` are that, and
  they are the files a Crunchyroll engineer would recognise as theirs.
* **The homepage log.** Rail-level impressions with the **rendered position** and a
  viewport signal. This POC generates them; in production they must be logged. **If
  position is not in the log, vertical ranking cannot be debiased, and this is the
  single highest-value change to make before a real build.**
* **Eligibility rules.** Which rails a viewer may see, evaluated before scoring.
* **Candidate resolution for personalized rails.** Continue Watching and Because
  You Watched resolve per viewer at request time; that is service logic, not a
  feature table.
* **The homepage service.** Calling vertical once, then horizontal per rail in
  parallel, with timeouts and a fallback order.
* **Fallbacks.** What the homepage renders when the endpoint is slow or down. This
  POC does **not** implement one and that is a real gap — see below.

### Crunchyroll operates

* Feature freshness per class: which tables refresh on a schedule, which stream.
* Online store capacity and replica count against measured read load.
* Endpoint provisioned concurrency against measured QPS, revisited as traffic
  grows.
* Retraining cadence, and the drift monitoring that triggers it. Inference tables
  are captured here; a monitor over them is not built.
* Cost. The online store bills continuously; a request-path endpoint without
  scale-to-zero bills continuously. Both are deliberate and both need an owner.

---

## 6 · What this POC does not do

Stated plainly, because a POC that only lists strengths is not evidence.

1. **The data is synthetic.** 300 viewers, 132 titles, 363k rail impressions,
   4,681 viewer × rail keys. The labels come from a latent utility the model can
   recover, so **the reported NDCG lift is a statement about the pipeline, not a
   forecast of Crunchyroll's lift.** Real gains depend on real signal.
2. **Online store cardinality is small.** 4,681 online rows is not 50 million.
   Keyed reads on Postgres with an index degrade gently, but "gently" is not
   "measured". A scale test on representative cardinality is needed before sizing.
3. **No fallback path.** No cached previous ranking, no editorial default on
   timeout, no circuit breaker. A production homepage needs all three, and their
   design affects the latency budget more than the model does.
4. **No A/B or online evaluation.** Offline NDCG against a logged policy is a
   directional signal. Interleaving or a bucket test is the real measurement, and
   this POC has neither.
5. **Position bias is corrected, not solved.** IPS on observed engagements with a
   viewport signal is a reasonable estimator. It is not the same as an unbiased
   randomised-exposure dataset, and it assumes the propensity model is right.
6. **Single region, single workspace.** No multi-region serving, no failover, no
   disaster recovery story.
7. **No streaming path for rail features.** The horizontal side has a CONTINUOUS
   streaming path for session features; the vertical side is TRIGGERED only. A
   viewer's rail interactions in the current session are therefore not reflected
   for one refresh interval. The mechanism to fix that exists and is demonstrated
   elsewhere in this repo; it is not wired up for rails.
8. **The benchmark is a benchmark.** Repeatable, in-region, with its configuration
   recorded — and still not a production load test against production payload
   sizes and cardinality.

---

## 7 · Running it

```bash
./setup.sh --profile <PROFILE>       # empty workspace to working demo, one command
make vertical PROFILE=<PROFILE>      # just the vertical path
make bench    PROFILE=<PROFILE>      # measure the endpoint in region
make bench-local PROFILE=<PROFILE>   # the same, from your laptop
make bench-pull PROFILE=<PROFILE>    # copy the write-up into docs/
make teardown-cost PROFILE=<PROFILE> # stop the always-on spend, keep the data
```

Nothing is pinned to the workspace this was built on. `scripts/bootstrap.sh`
resolves the three ids that differ per workspace — SQL warehouse, Lakebase
database resource, billing endpoint uid — at run time and writes them to
`.crfs.vars`, which is gitignored and passed to the bundle as `--var` flags.

| Notebook | What it does |
|---|---|
| `20_rail_data_generation.py` | Rail catalog, rail × title map, the homepage impression log with position bias, and the measured propensity table |
| `21_rail_features.py` | `rail_features` and `viewer_rail_features_ts`; publishes both to the same online store; asserts the online copy is one row per key |
| `22_train_rail_ranker.py` | Point-in-time training set, IPS-weighted fit, NDCG against three baselines plus an ablation, logged with its feature spec and registered |
| `23_deploy_rail_endpoint.py` | The request-path endpoint: no scale-to-zero, explicit provisioned concurrency, route optimization, inference table |
| `24_homepage_assembly.py` | A whole homepage from both rankers; the shared-table overlap resolved from UC; four-context sensitivity check |
| `25_serving_benchmark.py` | fanout, ramp, spike, features-only, server-side attribution; writes `serving_benchmark.md` |
