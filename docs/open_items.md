# Open items

What is unfinished or undecided in this POC, and the one thing only Crunchyroll can
answer. This replaced a ten-question discovery list, which was the wrong artifact: most
of those were questions any engineer would ask any recommender customer, not things this
build actually depends on.

Ordered by how much the answer changes what gets built.

---

## 1 · Rendered rail position in the homepage impression log

**The only question here that Crunchyroll must answer, and no work on our side
substitutes for it.**

Every label in a homepage log was observed at a position the *incumbent* policy chose.
Rails at the top get engagement because they are at the top. This POC corrects for that
with inverse-propensity weights from a measured `P(viewport | position)` curve — 0.97 at
position 1 falling to 0.09 at position 16 on synthetic data.

What we need to know about the log **as it exists today**:

* is the **rail-level impression** recorded at all, or only title-level clicks?
* is the **rendered position index** of each rail on the response stored?
* is there any **visibility signal** — rail scrolled into view, dwell — or only clicks?

**Why it decides things:** if position is not logged, vertical ranking cannot be
debiased, and the first model will largely relearn the incumbent editorial order while
reporting a good offline number for doing so. That is not a tuning problem; the labels
are wrong. Adding position and a visibility flag to the log is cheap, has no ML
dependency, and is the highest-value change to make before any model work. If a viewport
signal already exists, the propensity estimate becomes direct rather than modelled.

---

## 2 · Not measured: online-store read latency at real cardinality

**The largest untested assumption in the POC.**

The 52 ms p50 rests on one precomputed row per `(viewer, rail)`. This POC has **4,681**
such rows. Real scale is MAU × eligible rails — tens of millions to a billion.

At tens of millions the design stands and the question is only Lakebase capacity class
and read replicas. Approaching a billion, the honest options narrow to publishing top-N
rails per viewer rather than the full cross, or moving part of the viewer × rail signal
into a request-time computation. **A scale test at representative cardinality is needed
before anyone commits to a latency target.**

---

## 3 · Traffic splitting — now demonstrated, still not a rollout process

The ask says "model/version management **and deployment**". This POC pins an immutable
version and sends it **100%** of traffic. Model Serving supports splitting traffic
across served entities, and that is how a ranking model should actually be rolled out —
a canary on a small share, watched on the inference table, before it takes the homepage.

Promotion and deployment are already two separate steps here (retrain moves
`@champion`; nothing reaches traffic until notebook 23 runs), so the missing piece was
the split itself, not the discipline around it.

**Since:** `notebooks/30_advanced/31_feature_versioning.py` §6 puts two model versions
behind the endpoint at 90/10, reads the realised routes back, fires requests across the
split and restores 100% to the pinned version. What is still absent is the *process*
around it — a metric to judge the canary on, a promotion gate, and an automatic rollback.
Attribution would come from the inference table, which records the served entity per
request; nothing in this POC reads it for that purpose.

---

## 4 · Training at production volume — the substitution is now proven, the volume is not

The point-in-time join is Spark and scales. The **estimator does not** — it collects to
pandas and fits scikit-learn on one driver. Measured here: 363k labels against a
421k-row time-series table did not finish inside a 60-minute task on two separate runs,
which is why the demo fits on a 25% session sample.

The substitution is an estimator that trains in minibatches rather than in one driver's
memory, fed from `training_set.load_df()` with no collect, touching neither the feature
layer, the feature spec, nor the serving path.

**Since:** `notebooks/30_advanced/32_gpu_train.py` does exactly that — the same
point-in-time training set exported to Parquet on a UC volume, a torch MLP trained on a
serverless A10 in minibatches, logged with `fe.log_model` so the feature spec still
travels and the model is a drop-in for the same endpoint. `src/crfs/train_gpu.py` is the
module; `ai/train.yaml` runs it from a laptop.

What remains unproven is the **volume**, not the mechanism: this still trains on the 25%
session sample, because the 60-minute ceiling was the point-in-time join and the pandas
collect together, and only the second of those has been removed. The join is Spark and
scales; measuring it at Crunchyroll's cardinality is the outstanding test.

---

## 5 · Not implemented: a fallback ranking

Past available capacity the endpoint returns **429** rather than queueing, and recovery
is immediate. That is good behaviour, but it means the homepage must be able to render
without a fresh ranking.

This POC has no cached previous ranking, no editorial default on timeout, and no circuit
breaker. If Crunchyroll already has a default order or a per-viewer cached ranking, this
is a few lines in the homepage service. If not, its design affects the latency budget
more than the model does and should be scoped alongside.

---

## 6 · Platform: route optimization is rejected on this workspace

Every latency number in `serving_benchmark.md` carries the standard workspace
request-path overhead, because `route_optimized` was rejected here and it is a
**create-time only** property — it cannot be added to the existing endpoint.

This is ours to escalate, not Crunchyroll's to answer, and it should be raised with the
account team **before** any tighter latency commitment depends on it.

---

## Closed since the first draft

* **Title features shared only at source-data level.** The rail content stats were
  computed from the raw `titles` table rather than the `title_features` feature table,
  so title signal was shared as source data rather than as governed features. Now
  sourced from `title_features`, which also removed a duplicate definition of content
  age and a mild leakage (the generator's latent `intrinsic_popularity` in place of the
  observed `popularity_30d`). See `verification_log.md` V57.
* **How long autoscaling takes.** Originally found by accident — two ramp sweeps ten
  minutes apart differing by 2.6× — and now measured deliberately by a sustained-load
  phase that reports throughput per 30-second window. See V50 and
  `serving_benchmark.md`.
