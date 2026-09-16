# Open questions for the Crunchyroll DSML team

The POC answers the four asks with measured numbers. These are the questions whose
answers would change **sizing, cost or design** — not clarifications for their own sake.
Each one says what we would do differently depending on the answer, so none of them is
a question you have to answer twice.

Ordered by how much the answer changes.

---

## 1 · Does the homepage impression log already carry rendered rail position?

**Why it decides things:** every label in a homepage log was observed at a position the
current policy chose. Rails at the top get engagement *because* they are at the top. The
POC corrects for this with inverse-propensity weights derived from a measured
`P(viewport | position)` curve — 0.97 at position 1 falling to 0.09 at position 16.

Specifically we need to know, for the log as it exists **today**:

* is the **rail-level impression** logged at all, or only title-level clicks?
* is the **rendered position index** of each rail on the response recorded?
* is there any **visibility / viewport signal** (rail scrolled into view, dwell), or only clicks?

**What changes:** if position is not logged, vertical ranking cannot be debiased and the
first model would largely relearn the current editorial order while scoring well for it.
Adding position to the log is cheap, has no ML dependency, and is **the highest-value
change to make before a real build.** If a viewport signal also exists, the propensity
estimate is direct rather than modelled.

---

## 2 · What is peak homepage RPS, and what is the peak-to-median ratio?

**Why it decides things:** measured here, the endpoint holds **~212 req/s** at
provisioned concurrency 4–32 and returns **HTTP 429** past that rather than queueing.
Critically, capacity did **not** arrive inside a 12-second burst — so autoscaling does not
rescue a spike, and `min_provisioned_concurrency` has to be sized for peak.

We need peak RPS, not average, plus the shape: simulcast Friday, a major episode drop, a
season premiere.

**What changes:** the sizing rule is `concurrency ≈ QPS × execution_seconds`; at 56 ms
each unit of concurrency buys roughly 18 req/s. That turns your peak number directly into
a provisioned floor and a monthly cost. Without it any capacity statement we make is a
guess wearing a number.

---

## 3 · How many viewers × how many rails, on the online store?

**Why it decides things:** this is the **single biggest untested assumption** in the POC.
The `viewer_rail_features_ts` design — one precomputed row per (viewer, rail) — is what
makes the request path 56 ms. The POC has **4,681** such rows. Real scale is
MAU × eligible rails, so tens of millions to a billion.

**What changes:**
* at tens of millions of keys, the design stands and the question is only Lakebase
  capacity class and read replicas;
* approaching a billion, the honest options are narrowing what goes online (top-N rails
  per viewer rather than the full cross), or moving part of the viewer × rail signal into
  a request-time computation.

Either way we would want a **scale test at representative cardinality** before you commit
to a latency target. Give us the number and we can size it or test it.

---

## 4 · What is the latency budget for the vertical call specifically?

**Why it decides things:** we measured p50 **56 ms**, p95 **72 ms** in region, flat from 1
to 32 candidate rails, decomposed as ~40 ms feature lookups + ~16 ms model. What we do not
know is your budget, or how many network hops sit between your homepage service and the
endpoint.

**What changes:** if the budget for ranking is 150 ms+, there is comfortable room and no
further work. If it is tighter than ~80 ms end to end, then **route optimization becomes
required rather than nice to have** (it is create-time only and currently not enabled on
the workspace we built in), and the 40 ms feature-lookup share becomes the thing to
optimise — not the model.

---

## 5 · How many rails does the homepage score, and how many does it render?

**Why it decides things:** the POC scores 12–16 candidate rails per request. Latency was
**flat to 32 rails**, so within that range a richer homepage is close to free at the
ranking layer. Beyond 32 is unmeasured.

**What changes:** if you score 30, nothing changes and we can say so with data. If you
score 100+, we should re-run the fanout sweep at that width before quoting a latency.
Also relevant: how many rails render per device class, because that drives the number of
*horizontal* calls behind each vertical one.

---

## 6 · How fresh do viewer × rail features need to be?

**Why it decides things:** the POC recomputes them daily and publishes with a TRIGGERED
sync. Continue Watching is the obvious case where daily may not be enough — if a viewer
finishes an episode, should that rail move within the same session?

**What changes:** per-table choice between TRIGGERED (refresh on a schedule) and
CONTINUOUS (always-on streaming sync, always-on cost). The POC demonstrates both, so this
is a cost and a freshness decision rather than an engineering one — but the answer differs
per feature class and we would rather set it from your requirement than default it.

---

## 7 · Where does the horizontal ranker run today, and will it read the same store?

**Why it decides things:** the shared-feature-store argument in this POC rests on both
models reading the **same** online tables — demonstrated live: two endpoints, one Lakebase
project, shared `viewer_features_current` and `recent_behavior_current`, two shared UDFs.

**What changes:** if the horizontal ranker stays outside Databricks, the operational
saving (one pipeline, one online store, one capacity bill, no second copy to keep
consistent) is halved, and the training-serving consistency guarantee only covers the
vertical side. Worth being explicit about the target state before this is used to justify
the shared layer.

An honest note that belongs with this question: in this POC the **shared viewer features
contributed essentially nothing predictively to rail ranking** (permutation importance
indistinguishable from zero, either sign). Sharing paid off **operationally**, not
predictively. A viewer's genre affinity matters much more for *which title* than for
*which row*. We would rather say that than imply every shared feature earns its place in
every model.

---

## 8 · Do you have a fallback rail order today?

**Why it decides things:** past its ceiling the endpoint **rejects** with 429 rather than
queueing, and recovery is immediate. That is good behaviour — but it means the homepage
must be able to render without a fresh ranking.

**What changes:** the POC deliberately does **not** implement a fallback, and we list that
as a real gap. If you already have an editorial default order or a cached previous
ranking per viewer, the design is a few lines in the homepage service. If not, its design
affects the latency budget more than the model does and should be scoped alongside.

---

## 9 · What volume of homepage impressions per day would training run on?

**Why it decides things:** the point-in-time join is Spark and scales. The **estimator in
this POC does not** — it collects to pandas and fits scikit-learn on one driver. Measured
here: 363k labels against a 421k-row time-series table did not finish inside a 60-minute
task on two separate runs, which is why the demo fits on a 25% session sample.

**What changes:** at your volume the substitution is Spark ML or XGBoost/LightGBM on
Spark, fed from `training_set.load_df()` with no collect. It does not touch the feature
layer, the feature spec or the serving path — but we should size it rather than discover
it, and it is the second scalability item after cardinality.

---

## 10 · What would you consider a successful evaluation?

**Why it asks:** the ask says the goal is to assess Feature Store and Model Serving as
**shared infrastructure**, not as isolated capabilities. We have built to that reading —
two rankers, one feature layer, measured under load.

Worth confirming what closes the evaluation: a live walkthrough, the measured numbers
reviewed against your own targets, a scale test at your cardinality, or a hands-on run in
a Crunchyroll workspace. The last is one command, and the setup discovers workspace ids
rather than carrying ours.
