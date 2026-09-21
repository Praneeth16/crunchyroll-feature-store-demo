# Batch now, online later — what carries over

Crunchyroll's November deliverable is **batch**, because Lakebase is not yet available in
their region (GCP us-west1). Real-time is the end goal, not the starting point. So the
question is not whether Databricks can do batch — it is **how much of the batch work
survives the switch to online**.

Short answer, measured on this workspace against model version **10**: the feature
definitions, the training set, the registered model and the ranking logic are the same
objects. What changes is one API call and where features are read from. Batch and online
produced the **identical collection order** for the same viewer.

Everything below is measured in `serverless_lakebase_praneeth_catalog.crunchyroll_demo`,
not estimated.

---

## The two paths, side by side

| | batch — today, no online store | online — when Lakebase lands |
|---|---|---|
| features read from | offline Delta feature tables | published Lakebase copy |
| how | `fe.score_batch(model_uri, df)` | `POST /serving-endpoints/…/invocations` |
| feature definitions | `src/crfs/features.py`, `rails.py` | **identical** |
| feature spec | embedded in the model | **identical** |
| model | `crunchyroll_rail_ranker` v10 `@champion` | **identical version** |
| lookups performed | 4 feature tables + 5 UC Python UDFs | **the same 4 + 5** |
| ranking logic | window over the scored frame | same ranking inside the pyfunc |
| context features | fixed at scoring time | evaluated per request |
| requires an online store | **no** | yes |
| output | `rail_rankings_batch` Delta table | HTTP response |

`score_batch` resolves features from the **offline** store. Per Databricks documentation:
"If the model at model_uri is packaged with the features, the `score_batch()` call
automatically retrieves the required features from Feature Store before scoring the
model." Nothing in the batch path touches Lakebase, which is what makes it deliverable
today in a region where the online store is not yet available.

---

## Measured: the two paths agree

Notebook 26 scores a viewer through `score_batch`, then queries the live endpoint with the
same viewer, the same collections, the same context, and the same model version.

| | result |
|---|---|
| collections compared | **16** |
| identical rank | **16 of 16** |
| Spearman(batch, online) | **1.0** |
| max absolute probability difference | **0.0026** |

**The orderings are identical.** So the batch work is not throwaway: switching to
real-time later is a deployment change, not a modelling change, and the ranking does not
need re-validating.

**Why the probabilities differ slightly, and why that is correct.** The delta is not
noise and not a bug: `cr_rail_click_recency` takes `request_epoch_s` as an input, and the
batch job and the endpoint call happened seconds apart. A time-decay feature *should*
return a slightly different value at a different clock. The ranks are unaffected because
every collection in the request shifts by the same clock. If this number were zero, the
recency feature would not be doing its job.

---

## Measured: what batch cannot do

Batch precomputes one order per viewer **for one context**. Five of the model's features
are request-time UDFs reading `device`, `hour_of_day` and `request_epoch_s` — none of
which exist until a request arrives.

Same viewer, same collections, four contexts, scored through the endpoint:

**5 to 12 of 16 collections change position** between contexts, depending on the baseline
and on the wall clock. Two runs on 2026-09-18, both against the 21:00-on-TV order:
09:00 TV moved 6 then 5, 21:00 mobile moved 12 then 7, 09:00 mobile moved 9 both times.
The spread across runs is itself expected -- two of the five request-time features decay
with time, so the same four contexts need not separate by the same amount at every clock.
A table precomputed at 21:00-on-TV is wrong for **up to 12 of 16 positions** when the same
viewer opens the app in another context.

That is the honest cost of batch, and it is the thing the online store buys back. **The
Phase 2 argument is not primarily latency — it is personalization that a precomputed
table structurally cannot deliver.** Precomputing every context instead multiplies the
table by the number of contexts, which at a minutes-level refresh cadence is the
expensive direction.

---

## Measured: batch throughput, and what a minutes cadence implies

| | measured |
|---|---|
| rows scored | **4,628** (300 viewers × ~15.4 eligible collections) |
| wall time for `score_batch` + materialisation | **69.9 s** |
| throughput | **66 rows/s** |
| write of the ranked table | **2.9 s** |
| output | `rail_rankings_batch`, 4,628 rows, model_version 10 |

**These numbers do not extrapolate linearly and are not offered as a capacity model.**
69.9 s for 4,628 rows is dominated by Spark job startup on serverless, not by per-row
work; at a hundred million rows the per-row cost would be far lower and the total far
higher. The shape that does transfer:

* a **full** refresh is `O(viewers × eligible collections)`;
* an **incremental** refresh is `O(viewers whose features changed)`.

At the requested cadence of **minutes**, only the second is viable. Rescoring an entire
MAU every few minutes is not a tuning problem, it is the wrong shape.

### Change Data Feed is what makes the incremental mode possible

Verified on this workspace: `delta.enableChangeDataFeed` is **supported on all four**
feature tables (`viewer_features_current`, `recent_behavior_current`, `rail_features`,
`viewer_rail_features_ts`). So "whose features moved since the last run" is a query
rather than a guess, and `make batch-incremental` uses it:

1. read each feature table's change feed from the version the last run recorded;
2. collect the affected `viewer_id`s;
3. rescore only those, and write with `replaceWhere` so the rest of the table stands.

One deliberate asymmetry: `rail_features` is keyed by `rail_id`, so **any** change to it
affects every viewer's ordering and forces a full refresh. The notebook detects that and
says so rather than silently producing a partially-stale table.

---

## What Crunchyroll would run, in each phase

**Phase 1 — now, GCP us-west1, no online store**

```
notebooks/00,01      feature engineering        → Delta feature tables in UC
notebooks/20,21      rail/collection features   → Delta feature tables in UC
notebooks/22         PIT training set + model   → registered in Unity Catalog
notebooks/26         score_batch                → rail_rankings_batch (Delta)
                     make batch-incremental     → minutes cadence via CDF
```
No Lakebase. No serving endpoint. Their homepage service reads the precomputed table.

**Phase 2 — when Lakebase is available in their region**

```
notebooks/21         publish_table(...)         → the same tables, published online
notebooks/23         deploy the same version    → crunchyroll-rail-ranker endpoint
```

Nothing in the feature definitions, the training set, or the model changes. The model
version that was scoring in batch is the version that gets served. Phase 1 is not a
prototype that gets thrown away; it is the same pipeline with the last mile swapped.

**What Phase 2 adds, measured:** p50 **52 ms** per request against the online store, and
request-time context that moves up to **12 of 16** collections per viewer. What it costs:
the online store cannot scale to zero (~$11/day at CU_1 on this workspace) and the
request-path endpoint bills continuously.

---

## Open, and honest

* **The handoff from `rail_rankings_batch` to the homepage is not built.** Whether their
  service queries the Delta table directly, or populates its own cache from it, is
  theirs to decide and it changes the freshness contract. Worth settling before November.
* **A minutes-cadence job at their MAU is unsized.** The incremental mechanism exists and
  is measured at demo scale only. Sizing it needs their MAU and their feature-change rate.
* **`rail_features` changes force a full refresh.** At a minutes cadence, how often
  rail-grain features change is a design input we do not have.
