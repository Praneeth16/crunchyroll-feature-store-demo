# Risks, and what to do when each one bites

Ordered by how likely it is to matter on the day.

## 1 · The app cannot read the Postgres schema

**Symptom.** The raw-Lakebase panel shows a yellow badge and reads through the
Feature Serving endpoint instead of psql. Logs show
`permission denied for schema crunchyroll_demo`.

**Why.** The app's `postgres` resource auto-creates the service principal's Postgres
role, but the schema and its synced tables are owned by whoever published them. A
role that can connect still cannot select. Worse, a one-time
`GRANT SELECT ON ALL TABLES` does not cover tables published *later*.

**Fix.** `./scripts/grant_app_postgres.sh <profile>` — it issues `GRANT USAGE`,
`GRANT SELECT ON ALL TABLES`, and `ALTER DEFAULT PRIVILEGES` so future tables are
covered. Re-run it after any new `publish_table`. `make deploy` runs it for you.
The app degrades rather than dying, so this is never demo-fatal.

## 2 · Cost keeps running

**Symptom.** A bill for a demo nobody is watching.

**Why.** Online stores cannot scale to zero. Measured: **$15.95/day at `CU_2`**,
about half that at `CU_1`.

**Fix.** `make teardown-cost` between rehearsals; `make cost` before you forget.
Details in [cost_and_sizing.md](cost_and_sizing.md).

## 3 · Re-running notebook 01 collides with a live sync

**Symptom.** `AlreadyExists: Failing setup of Delta sync table: Destination table
online_viewer_features already exists in schema crunchyroll_demo` — and Unity
Catalog shows no such table, which makes it look impossible.

**Why.** Two objects back one online table: the UC FOREIGN entry plus the pipeline,
and the underlying **Postgres** table. Deleting the synced table removes the first
pair and leaves the second. The error message is about the Postgres schema.

**Fix.** Already handled: `ops.drop_synced_if_exists(..., online_store=...)` drops
both, and `ops.publish_or_refresh` clears an orphan before publishing and refreshes
instead of re-publishing when the table is healthy. If you meet this outside those
helpers, drop the Postgres table with `scripts/lakebase_explore.sh` and retry.

## 4 · Scores drift while the demo sits idle

**Symptom.** The ranking changes between two identical questions.

**Why.** `session_decay` is a function of the request clock, by design — it is the
feature that proves on-demand computation is real.

**Fix.** The UDF clamps at 1440 minutes, and both the app and notebook 05 freeze
`request_epoch_s` for the whole comparison. The app's clock toggle defaults to
frozen; turn it on deliberately, during the freshness beat.

## 5 · Ranker v2 is not better than v1

**Symptom.** Holdout AUC at or below v1's 0.6643 after adding request-time features.

**Fix.** Report it. Four extra features on a synthetic 107k-event dataset are not
guaranteed to move AUC, and the demo's claim is about the *mechanism*, not the lift.
If it matters, serve v1 and v2 side by side with a traffic split and let the
inference-table dashboard adjudicate — that is a better beat than a fabricated
improvement.

## 6 · `psycopg` is unavailable or blocked

**Symptom.** Latency measurement and the raw-feature panel fail.

**Fix.** Notebooks install `psycopg[binary]` in their first cell. From a laptop,
`scripts/measure_online_latency.py` covers it. Failing both, read the same values via
the Feature Serving endpoint — the numbers are identical, only the latency
attribution changes. Remember the pooled host rejects OAuth (`SASL authentication
failed`); use the direct host, which `online.py` already does.

## 7 · The agent cannot reach the Feature Serving endpoint

**Symptom.** The tool call returns a permission error at request time.

**Why.** Resource-based auth passthrough has to be granted to the endpoint's
principal, and it is not always available.

**Fix.** Notebook 12 takes `auth_mode`. Switch it to `secret`, which puts a PAT in
`environment_vars` from a Databricks secret scope. One parameter, not a rewrite.

## 8 · The Zerobus task fails

**Symptom.** `produce_events` errors on import.

**Why.** `databricks-zerobus-ingest-sdk` cannot pip-install at runtime on serverless;
it is declared as a task `environments:` dependency in `resources/jobs.yml`.

**Fix.** `mode=loop` is the default and needs nothing extra. Zerobus is the
production ingest story, not a dependency of the demo.

## 8b · `bundle deploy` cannot update an existing app

**Symptom.** The first deploy creates the app; every deploy after that fails, and
takes the whole bundle deploy down with it:

```
POST /api/2.0/apps/<app>/update -> 400 INVALID_PARAMETER_VALUE
Invalid update mask. Only description, budget_policy_id, usage_policy_id, resources,
user_api_scopes, compute_size, compute_min_instances, compute_max_instances,
git_repository, git_source, telemetry_export_destinations, compatibility_flags are
allowed. Supplied update mask: ... forward_user_access_token ...
```

**Why.** Databricks CLI v1.14.1 puts `forward_user_access_token` in the update mask
unconditionally, and this workspace's Apps API rejects it. The bundle never sets that
field. Reproduced twice with zero file changes.

**Fix.** The app is not a bundle resource. `scripts/deploy_app.sh` uses
`databricks apps create` / `apps create-update` plus `apps deploy`, which is the
supported path, and resolves the Lakebase database id and burst job id at run time
rather than hardcoding them. The bundle resource is kept for reference at
`docs/app.resource.yml.reference` for whenever the CLI catches up. `make deploy`
handles the bundle; `make deploy-app` handles the app.

Related: the script waits for the app's compute to leave `DELETING` before calling
create-update, because `create-update` on a deleting app returns
`App compute needs to be ACTIVE or STOPPED to update.`

## 8c · DBFS root is disabled

**Symptom.** `[DBFS_DISABLED] Public DBFS root is disabled. Access is denied on path:
/tmp/...`

**Why.** A bare path handed to Spark (`spark.read.parquet("/tmp/x.parquet")`) resolves
against DBFS root, which is off on this workspace. Local driver paths are fine for
model artifacts — MLflow packages them — but never for Spark I/O.

**Fix.** Build the DataFrame directly (`spark.createDataFrame(pdf)`), or write to the
UC volume `cfg.checkpoint(...)` / `/Volumes/<catalog>/<schema>/crfs_ops/...`. Notebook
08 hit this and now does the former.

## 9 · DAB dev-mode name prefixing

**Symptom.** `CreateRegisteredModel name "dev_..._catalog.schema.model" is not a
valid name`, or an app name that is not a legal app name.

**Why.** `mode: development` prefixes resource names, and a UC model name is
three-level.

**Fix.** Already handled: the bundle declares no `registered_models` (the notebooks
own registration) and the app sets `name: ${var.app_name}` explicitly.

## 10 · The demo clock drifts

**Symptom.** "Last 24 hours" features that are days old — the bug the first version
of this demo shipped with.

**Fix.** `end_date` defaults to yesterday, every as-of window derives from one
`demo_now`, and `scripts/verify.sh` fails if history ends more than two days ago.
Run `make verify` before presenting.

## 11 · Cold starts on stage

**Symptom.** The first query after idle takes 30–60 seconds.

**Fix.** Endpoints scale to zero to save money, which costs you the first request.
Pre-warm during an earlier beat, or set `scale_to_zero_enabled: false` and
`compute_min_instances: 1` on the app for demo day, and put them back afterwards.

---

## 12 · The rail ranker never scales to zero, and that is the point

**Likelihood: certain. It is a design decision, not a failure.**

`crunchyroll-rail-ranker` is created with `scale_to_zero_enabled: false` and a
provisioned-concurrency floor of 4. It therefore bills continuously from the moment
notebook 23 finishes, whether or not anyone queries it.

That is correct for something in a homepage request path — scale-from-zero has no
documented latency SLA and is measured in seconds — but it is a second always-on
charge on top of the online store. Both are deliberate and both need an owner.

* `make cost` shows what it is billing.
* `make teardown-cost` deletes it first, before the other endpoints, because it is
  the one costing money while idle.
* `scripts/verify.sh` **fails** if `scale_to_zero` is not disabled, so nobody
  accidentally benchmarks a demo-configured endpoint and quotes the number.

## 13 · Route optimization cannot be turned on later

**Likelihood: certain if you create the endpoint by hand first.**

`route_optimized` is a **create-time only** property. There is no update path. If the
endpoint already exists without it, the only way to get it is to delete and recreate —
a container rebuild, several minutes, and the inference table's captured history gone.

* Controlled by the `recreate_for_route_optimization` widget, default **`false`**. It
  defaulted to `true` and that was wrong: route optimization is rejected on this
  workspace, so every rerun deleted a working endpoint and rebuilt it (V32). An
  existing endpoint is now updated in place and the mismatch is reported, not acted on.
* Set it to `true` only when you are deliberately rebuilding on a workspace where
  route optimization is enabled. Route optimization is still requested on every
  *create*, and the fallback chain drops it when rejected, so no flag is needed to
  benefit from it on a workspace that supports it.
* The benchmark records `route_optimized=false` beside its numbers rather than
  claiming a path it is not using.
* Calls to a route-optimized endpoint need an OAuth token. `src/crfs/loadtest.py`
  resolves the URL and the auth headers together from the SDK config for exactly this
  reason.

## 14 · A diagnostic can kill a pipeline

**Likelihood: happened, 2026-09-16.**

A direct `psycopg` read against the Lakebase endpoint aborted a serverless kernel with
`exit code 134 (SIGABRT)`. A native abort cannot be caught, so the task failed and
blocked four downstream tasks — after all of its real work had already succeeded.

The rule that came out of it: **measurement code does not run on the pipeline.**
Notebook 21 builds and publishes and asserts; notebook 25 measures, as its own job,
and does the psycopg read last, after every result is already written to Delta and to
the volume. If it aborts, the cost is one diagnostic.

If you hit the same abort in notebook 25, the endpoint's own feature lookups are
unaffected — only the storage-layer attribution is missing. `make bench-local` reads
the same tables from a laptop instead.

## 15 · Position bias is corrected, not eliminated

**Likelihood: inherent.**

The vertical ranker is trained on a homepage log produced by the incumbent policy.
IPS weighting with a measured viewport propensity is a reasonable estimator, and it
assumes the propensity model is right. It is not equivalent to a randomised-exposure
dataset.

Consequences to state out loud rather than discover in a review:

* The NDCG lift in notebook 22 is measured against a **logged** policy on synthetic
  labels. It is evidence that the pipeline works, not a forecast of Crunchyroll's
  lift.
* The ablation without `rail_editorial_rank` exists so the comparison cannot be
  circular. Quote both numbers or neither.
* If `rail_position` is ever missing from the production homepage log, none of this
  is possible. That is the single highest-value logging change to make before a real
  build.

## 16 · No fallback path

**Likelihood: certain, and it is a real gap.**

This POC has no cached previous ranking, no editorial default on timeout, and no
circuit breaker. If the rail endpoint is slow or unavailable, the demo app surfaces
the error and the notebook raises.

A production homepage needs all three, and their design affects the latency budget
more than the model does — a 40 ms p95 with a 150 ms timeout and a cached fallback is
a different system from a 40 ms p95 with no fallback at all. Named here rather than
quietly omitted.

## 17 · The same encoder bug was latent in the watch-next ranker — now fixed

**Status: fixed in source. The endpoint carries the fix only after `train_ranker` and `deploy_ranker` re-run, because the encoder is pickled into the served pyfunc. Kept here because the second instance of it was only found by fixing the first.**

`notebooks/02_train_ranker.py` builds its feature matrix with the same pattern that
broke the rail ranker at serving time:

```python
X[c] = pd.to_numeric(df[c] if c in df else 0, errors="coerce").fillna(0.0)
```

For an absent column that is `pd.to_numeric(0)` — the **int** `0` — and `0 .fillna(...)`
raises `AttributeError: 'int' object has no attribute 'fillna'`. Model Serving reports it
as `Encountered an unexpected error while evaluating the model. Error ''`, with no
traceback (see `docs/verification_log.md` V28).

It has not bitten there because every looked-up feature reaches that endpoint on every
request. It would fire the first time a caller omits an optional column, or a feature
lookup returns nothing for a column rather than a null.

**Now fixed.** This section previously said the fix was out of scope because it would
mean rebuilding and redeploying a working horizontal path. That reasoning stopped applying
the moment the horizontal path was rebuilt anyway (to clear a data-clock drift and a
missing retriever endpoint), so the fix went in with it: both encoder sites in
`notebooks/02_train_ranker.py` now build an explicit
`pd.Series(0.0, index=df.index)`, mirroring `notebooks/22_train_rail_ranker.py`.

**Fixing it surfaced a second instance the original write-up had missed.** This section
described only the numeric branch. The categorical branch has the identical defect:

```python
X[c] = (df[c] if c in df else "unknown").astype(str).map(...)
```

For an absent column that is the bare string `"unknown"`, and `str` has no `.astype` —
`AttributeError: 'str' object has no attribute 'astype'`, surfacing as the same
information-free `Error ''`. Four sites in total across the two functions, not two.

The lesson worth keeping: **the write-up of a latent bug is itself unverified.** It was
produced by reading code, not by running it, and it undercounted the defect by half. A
latent bug that is documented rather than fixed should say which of the two it is.
