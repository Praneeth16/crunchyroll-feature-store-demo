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
