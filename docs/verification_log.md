# Verification log

The rule for this repo: **nothing enters the README without a dated entry here.**
Every row is something that was actually run against
`fevm-serverless-lakebase-praneeth`, with the command and the real output.

## 2026-09-07 / 2026-09-08

| # | What | How | Result |
|---|---|---|---|
| 1 | Online store cost | `system.billing.usage ⨝ list_prices` on `usage_metadata.endpoint_id = ep-wild-dawn-d2ao0nf7` | 30.67 DBU/day at $0.52/DBU = **$15.95/day**, flat Sep 2–6. 221.4 DBU ≈ $115 since Aug 31. |
| 2 | Capacity class drives the endpoint floor | `PATCH /api/2.0/feature-store/online-stores/...?update_mask=capacity` `{"capacity":"CU_1"}` then `postgres get-endpoint` | `CU_2 → CU_1` moved the endpoint from **min 8 / max 16 CU → min 4 / max 8 CU**. Store stayed `AVAILABLE`; all 3 synced tables stayed `SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE`. |
| 3 | Lakebase reachable over Postgres | `src/crfs/online.py` + psycopg 3.2.13 from a laptop | Connected as `praneeth.paikray@databricks.com` to db `serverless_lakebase_praneeth_catalog`, schema `crunchyroll_demo`. |
| 4 | The pooled host rejects OAuth | psycopg against `...-pooler.database.us-east-1...` | `SASL authentication failed` on all three pooler IPs; the direct `status.hosts.host` authenticates with the same token. `online.py` now defaults `pooled=False`. |
| 5 | Keyed-read latency, laptop | `store.keyed_read_latency("online_viewer_features", "viewer_id", 30 viewers)` | n=30, **p50 240.9 ms, p95 245.5 ms**, min 239.3. Network-dominated — label it as such. |
| 6 | Example keyed read | `store.keyed_read("online_recent_behavior","viewer_id","v0001")` | `minutes_watched_24h=288.83, skips_24h=1, active_titles_24h=7, last_primary_genre='sci_fi'` in 250 ms. |
| 7 | The 4 on-demand UDFs exist and behave | `DESCRIBE FUNCTION` + a SELECT exercising every branch | `match_scifi=0.3`, `match_action=0.4`, `cross_pop=0.42` (=0.3×(0.5+0.9)); `hour_near=0.8625`, `hour_far=0.0375`, **`hour_wrap_2h=0.75`** (23:00 vs 01:00 read as 2 hours apart, so the circular distance is right); `decay_10min=0.7165`, `decay_16min=0.5738`, `decay_clamped=0.0`, `decay_null=0.0`, `match_all_null=0.0`. |
| 8 | UDF bodies must be block-free | `DESCRIBE FUNCTION EXTENDED cr_hour_affinity_delta` | Stored body showed an indented `return` at column 0 → `IndentationError` inside the executor as `UDF_USER_CODE_ERROR`. Rewritten with conditional expressions only. |
| 9 | Preflight | `./scripts/preflight.sh fe-vm-lakebase-praneeth` | Passed: CLI 1.14.1, auth, schema, warehouse `4d39ac2e32b72a3a`, store `AVAILABLE CU_1`, endpoint `ACTIVE 4-8 CU`, db resource `...databases/db-p78x-mcrka97vph`, LLM `databricks-claude-sonnet-4-5` reachable. |
| 10 | Bundle | `databricks bundle validate --strict -t dev` then `deploy` | Validation OK; deploy created the app and 8 resources. |
| 11 | Notebook 00 | serverless job run | SUCCESS. `titles=132, viewers=300, entitlements=39600, engagement_events≈107k`, plus an empty `engagement_events_stream` with CDF on. `verify.sh` reports history ending 0 days ago. |
| 12 | `verify.sh` catches real drift | `./scripts/verify.sh` | Correctly failed on the two things that were genuinely missing at the time (the new viewer columns, and the not-yet-created retriever / feature-serving endpoints). |

## 2026-09-08, continued

| # | What | How | Result |
|---|---|---|---|
| 13 | Notebook 00 and 01 on the spine | `bundle run crfs_end_to_end` | `generate_data` SUCCESS, `build_features` SUCCESS, `pit_probe` SUCCESS, `train_ranker` SUCCESS |
| 14 | The new on-demand inputs reach Lakebase | psycopg read of `online_viewer_features` | `v0001 -> typical_watch_hour=17.303, hour_concentration=0.5157` in 253 ms. Postgres columns confirmed to include both, plus `last_event_epoch_s` on `online_recent_behavior`. |
| 15 | Idempotent re-publish | notebook 01 exit payload | All three online tables reported `"action": "refreshed"`, each `SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE` at the source commit version. No `AlreadyExists`. |
| 16 | App service principal can read the online tables | `./scripts/grant_app_postgres.sh` | Granted usage, select, and default privileges; verified via `information_schema.table_privileges`: SP can SELECT `online_recent_behavior`, `online_title_features`, `online_viewer_features`. |
| 17 | Dashboard datasets | each query run via `aitools tools query`, then `bundle deploy` + `lakeview get` | All six return rows (sync-health returns empty until notebook 13 has run, which is correct). Deployed dashboard reports **6 datasets, 1 page** — it was empty before, because the JSON had a `dashboard` wrapper instead of top-level `datasets`/`pages`. |
| 18 | Cost after right-sizing | `make cost` | 2026-09-07 shows 21.09 DBU / $10.97 for the Lakebase endpoint (partial day at `CU_1`, down from a flat 30.67 DBU / $15.95 at `CU_2`), plus real serving-inference DBUs now that endpoints are being queried. |

| 19 | **Keyed-read latency, in-region** | notebook 04 on serverless compute | **n=30, p50 2.7 ms, p95 3.8 ms, min 2.5, max 5.1** against `online_viewer_features`. This is the number that matters and the demo never had it before. |
| 20 | The old measurement was wrong by ~420x | same notebook, both paths | `spark.sql` on the FOREIGN table: **p50 1131 ms**. Endpoint query for 25 candidates in one request: **142 ms**, top pick "Fullmetal Alchemist: Brotherhood" at 0.6358. |

| 21 | **Ranker v2 with request-time features** | spine task `ondemand_features` | Registry version 3. Holdout **AUC 0.6696** vs v1 **0.6643** (+0.0053). 38 numeric + 4 categorical, 61,222 train / 7,302 holdout rows. Four on-demand outputs present: `affinity_match`, `affinity_x_popularity`, `hour_affinity`, `session_decay`. |

## Bugs this process found and fixed

| Bug | Symptom | Fix |
|---|---|---|
| Demo clock frozen at 2026-08-31 | "Last 24h" online features were a week older than the wall clock the freshness beat used | `end_date` defaults to yesterday; one `demo_now` in `config.py` |
| `publish_table` is a create, not an upsert | `AlreadyExists: Failing setup of Delta sync table` on any re-run | `ops.publish_or_refresh` publishes once, then starts a pipeline update |
| Unpublishing leaves the Postgres table behind | UC showed no `online_viewer_features` at all, yet publish still failed with `AlreadyExists` — the name was taken in the *Postgres* schema | `ops.drop_synced_if_exists(..., online_store=...)` drops both, and `publish_or_refresh` clears an orphan before publishing |
| `psycopg` `query()` on DDL | `the last operation didn't produce records` after `DROP TABLE` | Return early when `cur.description is None` |
| `wait_for_sync` return shape | `KeyError: 'name'` | It now includes the table name |
| UDF `COMMENT` with an apostrophe | `PARSE_SYNTAX_ERROR` near `s` | Apostrophes doubled in `_fn` |
| DAB dev-mode prefixes 3-level model names | `CreateRegisteredModel name "dev_..._catalog.schema.model" is not a valid name` | `registered_models` removed from the bundle; notebooks own registration |
| App references endpoints that do not exist yet | `cannot create resources.apps: Endpoint 'crunchyroll-candidate-retriever' does not exist (404)` | `scripts/deploy_app.sh` declares only endpoints that currently exist |
| `bundle deploy` cannot update an existing app | `Invalid update mask ... forward_user_access_token` on every deploy after the first | App moved out of the bundle to `scripts/deploy_app.sh`; reference resource kept in `docs/` |
| `spark.read.parquet("/tmp/...")` | `[DBFS_DISABLED] Public DBFS root is disabled` | Notebook 08 builds the DataFrame directly instead of round-tripping through a file |
| Rolling aggregation over an object column | `DataError: Cannot aggregate non-numeric type: object` on pandas 2.x | `features.py` selects only fact columns and coerces them with `pd.to_numeric` before rolling |
| f-string containing a backslash | `SyntaxError: f-string expression part cannot include a backslash` (Python < 3.12) | Notebook 06's two offending lines rewritten |
| Empty dashboard | Deployed dashboard had 0 datasets, 0 pages | JSON rebuilt in the real Lakeview shape: top-level `datasets` with `queryLines`, `pages[].layout[].widget` inline |
| `verify.sh` looked up `information_schema` under the schema | False "missing columns" failure | `information_schema` is catalog-level |
| **Code stranded inside `%md` cells** | Notebooks 10 and 11 reported SUCCESS in 40-52 s having done nothing: every code cell followed a `# MAGIC %md` header with no `# COMMAND ----------` between them, so Databricks treated it all as markdown | 18 separators inserted; a scanner now checks every notebook for the pattern. This is the most dangerous defect found — it fails *silently green* |
| `fe.get_online_store(name)` positionally | `TypeError: takes 1 positional argument but 2 were given` | Keyword only; notebook 08 now calls `ops.publish_or_refresh` |
| UDF integer params declared `INT` | `ValueError: FeatureFunction argument column 'genre_action' has type 'bigint' and parameter 'g_action' has type 'int'` | All integer UDF params declared `BIGINT` — pandas int64 becomes Delta bigint and FeatureFunction type-matches exactly |
| `feature_functions=` kwarg | `TypeError` / `NameError` in notebook 06 | FeatureLookups and FeatureFunctions share the single `feature_lookups=` list |

| 22 | **TRIGGERED freshness, genuinely moving** | spine task `freshness_triggered`, with the new assertion in place | Online row `38.5 min / slice_of_life / 2 titles` → `142.0 min / sci_fi / 3 titles`. **17 of 25 candidates re-scored, max abs delta 0.0271.** TRIGGERED sync 39.3 s wall clock; keyed read 3 ms; ranker queries 147 ms then 138 ms. The top pick (*Mushoku Tensei*) held — the ordering underneath moved, #1 did not. |
| 23 | Latency reproduced on a second run | spine task `smoke_query` | keyed read n=30 **p50 2.7 ms, p95 3.3 ms**; `spark.sql` on the FOREIGN table **p50 1972 ms**; endpoint 149 ms for 25 candidates. `request_epoch_s` present in the payload. |

| 24 | **Feature Serving endpoint, no model in the path** | `07` green, then queried independently from a laptop | Spec `crunchyroll_viewer_feature_spec` (3 lookups + 4 on-demand functions), endpoint `crunchyroll-viewer-features` READY. Response arrives under `outputs`, not `predictions`. Notebook-measured query 111.9 ms; from a laptop 1144 ms cold then **343-350 ms warm**. |
| 25 | The endpoint returns the values the freshness demo wrote | same query | `minutes_watched_24h: 142.0`, `last_primary_genre: 'sci_fi'` — the exact row notebook 05's burst produced. Two access paths, one governed value. |
| 26 | **On-demand features really are computed per request** | three queries, identical stored features, different request context | `hour_affinity` **0.5244** at `hour_of_day=21` vs **0.2335** at `hour_of_day=9`. `session_decay` **0.1629** for a now-request vs **1.0** for a request timestamped 6 h earlier (elapsed goes negative, the guard clamps to 0 minutes, so decay is 1.0 — the documented behaviour, worth knowing before someone reads it as a bug). `affinity_match` stays **0.0601** across all three, correctly, since viewer and title did not change. |
| 27 | Missing lookup key degrades instead of 500ing | query with `viewer_id='v9999_does_not_exist'` | `minutes_watched_24h: None`, `affinity_match: 0.0`, `session_decay: 0.0`. This is exactly why every UDF guards `None` — an unguarded one raises *inside model serving*. |

| 28 | **Retriever, end to end except its endpoint** | `08` green | recall@60 **0.9948** for SVD vs **0.6550** popularity vs **0.4508** random. `viewer_embedding_current` published; `online_viewer_embedding` reports `SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE`. `retriever_registered: false` — UC registration of the pyfunc is unresolved, the endpoint deployment is skipped, and the notebook says so in its exit payload rather than failing or hiding it. |
| 29 | Event producer | `11` green | `mode=loop`, 2 events/second for 10 minutes, **3,412 rows** in `engagement_events_stream`. |

**Caveat on that recall number, before it reaches a slide:** 0.9948 on 300 synthetic
viewers whose genre affinities the generator planted is close to "the SVD recovered the
generator", not evidence about real traffic. The *comparison* against popularity and
random is the meaningful part; the absolute value is not.

| 30 | **CONTINUOUS publish to Lakebase, live** | `10` green | `online_session_features` reports `SYNCED_TABLE_ONLINE_CONTINUOUS_UPDATE`, pipeline `eff5e3f9-3037-4442-838a-bfedd3e791b8`. All five online tables now exist in Postgres: `online_viewer_features`, `online_title_features`, `online_recent_behavior`, `online_viewer_embedding`, `online_session_features`. |
| 31 | Keyed reads against the continuous table | `10`, in-region | **p50 2.8 ms, p95 4.3 ms** (n=30). `spark.sql` on the same FOREIGN table: **3478.9 ms** — about 1,240x. Reproduces the earlier 2.7/3.3 ms figures on a different table. |
| 32 | Commit-to-Postgres, from the platform's own metadata | `10` sync state | `delta_commit_timestamp 00:00:11Z` → `sync_end 00:00:14.79Z` = **~3.8 s** for the Lakebase leg. |
| 33 | End-to-end event→online-visible, **with a large caveat** | `10`, 8 cycles, 500 ms poll | **p50 80.8 s, p95 104.2 s.** This is NOT the platform's floor and must not be quoted as one -- see below. |

### Why the end-to-end number is 81 seconds

Each measurement cycle emits an event, runs an `availableNow` drain, then polls Postgres
until the value appears. On serverless notebook compute a fresh streaming query costs
roughly 30-60 s to start, and that startup happens **once per cycle**. It dominates
everything else:

```
event -> Delta                     sub-second (a plain append)
availableNow query startup + drain ~30-60 s   <-- this is the whole number
Delta commit -> Lakebase           ~3.8 s     (measured from the sync metadata)
keyed read                         2.8 ms
```

So 81 s measures *this configuration*, whose aggregation leg cannot be always-on
(`INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`). An always-on query on classic compute or a
Lakeflow pipeline removes the startup entirely and leaves the ~4 s sync plus the read.
The notebook says this, and the README quotes the decomposition rather than the headline.

## Cross-check against the official docs (2026-09-08)

Checked everything in this log against
<https://docs.databricks.com/aws/en/machine-learning/feature-store/online-feature-store>.

**Confirmed by the docs** — capacity values `CU_1|CU_2|CU_4|CU_8`; *"Lakebase scale-to-zero
is not supported"* and *"Online stores continuously incur costs. Delete online stores that
are no longer needed."*; CDF required for both `TRIGGERED` and `CONTINUOUS`; primary key
constraint required and non-nullable; up to 3 read replicas (4 instances).

**One thing this repo had wrong, now fixed.** The docs are explicit that
`w.feature_store.delete_online_table()` is *"the only recommended method"* for deleting an
online table, because *"it removes the table from both Unity Catalog and the database.
Other methods such as the Databricks SQL command DROP TABLE or the Python SDK command to
delete a synced table do not delete the table from underlying database storage."*

That is precisely the failure this log records at finding #3: deleting the **synced table**
left the Postgres table behind, so the next `publish_table` hit `AlreadyExists` while UC
showed nothing. The workaround (drop the Postgres table over psycopg) was treating a
symptom. `ops.drop_synced_if_exists` now calls the documented API first and keeps the old
path only to clean up tables published before the fix.

**Gotchas the docs list that this repo had not accounted for:**

| Documented limitation | Why it matters here |
|---|---|
| *"An online table's catalog name must match its underlying database name... if they differ, the model serving endpoint fails to deploy."* | Ours match — PG `current_database()` is `serverless_lakebase_praneeth_catalog`, same as the UC catalog. Verified, not assumed. |
| *"When a feature table is published to multiple online tables, model serving and feature serving endpoints always resolve to the oldest online table based on the creation timestamp."* | Real risk in this repo, which republishes and recreates tables repeatedly. A stale online table left behind would silently keep serving. Teardown must remove old ones, not just add new. |
| `filter_condition`, `checkpoint_location`, `mode`, `trigger`, `features` are **unsupported** on `publish_table` | They appear in the signature. We pass none of them. |
| *"Skipping publishing to online table '...' because the feature sync pipeline is already running."* Only one sync per online table at a time. | Explains why parallel job submissions against the same table were fragile during this build. |
| `get_status()` is the documented way to wait for a publish to finish | This repo polls `GET /api/2.0/database/synced_tables/{name}` instead, which also gives `detailed_state` and the commit versions the freshness assertions need. Worth revisiting for simplicity. |

**No latency figures or freshness SLAs appear anywhere on that page.** Every latency number
in this repo is our own measurement, which is the right way round.

## Codex review pass (2026-09-08)

An independent Codex review of the whole repo found **8 defects that would fail at
runtime** and 4 lesser ones. All are fixed. The ones that mattered:

| Finding | Symptom | Fix |
|---|---|---|
| `app/app.py` called `w.sql(...)` | No such method on `WorkspaceClient`; the `try/except` swallowed the `AttributeError`, so the ranked list and funnel were **always empty** on every page load | `w.statement_execution.execute_statement(...)` |
| `app/app.py` burst handler called `rank_candidates` with 3 of 8 args | `TypeError` the moment the freshness button was clicked | full call, and it consumes the returned ranked frame rather than treating it as a score list |
| `07` built `w` from `dbutils...currentWorkspace()` | `Py4JError: Method notebook_context([]) does not exist` — killed the task twice on the live spine | `WorkspaceClient()` |
| `11` had `# MAGIC # COMMAND ----------` between two `%pip` lines | Not a separator inside a magic block, so `databricks-feature-engineering` was never installed → `ModuleNotFoundError` on a fresh kernel | one `%pip` line installing both |
| `12`'s generated `tools.py` had 3 `SyntaxError`s | Stray `"` after the f-string in each `except` branch; `exec_module` would raise at model load, making the agent dead on arrival | return statements corrected; the rendered template now parses |
| `12` used `statement_execution.execute(...)` and `result.result_set.rows[i].values[j]` | Method does not exist; result shape wrong | `execute_statement(...)` and `result.result.data_array` |
| `12` used `mlflow.models.log(...)` | Not an MLflow API | `mlflow.pyfunc.log_model(python_model=...)` |
| `08` created `viewer_embedding_current` with `saveAsTable` | No primary key, so `publish_table` refused it: `Tables without primary keys cannot be published on Databricks Online Feature Store` | `fe.create_table(primary_keys=["viewer_id"])`, and the notebook now detects a non-feature-table predecessor and replaces it |
| `app/app.py` `hour_override = int(time.time() % 3600 / 60)` | Minutes-within-hour (0-59), not hour-of-day; values 24-59 aliased back to 0-11 through the UDF's modulo | `dt.datetime.now().hour` |
| `04` omitted `request_epoch_s` from the request | Against the v2 ranker, `cr_session_decay` received `None` and returned 0.0 for every row — the feature was silently dead | payload built by `candidates.request_records`, so notebook 04, notebook 05 and the app share one contract |
| `06` hardcoded v1's AUC | A re-run of notebook 02 would leave the comparison quoting a stale baseline | v1's `holdout_auc` read from the model registry, constant kept only as a fallback |
| `teardown.sh` assumed cwd for `sys.path` | Run from elsewhere, step 4 silently skipped and synced tables kept billing | repo root resolved from `$0`, and the failure now exits non-zero with instructions |

### Defects that only a live run could surface

Every one of these passed a static review and failed on the workspace:

| Finding | Symptom | Fix |
|---|---|---|
| `08` wrote model artifacts to `/tmp` | `PermissionError: [Errno 13] Permission denied: '/tmp/cr_retriever/item_factors.pkl'` — `/tmp` is not reliably writable on serverless | `tempfile.mkdtemp()` |
| `08` passed `input_example` alongside an explicit signature | MLflow infers an inputs-only signature from the example, which overrides the explicit one, and UC refuses: *"signature that includes only inputs. All models in the Unity Catalog must be logged with a model signature containing both input and output type specifications"* | explicit `ModelSignature` only, no `input_example` |
| `07` created a feature spec that already existed | `RESOURCE_ALREADY_EXISTS: Routine or Model 'crunchyroll_viewer_feature_spec' already exists` — and a spec cannot be dropped while an endpoint serves it | delete the endpoint, then the spec, then recreate; reuse on a residual collision |
| `07` queried the endpoint immediately after creating it | `ResourceDoesNotExist: The given endpoint does not exist` from `POST /invocations` — provisioning had not finished | poll `serving_endpoints.get(...).state` until `READY` / `NOT_UPDATING` |
| `07` read the response from `resp.predictions` | `TypeError: 'NoneType' object is not iterable`. A **Feature Serving** endpoint does not answer in `predictions` the way a model endpoint does — feature values arrive under `outputs` | read whichever field is present and print the raw response keys, so the notebook documents the shape instead of assuming it |
| `08` two-step registration via `mlflow.models.set_signature` | `MlflowException: Failed to download an "MLmodel" model file from "runs:/.../cr_retriever"` — the artifact is not reachable that way here | `fe.log_model(..., infer_input_example=True)`: the client runs the model on an example from the training set and infers the **output** schema too, which is exactly what UC requires |

The `08` sequence is a good example of why probing beats guessing. Three attempts failed
(`signature=` kwarg ignored, `input_example` overriding it, `set_signature` unable to reach
the artifact) before introspecting the real API on the workspace showed the intended
parameter:

```
fe.log_model(*, model, artifact_path, flavor, training_set=None,
             registered_model_name=None, await_registration_for=300,
             infer_input_example=False, extra_pip_requirements=None, **kwargs)
```

| `10` created the checkpoint volume through the SDK | `AttributeError: 'VolumesAPI' object has no attribute 'get_by_name'`, then `TypeError: VolumesAPI.create() missing 1 required positional argument: 'volume_type'` | the volume is a bundle resource; verify with `DESCRIBE VOLUME` instead of creating it |
| `foreachBatch` cannot use any SDK-backed client | `ValueError: default auth: cannot configure default credentials` raised **inside the foreachBatch Python process**. That worker is a separate process with no Databricks credentials, so `FeatureEngineeringClient()` — and therefore `fe.write_table` — is unavailable there | upsert with a plain Delta `MERGE` via `batch_df.sparkSession`. Same operation `fe.write_table(mode="merge")` performs; the table stays a registered feature table with CDF on, so the CONTINUOUS publish to Lakebase is unaffected |
| Temp view inside `foreachBatch` | `[NOT_SUPPORTED_WITH_SERVERLESS] GLOBAL TEMPORARY VIEW is not supported` | the `DeltaTable` merge builder against `batch_df.sparkSession` — no view at all |
| Non-windowed streaming aggregation in append mode | `STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION: Invalid streaming output mode: append` — `groupBy("viewer_id")` has no time window, so a watermark does not make append legal | `outputMode("update")`, which is also the right semantics for a merge |
| Serverless notebooks reject infinite triggers | `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED: Trigger type ProcessingTime is not supported for this cluster type. Use a different trigger type e.g. AvailableNow, Once.` | `availableNow` drains, with the three-leg continuity table written into the notebook. The Lakebase leg (`publish_mode="CONTINUOUS"`) is a platform-run streaming pipeline and stays genuinely continuous |
| `11` re-issued `CREATE TABLE` for a table notebook 00 owns | `INVALID_PARAMETER_VALUE: Missing cloud file system scheme` from Unity Catalog | guard the DDL with `spark.catalog.tableExists` so it only runs on a genuinely cold start |
| `08` excluded `viewer_id` from the training set | Six consecutive `MlflowException: ... a signature that includes only inputs` failures. The real cause was three layers from the message: `fe.log_model` infers the output schema by **running the model** on an example drawn from the training set, `predict()` reads `viewer_id`, and `exclude_columns=["viewer_id"]` had removed it — so the run failed, no output schema was inferred, and UC refused the registration | keep `viewer_id` in the training set |

That last one is the clearest lesson in this log. Five of the six attempts changed the
*logging call* (`signature=`, `input_example`, `set_signature`, `infer_input_example`, the
return dtype) because that is what the error message pointed at. The defect was in the
training set two cells earlier. When an error repeats through several plausible fixes,
the cause is usually not where the message says it is.

### Two more the static review could not see

Codex reviewed statically, so these only surfaced by running the thing:

| Finding | Symptom | Fix |
|---|---|---|
| `wait_for_sync` could be satisfied by the **previous** sync | `freshness_triggered` passed green with `max_abs_delta: 0.0` and `n_moved: 0` — the endpoint re-ranked against stale online features, so the demo's headline beat was doing nothing while reporting success | `ops.refresh_and_wait` anchors on `sync_end_timestamp` advancing, and notebook 05 now **raises** if the online row did not change |
| ...and then the fix was too strict | `TimeoutError: online_title_features: no new sync completed within 600s`. The data regenerates deterministically from `SEED=42`, so the merge wrote byte-identical rows, there was nothing to sync, and `sync_end` correctly never moved | the wait is satisfied by *either* a new sync *or* `ONLINE` plus `last_processed_commit_version >= source commit` |

The pair is worth keeping in mind together: the first made a broken demo look green, the
second made a correct table look broken. The condition has to distinguish "stale" from
"already current", and only the source commit version can do that.

Codex confirmed clean: all of `src/crfs/` (including the circular-mean watch-hour maths
and the pandas 2.x dtype handling), `00`, `01`, `02`, `02b`, `05`, `10`, `13`, `99`,
`databricks.yml`, `resources/`, `Makefile`, `deploy_app.sh`, `verify.sh`, and the
README's numbers against this log.

## Still to verify

Tracked honestly rather than assumed. Everything above this line was actually run.

- **Retriever endpoint** (`08`): UC registration of the pyfunc is unresolved, so the
  endpoint is never deployed and the funnel's retrieval hop is unmeasured. The SVD,
  the published feature table, its online mirror and the recall metrics are all verified.
- **Agent** (`12`): a real answer quoting real feature values, and which auth path it needed.
- **The app's rendered UI.** It deploys, starts, and its logs are clean, but nobody has
  opened the page — the six regions, the burst button and the ops footer are unverified
  visually.
- **The dashboard's rendered widgets.** Six datasets are deployed and every query returns
  rows, but `sync_health` stays empty until notebook 13 populates `crfs_ops_sync_log`.
- **Cost after right-sizing over a full day.** Partial-day billing already shows the drop
  from $15.95 to about $10.97; the steady-state `CU_1` figure needs 24 hours.

No number from this list appears in the README. When one is measured it goes above, with
the command that produced it.
