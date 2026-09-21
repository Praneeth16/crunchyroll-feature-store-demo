# The walkthrough — every step, with the numbers it produced

Moved out of the README so the landing page stays a landing page. This is the
guided tour: what each notebook does, what it printed on this workspace, and the
reasoning behind the choices. The README links here from *What runs, in order*.

Every number below was measured on the workspace this demo was built on and has a
row in [verification_log.md](verification_log.md). Prose is not the source of
truth for a model's own metrics -- the registry is.

---

## Step 0 · Raw signals

`notebooks/00_shared/00_data_generation.py` writes five tables into
`serverless_lakebase_praneeth_catalog.crunchyroll_demo`, every one with Change Data Feed
enabled:

| Table | Rows | What it is |
|---|---|---|
| `titles` | 132 | Recognisable anime titles with `primary_genre`, `franchise`, `episode_count`, `is_simulcast` and 8 genre flags |
| `viewers` | 300 | Viewers with latent genre affinities, a tier and a territory |
| `entitlements` | 39,600 | The 300 × 132 matrix of tier × territory × maturity rights — a hard filter, never a feature |
| `engagement_events` | ≈107,000 | 90 days of impressions, plays, skips and completes, generated with real signal so the model has something honest to learn |
| `engagement_events_stream` | 0 at first | The live-event landing table, seeded empty. Columns: `event_id`, `viewer_id`, `title_id`, `event_ts`, `event_type`, `watch_seconds`, `surface`, `device`, `locale`, `produced_epoch_ms` |

`engagement_events_stream` exists so live events never touch the training corpus. The
first version of this demo appended `burst-*` rows straight into `engagement_events`,
which mutated the corpus every time the freshness beat ran, so training stopped being
reproducible. `produced_epoch_ms` on that table is the producer's own clock, carried
all the way through to the online row — subtracting two readings of that single clock is
how the streaming freshness number avoids a clock-skew argument.

History ends **yesterday** by default (`end_date=""` resolves through
`Config.end_date_resolved` in `src/crfs/config.py`). The first version of this demo
hardcoded `END_DATE = "2026-08-31"`, so by the time it was presented the "last 24
hours" online features were a week older than the wall clock the freshness beat used.
`scripts/verify.sh` now fails if `max(event_ts)` falls more than two days behind
`current_date`.

![Catalog Explorer](images/01-catalog-explorer.png)
*`images/01-catalog-explorer.png` — the schema in Catalog Explorer.*

![engagement_events](images/02-events-table.png)
*`images/02-events-table.png` — `engagement_events` sample rows.*

> "Nothing here is feature-store specific yet. Viewing events, catalog metadata,
> entitlements — the governed raw signals any media company already has."

## Step 1 · Define once, publish to Lakebase

`notebooks/00_shared/01_feature_engineering.py` builds four feature tables from definitions that
live in `src/crfs/features.py` — imported here and by `05_freshness_triggered.py` and
`10_streaming_continuous.py`. That sharing is not tidiness: the first version re-derived
the recent-behaviour maths separately in 01 and 05, which is exactly the
training/serving skew this demo argues against, committed in its own source.

The four functions that define every offline value:

| Function in `src/crfs/features.py` | Produces | Feature table |
|---|---|---|
| `build_viewer_timeseries()` | Daily snapshots: 8 genre affinities, completion propensity, `typical_watch_hour`, `hour_concentration`, 7/30-day rolling windows | `viewer_features_ts` (PK `viewer_id, ts`, `timeseries_column="ts"`) |
| `viewer_current_from_ts()` | The latest row per viewer, taken from the same time series so offline and online cannot diverge | `viewer_features_current` |
| `build_title_features()` | `popularity_30d`, rating, recency, 8 genre flags | `title_features` |
| `build_recent_behavior()` | `minutes_watched_24h`, `skips_24h`, `active_titles_24h`, `last_primary_genre`, `last_event_epoch_s` | `recent_behavior_current` |

`typical_watch_hour` is a **circular** mean — hours are on a clock, so the average of
23:00 and 01:00 is midnight, not noon. It is computed with `sin`/`cos` and `atan2`, and
`hour_concentration` is the resultant length, which doubles as a confidence weight for
the `hour_affinity` UDF in step 6.

What notebook 01 then does, in order:

- **Primary keys, non-null, Change Data Feed** — the contract an online store needs.
  `upsert_feature_table()` (defined in the notebook) writes with
  `fe.write_table(mode="merge")` when the schema is unchanged, and drops and recreates
  only when the column set actually changed — deleting the dependent synced table first,
  via `ops.drop_synced_if_exists()`. The first version dropped unconditionally, which
  destroyed the source of a live synced table on every re-run.
- `fe.create_online_store(name="crunchyroll-online-store", capacity="CU_1")` provisions
  a managed Lakebase project, its `production` branch and its `primary` endpoint.
- `fe.publish_table(..., publish_mode="TRIGGERED")` for the three tables listed in the
  notebook's `PUBLISH` list → `online_viewer_features`, `online_title_features`,
  `online_recent_behavior`.
- The publish itself goes through `ops.publish_or_refresh()`, because `publish_table` is
  a **create**, not an upsert: called twice it raises `AlreadyExists`. That helper
  publishes the first time and calls the synced-table refresh every time after.
- The wait is `ops.wait_for_sync()` / `ops.refresh_and_wait()`, which poll
  `GET /api/2.0/database/synced_tables/{full_name}` until
  `triggered_update_status.last_processed_commit_version` reaches the commit version
  `ops.source_commit_version()` read from `DESCRIBE HISTORY` — not a `sleep`. There is
  no `time.sleep()` left anywhere in the pipeline; the first version had about four
  minutes of it.

![Feature table detail](images/03-feature-table-viewer.png)
*`images/03-feature-table-viewer.png` — `viewer_features_current` in Catalog Explorer,
showing the primary key and the feature-table badge.*

![Online tables](images/04-online-tables.png)
*`images/04-online-tables.png` — the published online tables and their sync state.*

![Lakebase project](images/13-lakebase-project.png)
*`images/13-lakebase-project.png` — the Lakebase project `crunchyroll-online-store`
that `fe.create_online_store` provisioned.*

The online store is a real Postgres database. Read it the way an application would —
`scripts/lakebase_explore.sh` opens `psql` with a freshly minted OAuth token:

```bash
./scripts/lakebase_explore.sh <profile> <catalog>
# \dt crunchyroll_demo.*
# SELECT viewer_id, minutes_watched_24h, last_primary_genre
#   FROM crunchyroll_demo.online_recent_behavior WHERE viewer_id = 'v0001';
```

From Python, `src/crfs/online.py` is the same access path used by the app, the latency
measurement and the freshness poller. `OnlineStore.keyed_read()` does one keyed
`SELECT`, `keyed_read_latency()` times a batch of them, and `wait_for_value()` polls
until a column reaches a target — that last one is what makes the streaming latency
measurable. `scripts/measure_online_latency.py` is the standalone laptop version.

Measured two ways, and the gap is the point:

```
Keyed read via Postgres, from in-region compute      n=30  p50   2.7 ms   p95  3.8 ms
Keyed read via Postgres, from a laptop               n=30  p50 240.9 ms   p95 245.5 ms
Same rows via spark.sql on the FOREIGN table               p50 1131 ms
Endpoint query, 25 candidates in one request                    142 ms
```

The single-digit milliseconds are what the serving path actually costs. The laptop
number is network round trip, which is honest but says more about wifi than about
Lakebase. The 1131 ms is serverless SQL planning plus a federated read — the first
version of this demo reported *that* as online-store latency, and it is roughly 420×
the truth.

One gotcha worth knowing: connect to the endpoint's **direct** host. The
`read_write_pooled_host` rejects OAuth tokens with `SASL authentication failed`.

## Step 2 · Point-in-time training

`notebooks/10_horizontal/02_train_ranker.py` opens with the proof: a sample of impressions joined
against `viewer_features_ts` with `timestamp_lookup_key="ts"`, printing feature values
**at impression time** beside today's values.
`notebooks/10_horizontal/02b_pit_probe.py` is the same proof standalone, so it can be shown without
the training run around it.

![PIT proof](images/06-pit-proof.png)
*`images/06-pit-proof.png` — the same feature, as of impression time and as of now.*

> "Those two columns differ because the viewer kept watching after that impression.
> The model trains on what we knew *then*. Hand-built training joins get this wrong
> constantly, and it always flatters the offline metrics."

Then `fe.create_training_set()` assembles the PIT-correct frame from `FeatureLookup`s
against `viewer_features_ts`, `title_features` and `recent_behavior_current`, and
`fe.log_model(..., training_set=..., registered_model_name=...)` registers the ranker in
Unity Catalog **with the feature spec inside it**. That is the mechanism the whole demo
rests on: at serving time nothing in the request has to name a feature table, because
the model already carries the lookup graph.

![Model version](images/08b-model-version-spec.png)
*`images/08b-model-version-spec.png` — the registered model version with its embedded
feature spec.*

## Step 3–4 · Serving with automatic feature lookup

`notebooks/10_horizontal/03_deploy_ranker_endpoint.py` creates the endpoint
`crunchyroll-watch-next-ranker` with AI Gateway inference tables enabled, writing
requests and responses to `cr_ranker_inference_payload`. No serving code touches a
feature table — the registered model already knows what it needs and where it lives.
The notebook takes `model_version` as a widget and is idempotent, retrying on
`ResourceConflict`, which is what lets the spine call it twice (`deploy_ranker` for v1,
`deploy_ranker_v2` after the on-demand features land).

![Endpoint ready](images/09-endpoint-ready.png)
*`images/09-endpoint-ready.png` — the ranker endpoint READY, with inference tables on.*

`notebooks/10_horizontal/04_query_ranker.py` plays the application, building its request through
`src/crfs/candidates.py` — `candidates()` picks the 25 titles, `request_records()`
builds the payload, `query_ranker()` sends it and `rank()` sorts the response. The app
and the agent use those same four functions, so there is exactly one definition of what
a request looks like. It sends only this:

```json
{"viewer_id": "v0xxx", "title_id": "t0042", "surface": "post_play",
 "device": "tv", "locale": "en-US", "hour_of_day": 21, "request_epoch_s": 1788...}
```

Seven fields — `REQUEST_KEYS` in `src/crfs/candidates.py`. No feature values, no feature
names, no table names — the
endpoint fetches 38 numeric and 4 categorical features from Lakebase itself and computes
four more at request time.

![Ranked output](images/10-query-ranked.png)
*`images/10-query-ranked.png` — 25 candidates in, ranked titles out.*

![Inference table](images/11-inference-table.png)
*`images/11-inference-table.png` — `cr_ranker_inference_payload`, the AI Gateway
inference table, which is also the retraining corpus and the dashboard's source.*

**On latency, honestly.** This notebook used to time `spark.sql()` against the FOREIGN
online table and report ~1 s as "online keyed read latency". That number was
serverless SQL planning plus a federated read; it never touched the serving path. It
now reports three separately labelled numbers: the endpoint round trip, a psycopg
keyed read, and — for contrast only — the SQL-console read.

## Step 6 · Features the store cannot hold

Some features cannot be precomputed. `notebooks/10_horizontal/06_ondemand_features.py` creates four
UC **Python** UDFs from the DDL generated by `src/crfs/udfs.py` (`udfs.ddl(fq)` emits
them, `udfs.feature_functions(fq)` returns the matching `FeatureFunction` list), and they
are evaluated inside the endpoint after the online lookups:

| UDF (three-level name in your schema) | Output feature | Why it must be on demand |
|---|---|---|
| `cr_genre_affinity_match` | `affinity_match` | viewer × title cross — precomputing means one row per viewer per title, 39,600 rows here and billions at real scale |
| `cr_affinity_popularity_cross` | `affinity_x_popularity` | the same cross, scaled by `popularity_30d` |
| `cr_hour_affinity_delta` | `hour_affinity` | `hour_of_day` only exists in the request; the circular distance is weighted by `hour_concentration` |
| `cr_session_decay` | `session_decay` | `exp(-minutes/30)`, clamped at 1440 minutes, from the wall clock at request time |

They reach the model through `fe.create_training_set(feature_lookups=lookups + on_demand)`
— note that the `FeatureFunction`s go into the **same** `feature_lookups` list. There is
no `feature_functions=` keyword argument, which is an easy hour to lose.

Three encoding rules are baked into `src/crfs/udfs.py`, each one learned the hard way and
commented there:

- **`BIGINT`, not `INT`**, for every genre-flag argument. pandas `int64` lands in Delta as
  `bigint`, and a mismatch fails at training-set creation with
  `FeatureFunction argument column 'genre_action' … has type 'bigint'`.
- **Apostrophes in `COMMENT` are doubled**, or the DDL dies with `PARSE_SYNTAX_ERROR`.
- **Bodies contain no indented blocks** — conditional expressions only. Leading whitespace
  does not survive the round trip into the stored function body, which turns an indented
  `return` into an `IndentationError` at query time, surfacing as `UDF_USER_CODE_ERROR`.

Verified behaviour (2026-09-07), including the cases that matter:

```
match_scifi          0.3       affinity for the title's genre
match_action         0.4
cross_pop            0.42      = 0.3 × (0.5 + 0.9)
hour_near            0.8625    request 21:00 vs habit 21:30
hour_far             0.0375    request 09:00 vs habit 21:30
hour_wrap_2h         0.75      23:00 vs 01:00 → two hours apart, not twenty-two
decay_10min          0.7165    exp(-10/30)
decay_clamped        0.0       older than 24h
decay_null           0.0       missing key → no exception
match_all_null       0.0
```

Result of adding them, reported as measured: holdout AUC **0.6696** for v2 against
**0.6643** for v1, so **+0.0053** on 61,222 training and 7,302 holdout rows, with 38
numeric plus 4 categorical features. A small lift on synthetic data. The claim this
demo makes is about the mechanism — features that cannot be precomputed still travel
with the model and are evaluated inside the endpoint — not about the number.

Two things to say out loud: the label frame gains
`request_epoch_s = unix_timestamp(event_ts)`, which is the point-in-time-correct
definition of the request clock; and `request_epoch_s` stays **out** of the model's
feature list — only `session_decay` enters, or the model learns absolute time and rots.

Every UDF guards `None` explicitly, because FeatureFunction inputs arrive as NaN online
and `None` in batch when a lookup key is missing, and an unguarded UDF raises *inside
model serving* — the caller sees a 500 with no useful detail.

> **Screenshot missing:** `images/05-ondemand-udfs.png` — the four `cr_*` functions in
> Catalog Explorer under **Functions**, or the output of
> `DESCRIBE FUNCTION EXTENDED <catalog>.<schema>.cr_hour_affinity_delta`. See
> [Screenshots](#screenshots).

## Step 7 · Features without a model

`notebooks/10_horizontal/07_feature_serving.py` creates a feature spec
(`fe.create_feature_spec(name=..., features=lookups + on_demand)`) and a **Feature
Serving endpoint** named `crunchyroll-viewer-features`: keys in, feature values out, no
model in the path. For when the scoring model lives outside Databricks, or the
application needs the values for its own logic — the app's raw-Lakebase panel falls back
to it, and the agent's `get_viewer_context` tool uses it as its only feature source.

The response carries the stored Lakebase values *and* the request-time UDF outputs,
and the notebook shows them matching a direct Postgres keyed read.

Two API shapes that are easy to get wrong: `served_entities` takes a **single
`ServedEntity`**, not a list; and the response arrives under **`outputs`**, not
`predictions` — reading `resp.predictions` gets you `None`.

Verified by querying it three times with identical stored features and different
request context:

```
hour_of_day=21, now        hour_affinity 0.5244   session_decay 0.1629    350 ms
hour_of_day=9,  now        hour_affinity 0.2335   session_decay 0.1629    343 ms
hour_of_day=21, 6h ago     hour_affinity 0.5244   session_decay 1.0000    343 ms
affinity_match holds at 0.0601 throughout — same viewer, same title.
```

That is the proof the UDFs run inside the endpoint rather than being baked in. It also
returns `minutes_watched_24h: 142.0` and `last_primary_genre: 'sci_fi'` — the exact row
the freshness demo wrote, reached through a different access path.

And the negative test: `viewer_id='v9999_does_not_exist'` comes back with
`minutes_watched_24h: None`, `affinity_match: 0.0`, `session_decay: 0.0`. Graceful,
because every UDF guards `None`. An unguarded one raises inside model serving and the
caller sees a 500.

> **Screenshot missing:** `images/07-feature-serving-endpoint.png` — the
> `crunchyroll-viewer-features` endpoint page under **Serving**, showing the served
> entity is a *feature spec* rather than a model. See [Screenshots](#screenshots).

## Step 8 · Two models, one feature layer

`notebooks/10_horizontal/08_retrieval_ranker.py` adds retrieval: `TruncatedSVD(n_components=8)` on the
300 × 132 implicit play matrix, restricted to the same training window as the ranker so
there is no leakage.

The payoff is where the viewer factors go — into `viewer_embedding_current`
(`viewer_id`, `vf_0` … `vf_7`), a governed feature table published to Lakebase as
`online_viewer_embedding` by the same `ops.publish_or_refresh()` path as everything
else. The retrieval model's own representation is a feature, versioned and served like
any other. Item factors are baked into the model artifact; at Crunchyroll's catalogue
size that side moves to Vector Search and the request contract does not change.

One shape to get right: the frame passed to `fe.log_model` must carry **only the lookup
key**. Include `vf_0` … `vf_7` in it as well and registration fails with
`Columns … are already specified in FeatureLookups: 'vf_0', …` — the model is supposed to
fetch those from Lakebase by `viewer_id` at request time, not receive them.

Recall@60 is reported against a popularity-only baseline and against random. If SVD
does not beat popularity on this synthetic data, the notebook says so and popularity
stays the baseline arm.

The funnel is orchestrated by the application (`app/app.py`), not by a wrapper model —
retriever, then the entitlement filter as one Postgres query, then the ranker on the
survivors, with a latency chip per hop. A wrapper model would need an outbound HTTPS call
from inside model serving to reach the other endpoint, or a duplicated artifact; neither
is worth it when the caller can make two calls.

> **Screenshot missing:** `images/15-retriever-recall.png` — notebook 08's metrics cell,
> recall@60 for SVD against the popularity and random baselines. See
> [Screenshots](#screenshots).

## Step 5 & 10 · Freshness, measured

Two notebooks, two publish modes, one contract.

`notebooks/10_horizontal/05_freshness_triggered.py` — **TRIGGERED**: reset a viewer to a calm
baseline, rank 25 candidates, complete three sci-fi episodes *now* (written to
`engagement_events_stream`, never to the training corpus), recompute
`recent_behavior_current` through the shared `features.build_recent_behavior()`, call
`ops.refresh_and_wait()`, rank again. A refresh per change, which is right for a feature
that moves a few times a day.

Measured on the run that produced this README:

```
online row BEFORE    38.5 min · slice_of_life · 2 titles
online row AFTER    142.0 min · sci_fi        · 3 titles
17 of 25 candidates re-scored · max |delta| 0.0271
TRIGGERED sync 39.3 s wall clock · keyed read 3 ms · queries 147 ms then 138 ms
```

The top pick held (*Mushoku Tensei* before and after) — the ordering underneath moved,
number one did not. Say that rather than implying a reshuffle; the claim is that an event
landing now changes the next scoring pass, not that it always changes the winner.

The notebook **fails** if the online row did not change. An earlier version passed green
with a delta of exactly 0.0, because the sync wait could be satisfied by the *previous*
completed sync and the endpoint then re-ranked against stale features. A demo that
silently compares identical inputs is worse than one that breaks.

![Online row before/after](images/14-freshness-features.png)
*`images/14-freshness-features.png` — the online row for `v0001` before and after the
three episodes.*

![Before/after movers](images/12-freshness-before-after.png)
*`images/12-freshness-before-after.png` — the candidates whose score moved, with the
deltas.*

`notebooks/40_streaming/10_streaming_continuous.py` — **CONTINUOUS**: a streaming aggregate maintains
`session_features_current` and the sync pipeline keeps `online_session_features` current
with no refresh call at all. `publish_table(..., publish_mode="CONTINUOUS")` is called
**once**; after that the pipeline owns the freshness.

`features.session_aggregate()` does the aggregation — a 10-minute watermark, per-viewer
`session_seconds`, `session_skips`, `session_events`, `last_event_epoch_s` and the
forwarded `src_event_epoch_ms`. It writes through
`foreachBatch(lambda df, _: fe.write_table(..., mode="merge"))` with a checkpoint on
`/Volumes/<catalog>/<schema>/crfs_ops/checkpoints/`.

Three serverless constraints shaped that design, all of them worth knowing before you
write your own:

- `trigger(processingTime=…)` with no bound is rejected as
  `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`; the notebook uses
  `trigger(availableNow=True)` to drain, cycle by cycle.
- `outputMode("append")` on a non-windowed aggregation fails with
  `STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION` — it has to be `update`.
- Inside `foreachBatch` you **cannot** construct a credentialed client
  (`cannot configure default credentials`), so the MERGE is done with the `DeltaTable`
  builder against the already-resolved table rather than through a fresh SDK call.

Events arrive via `notebooks/40_streaming/11_event_producer.py`, which has three modes selected by a
widget: `burst` (n events and exit — what the app's button and `make burst` fire), `loop`
(background traffic while you talk, the default for `crfs_streaming`) and `zerobus`
(the same payload over gRPC direct-to-Delta, which needs
`databricks-zerobus-ingest-sdk` declared in the task's `environments:` because it cannot
be pip-installed at runtime on serverless).

**How the freshness number is produced.** Every event carries `produced_epoch_ms`, the
producer's own clock, and that value is aggregated forward into the online row as
`src_event_epoch_ms`. `OnlineStore.wait_for_value()` in `src/crfs/online.py` polls the
keyed row every 250 ms until it sees that value, then subtracts. Both ends of the
subtraction are the same clock, so there is no skew argument — and the poll interval is
reported alongside, because it quantises the answer. The notebook also prints the
platform's own decomposition from the synced-table API (`delta_commit_timestamp` versus
`sync_end_timestamp`), so the compute-and-commit leg and the commit-to-Postgres leg can
be read separately.

**Do not quote the end-to-end number as a platform figure.** On the verified run it was
about 81 seconds, and the decomposition shows why: `availableNow` cycle startup dominates,
and the Lakebase leg itself is roughly 3.8 seconds. That is a property of this
configuration, not of the sync. The number to quote from this beat is the shape — no
refresh call, no orchestration — and the keyed-read latency once the value is there.

> **Screenshots missing:** `images/16-streaming-continuous.png` (the
> `online_session_features` synced table showing `CONTINUOUS` and a moving
> `sync_end_timestamp`) and `images/17-zerobus-events.png` (`engagement_events_stream`
> filling as the producer runs). See [Screenshots](#screenshots).

Zerobus writes to **Delta only** — it cannot write to Lakebase Postgres, and the
published online tables are read-only there because the sync pipeline owns them. Why
this demo does not use Stream Feature Views, Real-Time Mode, or the native Postgres
sink is answered concretely in [docs/streaming_paths.md](docs/streaming_paths.md).

## Step 12 · An agent on the feature store

`notebooks/50_agent/12_agent_explain.py` logs an `mlflow.pyfunc.ResponsesAgent` (falling back to
`ChatAgent` if the installed MLflow lacks it — the notebook prints which) backed by
`databricks-claude-sonnet-4-5`, with three tools:

| Tool | Calls | Why |
|---|---|---|
| `get_viewer_context(viewer_id, title_id, hour_of_day)` | the **Feature Serving endpoint** `crunchyroll-viewer-features` | The LLM reads the same governed Lakebase values the ranker read, including the request-time UDF outputs. This is the headline of the beat |
| `score_candidates(viewer_id, title_ids, surface, device, hour_of_day)` | the ranker endpoint | Counterfactuals ("would it still rank first on mobile at 8am?") are answered by re-querying, not by guessing |
| `describe_title(title_id)` | `titles` via the SQL warehouse | Readable prose instead of ids |

The system prompt requires it to answer only from tool output, quote the feature values
it used, and say when one is missing. Tool bodies are defined **inline** in the notebook
and templated into a `tools.py` artifact at log time — nothing the served agent needs at
load time may live in `src/crfs/`, which is driver-side only.

Resources are declared at log time (`DatabricksServingEndpoint` ×2, `DatabricksTable`,
`DatabricksSQLWarehouse`) so the endpoint's principal gets automatic auth passthrough;
`auth_mode=secret` with a scoped PAT is the one-widget fallback if a live run needs it.

> **Screenshot missing:** `images/18-agent-trace.png` — the MLflow trace for one answer,
> showing the `get_viewer_context` tool call and the feature values it returned. This one
> needs the notebook to be run first; see
> [what this demo does not do](#what-this-demo-does-not-do).

## The app

A Streamlit app on Databricks Apps — `app/app.py` (the six regions),
`app/lib/lakebase.py` (the Postgres access layer, the same `OnlineStore` class as
`src/crfs/online.py` but vendored so the app has no dependency on the repo's driver
code), `app/app.yaml` (env vars) and `app/requirements.txt`.

| Region in `app/app.py` | What it shows |
|---|---|
| Sidebar — Configuration | Viewer, surface, device, locale, hour, model version, and a **frozen-vs-real clock** toggle. It defaults to frozen, because `session_decay` would otherwise re-score a demo left idle mid-sentence |
| Funnel | `132 → 60 → N entitled → 25`, with a latency chip per hop |
| Ranked Watch Next | Ranked cards, each with a "Why?" expander that calls the explainer agent |
| Online Store (Raw) | The actual rows from the online tables, the SQL that produced them, and a keyed-read latency chip. On-demand values are labelled *computed at request time, not stored* |
| Freshness | A **"watch 3 episodes now"** button that fires the `crfs_event_burst` job, then polls Postgres every 250 ms and reports the measured seconds until the online value moved, then re-ranks and diffs the ordering |
| Operating it | Store capacity, Lakebase endpoint state, per-table sync lag and the last three days of spend, all read live |

If the app's service principal has not been granted read access to the Postgres schema
it degrades to reading the same values through the Feature Serving endpoint and shows a
visible badge. A grant problem never takes the demo down.

Two grant scripts, and both matter:

- `scripts/grant_app_postgres.sh <profile>` — `GRANT USAGE ON SCHEMA`,
  `GRANT SELECT ON ALL TABLES`, then `ALTER DEFAULT PRIVILEGES`. The schema and its
  synced tables are owned by whoever created them, so a fresh app service principal
  connects successfully and still gets `permission denied for schema crunchyroll_demo`.
  The `ALTER DEFAULT PRIVILEGES` line is the part that covers tables published *later* —
  re-run this after any new `publish_table`, which is why `make deploy-app` runs it as a
  post-step.
- `scripts/grant_app_endpoints.sh <profile>` — `CAN_QUERY` on the serving endpoints and
  `CAN_USE` on the warehouse.

The app is deployed by `scripts/deploy_app.sh`, **not** by the bundle: CLI v1.14.1 always
sends `forward_user_access_token` in the update mask and the Apps API rejects it, so
`bundle deploy` cannot update an app that already exists. The reasoning and the filed
issue are in [docs/risks.md](docs/risks.md), and
[docs/app.resource.yml.reference](docs/app.resource.yml.reference) keeps the resource
declaration that *would* go in the bundle once the CLI allows it.

> **Screenshots missing:** `images/19-app-funnel.png`, `images/20-app-lakebase-panel.png`
> and `images/21-app-freshness.png`. The app deploys and its logs are clean, but nobody
> has opened the page. See [Screenshots](#screenshots).

## Step 13 · Operating it

`notebooks/90_ops/13_ops_and_cost.py` prints one operator table per online table —
`detailed_state`, source commit versus processed commit, lag seconds, pipeline state,
online versus offline row counts — then store capacity, endpoint CU bounds, and 14 days
of DBU and dollars per SKU. Everything it prints comes from `src/crfs/ops.py`:
`sync_summary()`, `sync_lag_seconds()`, `pipeline_health()`, `online_store_status()`,
`lakebase_endpoint()` and `daily_cost()`.

It also persists its own sync numbers to the `crfs_ops_sync_log` table, because
dashboards run SQL and cannot call REST — that table is the only honest way to get sync
state onto a dashboard widget.

The AI/BI dashboard `dashboards/crfs_feature_ops.lvdash.json` has six datasets, every
query tested against the workspace by `scripts/validate_dashboard_queries.sh` before it
was committed:

| Dataset | Source | Answers |
|---|---|---|
| `serving_latency` | `cr_ranker_inference_payload` | p50/p95 and volume by `served_entity_id` — v1 against v2 |
| `request_mix` | `from_json(request)` on the same table | Proves the app sends only keys plus context, never feature values |
| `score_by_genre` | inference payload ⨝ `titles` | Score distribution and the titles that win |
| `sync_health` | `crfs_ops_sync_log` | Per-table sync state and lag. **Empty until notebook 13 has run**, which is correct, not a bug |
| `feature_freshness` | `crfs_ops_sync_log` | How stale the online values are allowed to be |
| `cost` | `system.billing.usage` ⨝ `list_prices` | Daily DBU and list USD per SKU |

If the dashboard looks empty, check the JSON shape first: an earlier version wrapped
everything in a `dashboard {}` object instead of putting `datasets` and `pages` at the top
level, and the API accepted it silently.

> **Screenshots missing:** `images/22-dashboard.png` (the rendered dashboard) and
> `images/23-ops-report.png` (notebook 13's operator table). See
> [Screenshots](#screenshots).

## Cost, and stopping it

**A Lakebase online store cannot scale to zero.** The docs are blunt about it —
*"Lakebase scale-to-zero is not supported"* and *"Online stores continuously incur costs.
Delete online stores that are no longer needed."* It is the one line that bills while
nobody is watching.

Measured, not estimated:

| | |
|---|---|
| SKU | `ENTERPRISE_DATABASE_SERVERLESS_COMPUTE_US_EAST_N_VIRGINIA` at **$0.52/DBU** |
| At `CU_2` | 30.67 DBU/day = **$15.95/day ≈ $485/month** |
| Endpoint at `CU_2` | min 8 / max 16 CU |
| After moving to `CU_1` | endpoint **min 4 / max 8 CU** — the capacity class governs the floor, `CU_N` → 4N CU |

That last row is the useful finding: changing the capacity class moved the endpoint
bounds by itself, with all three published tables staying
`SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE` throughout. So the class is the lever, not the
endpoint's autoscaling bounds — which is why the bundle does not manage
`postgres_endpoints` by default.

```bash
make cost               # daily DBU and list USD, from system.billing
make teardown-cost      # app, endpoints, online tables, online store. Data untouched.
```

Teardown deletes online tables with `w.feature_store.delete_online_table()` — the docs
call it *"the only recommended method"*, because `DROP TABLE` and the synced-table delete
both leave the table behind in Postgres. Getting that wrong produces the confusing state
where Unity Catalog shows nothing and the next `publish_table` still fails with
`AlreadyExists`.

Full details and the five levers in order: [docs/cost_and_sizing.md](docs/cost_and_sizing.md).


---

## Vertical ranking — the detail

online:  4,681 rows
one row per key confirmed; online is ~99x smaller
```

and the synced table's own spec came back as
`primary_key_columns=[viewer_id, rail_id]`, `timeseries_key=ts`. Notebook 21 asserts
this rather than printing it, because everything the serving path claims depends on
it.

So there is no `viewer_rail_current` mirror. Training and serving read the same table
through the same `FeatureLookup`. That is a stronger statement than "two tables built
from one definition" — the horizontal path still keeps `viewer_features_ts` +
`viewer_features_current`, and collapsing it the same way is the recommendation
written up in [docs/vertical_ranking.md](docs/vertical_ranking.md).

### Position bias, which a homepage ranker cannot skip

Every label in a homepage log was observed at a position the *incumbent* policy
chose. Measured on this data, `P(viewport | position)` falls from **0.97 at position 1
to 0.09 at position 16**. Fit that raw and the model learns the old homepage.

| Mechanism | Where |
|---|---|
| `rail_position_propensity` — measured `P(viewport \| position)` and clipped IPS weights | notebook 20 |
| Clicked rows weighted by `1 / P(viewport \| position)`, clipped at 10× | notebook 22 |
| **Rendered position is never a feature** — at request time it is the output, not an input | notebook 22 |
| AUC reported on all impressions *and* on viewed impressions only | notebook 22 |
| NDCG@3/@5 and MRR per session vs the incumbent editorial order, rail CTR, and random | notebook 22 |
| An ablation that drops the whole rail-identity block, isolating personalization from "a better fixed order" | notebook 22 |

Measured on 537 holdout homepage sessions: **NDCG@5 0.7157 for the ranker against
0.6791 for the incumbent editorial order — +5.39%**; MRR 0.7147 against 0.6775; holdout
AUC 0.6345 on viewed impressions.

The ablation that drops **all 13 rail-identity features** loses nothing — NDCG@5 0.7161,
slightly *up*, Spearman 0.9735 confirming the models differ. **So the whole lift is
personalization**, not a better fixed order. Rail-level aggregates score on permutation
importance (an AUC metric) yet cannot reorder rails for one viewer, because within a
session every viewer sees the same rail-level priors. Note the importance *ordering*
between the two new tables is not stable run to run and should not be quoted; the stable
findings are that the two new tables dominate and the shared viewer tables sit at ~zero
for rail ranking. Reconciled, with multiple runs' numbers, in
[docs/vertical_ranking.md](docs/vertical_ranking.md), along with why the shared viewer
tables contribute ~nothing to *rail* ranking and what that does and does not say about
sharing a feature store.

Those labels come from a latent utility the model can recover, so read the lift as
evidence the pipeline works rather than a forecast of Crunchyroll's lift.


---

## Screenshots — what exists and what is missing

## Screenshots

Twelve screenshots are in `images/`, all captured from the live workspace this demo was
built on. Twelve more are named below but do not exist yet. They are listed with the
exact filename to use and the exact place to capture it, so anyone can fill them in and
the README's `![...]` links will start resolving without any other edit.

### What exists

| File | Shows | Beat |
|---|---|---|
| `images/01-catalog-explorer.png` | The `crunchyroll_demo` schema in Catalog Explorer | Step 0 |
| `images/02-events-table.png` | `engagement_events` sample rows | Step 0 |
| `images/03-feature-table-viewer.png` | `viewer_features_current` with its PK and feature-table badge | Step 1 |
| `images/04-online-tables.png` | The three published online tables and their sync state | Step 1 |
| `images/13-lakebase-project.png` | The `crunchyroll-online-store` Lakebase project | Step 1 |
| `images/06-pit-proof.png` | The same feature as-of-impression versus as-of-now | Step 2 |
| `images/08b-model-version-spec.png` | The registered model version with its embedded feature spec | Step 2 |
| `images/09-endpoint-ready.png` | The ranker endpoint READY, inference tables on | Step 3 |
| `images/10-query-ranked.png` | 25 candidates in, ranked titles out | Step 4 |
| `images/11-inference-table.png` | `cr_ranker_inference_payload` | Step 4 |
| `images/14-freshness-features.png` | The online row for `v0001` before and after | Step 5 |
| `images/12-freshness-before-after.png` | The candidates whose score moved | Step 5 |

**All twelve were captured on 2026-09-01, before the 2026-09-07/08 corrections.** They
show the right screens and the right shape, but any number visible in them predates the
latency fix and the data-clock fix, so read the numbers from this README's text — those
come from the corrected runs and each has a row in
[docs/verification_log.md](docs/verification_log.md). Re-capturing them is the cheapest
outstanding improvement to this repo.

### What is still missing

Nothing below has been captured. The README marks each gap inline as well, so a reader
never mistakes an absent image for an unwritten step.

| File to create | Capture it from | Prerequisite |
|---|---|---|
| `images/05-ondemand-udfs.png` | Catalog Explorer → the schema → **Functions**, showing the four `cr_*` UDFs. Or the output of `DESCRIBE FUNCTION EXTENDED <catalog>.<schema>.cr_hour_affinity_delta` | `ondemand_features` task has run |
| `images/07-feature-serving-endpoint.png` | **Serving** → `crunchyroll-viewer-features`, showing the served entity is a feature spec, not a model | `feature_serving` task has run |
| `images/15-retriever-recall.png` | Notebook 08's metrics cell: recall@60 for SVD against popularity and random | `train_retriever` task has run |
| `images/16-streaming-continuous.png` | Catalog Explorer → `online_session_features`, showing `CONTINUOUS` and a moving `sync_end_timestamp` | `make streaming` is running |
| `images/17-zerobus-events.png` | `SELECT count(*) FROM engagement_events_stream` climbing while the producer runs | `make streaming` is running |
| `images/18-agent-trace.png` | The MLflow trace for one answer, showing the `get_viewer_context` tool call and the values it returned | **`make agent` has never been run** |
| `images/19-app-funnel.png` | The app's funnel strip with its per-hop latency chips | App page has never been opened |
| `images/20-app-lakebase-panel.png` | The app's raw Lakebase panel: real rows, the SQL, the latency chip | App page has never been opened |
| `images/21-app-freshness.png` | The app after "watch 3 episodes now": the measured seconds and the re-ranked list | App page has never been opened |
| `images/22-dashboard.png` | The rendered `crfs_feature_ops` dashboard | Run notebook 13 first, or `sync_health` is legitimately empty |
| `images/23-ops-report.png` | Notebook 13's operator table | `ops_report` task has run |
| `images/24-cost.png` | `make cost` output, or the `cost` dashboard widget | 24 h at `CU_1` for a clean steady-state figure |

The prerequisite column is the honest reason each one is missing: seven need nothing but
someone taking the screenshot after a run that has already succeeded, three need the app
page opened for the first time, one needs the agent notebook to run at all, and one needs
a full day of billing at `CU_1`.

Convention if you add more: `images/NN-short-name.png`, where `NN` matches the step
number in this README, and add a one-line italic caption under the image saying which
file it is.

