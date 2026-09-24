# Feature Views, and when to use them instead of feature tables

Feature Views are the declarative authoring path announced in
[*Introducing Feature Views*](https://www.databricks.com/blog/introducing-feature-views)
(10 July 2026, **Public Preview**). Instead of computing feature values and writing a
table, you declare what a feature *is* — source, entity, timestamp, aggregation,
window — and the platform owns the computation, the backfill, the refresh and the
online copy.

This repo now has both paths against the same data:

* the GA path — `src/crfs/features.py` + `notebooks/00_shared/01_feature_engineering.py`,
* the declarative path — `src/crfs/feature_views.py` + `notebooks/30_advanced/30_feature_views.py`
  (`make feature-views`).

Nothing in the GA demo depends on the second one. That is deliberate: it is a preview
API, and the existing pipeline, endpoints and app are what a customer readout rests on.

## Requirements, measured rather than assumed

| | |
|---|---|
| package | `databricks-feature-engineering>=0.16.0` |
| compute | serverless, or DBR 17.0 ML+ |
| status | Public Preview — enabled per workspace on the Previews page |
| verified here | `notebooks/30_advanced/29_preview_probe.py` (`make probe`) computed a real feature on the reference workspace (AWS us-east-1) |

Run `make probe` on any new workspace before anything else in this track. It registers
nothing and writes nothing.

## What the DSL can express

**The published limitations understate this.** The docs say "limited list of functions
(UDAFs) supported" and show `Sum`, `Avg`, `Count`. The installed package exports
**18 aggregation operators** plus three escape hatches. Read off the package by the
probe, not from the docs:

```
Sum  Avg  Count  Min  Max  First  Last  FirstN  LastN  FirstDistinct  LastDistinct
ApproxCountDistinct  ApproxPercentile  PercentileApprox  StddevSamp  StddevPop
VarSamp  VarPop
```

```python
Feature(*, source: DataSource,
        function: Union[AggregationFunction, ColumnSelection, RowTransformation],
        entity: Optional[List[str]] = None,
        timeseries_column: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None)

CustomUDF(function_name: str, input_bindings: Dict[str, str] = {})
FeatureViewSource(*, features: List[Feature])
RequestSource(*, schema: List[FieldDefinition])
```

The three that decide how far this path goes:

* **`CustomUDF(function_name=…)`** binds a **Unity Catalog function**. The seven
  `cr_*` UDFs this repo already has are therefore reusable as feature transformations,
  which means "the DSL only does sums" is wrong.
* **`RowTransformation`** transforms source rows before aggregation.
* **`FeatureViewSource(features=[…])`** chains a feature onto other features — which is
  how a ratio or a circular mean becomes expressible: aggregate the parts, then combine
  them.

Sources: `DeltaTableSource`, `StreamSource` (Kafka/Kinesis), `DataFrameSource`,
`VolumeSource`, `FeatureViewSource`, `RequestSource`.
Windows: `TumblingWindow`, `SlidingWindow`, `RollingWindow`, `SawtoothWindow`, each
taking `window_duration` plus optional `delay`, `offset` and `start_time`.

### Limits that bite in practice

From the docs, and each one is a decision rather than a footnote:

* **Batch rolling-window features cannot be materialized.** A `RollingWindow` batch
  feature trains fine and then cannot be served. `src/crfs/feature_views.py` uses
  `SlidingWindow` throughout for exactly this reason.
* **`ColumnSelection` features can only be materialized online**, not offline.
* **`RequestSource` features cannot be materialized at all** — they are computed per
  request, like the GA path's on-demand UDFs.
* **A label must not be a column of a feature source.** `played` lives in
  `engagement_events`, so notebook 30 renames its label to `engaged`.
* **Entity, timestamp and request column names must match** between the label frame and
  the definitions, and must be globally unique across sources. The GA path calls its
  label timestamp `ts`; the feature-view path has to call it `event_ts`.
* **Entity columns cannot be `DATE` or `TIMESTAMP`.**

## The two paths, side by side

| | GA feature tables | Feature Views |
|---|---|---|
| where the logic lives | `src/crfs/features.py`, ~400 lines of pandas | the `Feature` definition |
| who computes it | this repo's notebooks and jobs | the platform |
| backfill | a notebook you write and re-run | implied by the window |
| online copy | `fe.publish_table` + a sync to poll (`src/crfs/ops.py`) | `fe.materialize_features(online_config=…)` |
| training set | `FeatureLookup(table_name=…, timestamp_lookup_key=…)` | `features=[…]` — no table name anywhere |
| point-in-time | a separate `_ts` table plus a `timestamp_lookup_key` | inherent: the window is the definition |
| expressible | anything Python can compute | 18 operators, `CustomUDF`, `RowTransformation`, chaining |
| model contract | `fe.log_model(training_set=…)` | **identical** |
| serving | automatic feature lookup | **identical** |
| status | GA | Public Preview |
| in Crunchyroll's region (GCP us-west1) | yes | not yet — same gap as Lakebase |

The last two rows are why this is a second track. The first eight are why it is worth
having.

## What would and would not migrate

Taking the existing feature layer feature by feature:

| Feature | Declarative? | How |
|---|---|---|
| `minutes_watched_7d`, `plays_7d`, `skips_7d`, `minutes_watched_24h`, `skips_24h`, `plays_30d` | yes, directly | `Sum`/`Count` over a `SlidingWindow` |
| `active_titles_24h` | yes | `ApproxCountDistinct(title_id)` |
| `avg_watch_minutes_30d` | yes | `Avg` |
| `completion_rate_30d` | yes, chained | two aggregates + `FeatureViewSource` / `RowTransformation` for the ratio |
| `genre_affinity_*` (8 columns) | yes, awkwardly | per-genre `RowTransformation` then `Sum`, or a `CustomUDF` over the affinity vector |
| `typical_watch_hour`, `hour_concentration` | yes, chained | `Sum(sin)`, `Sum(cos)` then `atan2` in a `CustomUDF` — the circular mean is not an operator |
| `title_features` (popularity, recency, genre flags) | partly | catalog metadata is `ColumnSelection`, which is **online-only** when materialized |
| `rail_features`, `viewer_rail_features_ts` | not usefully | the generator, the eligibility rules and the propensity model are procedural; expressing them declaratively would obscure them |
| the 7 `cr_*` request-time UDFs | yes | `CustomUDF` binds them by name, or `RequestSource` |

So a full migration is **possible** for the viewer-grain layer and **not worthwhile**
for the rail-grain layer. The honest recommendation: author *new* aggregate features
declaratively, leave procedural features where they are, and do not rewrite a working
feature table to prove a point.

## What materialization actually creates

Measured, because none of it is guessable from the arguments you pass:

```
fe.materialize_features(features=[12 features],
                        offline_config=OfflineStoreConfig(table_name_prefix="fv_viewer"),
                        online_config=OnlineStoreConfig(table_name_prefix="fv_viewer_online",
                                                        online_store_name=<existing store>),
                        trigger=TableTrigger())
```

produced, on this workspace:

| Object | Rows | What it is |
|---|---|---|
| `fv_viewer_c7klmw` | 30,798 | offline feature table for one (entity, window) group |
| `fv_viewer_c7klmw_latest_view` | 1 | latest-per-key view over it |
| `fv_viewer_online_c7klmw` | 1 | the online copy, a FOREIGN table in the Lakebase store |
| `fv_viewer_z4kad0` | 19,655 | offline table for the next group |
| `fv_viewer_z4kad0_latest_view` | 297 | its latest-per-key view |
| `fv_viewer_online_z4kad0` | 297 | its online copy |
| `*_partial_aggregates` | — | internal intermediates, one per group |

Four facts worth carrying:

* **`table_name_prefix` is a prefix.** The platform appends a generated suffix, so you
  cannot predict the table name and should not hardcode it.
* **One group of tables per (entity, window) grouping**, not one per call. Twelve features
  across two grains and three window shapes did not produce one table.
* **`list_materialized_features` is per feature**, and its `feature_name` keyword is
  required — calling it without one raises a `TypeError` that reads like "nothing is
  materialized" if the exception is swallowed.
* **It is not idempotent.** A second call for the same feature raises
  `ResourceAlreadyExists`. `src/crfs/feature_views.py::materialize_new` skips what is
  already materialized so the notebook can be re-run.

The one-row online table above is not a materialization fault: the 24h-window features
are empty because this workspace's generated history ended five days earlier, which
`make verify` reports as a drifted demo clock.

## What notebook 30 actually does

1. declares seven viewer features over `engagement_events` (24h / 7d / 30d windows),
2. `fe.compute_features` — evaluates them without registering anything,
3. `fe.register_feature` — each becomes a governed UC object,
4. `fe.create_training_set(df=labels, features=[…], label="engaged")` — **no table
   name and no join**,
5. trains, then `fe.log_model(training_set=…)` so the dependencies travel with the model,
6. reads the feature spec back *out of the registered version* to show it is there,
7. `fe.score_batch` — the platform resolves every feature from the model itself,
8. `fe.materialize_features` into the **existing** Lakebase online store, with
   `TableTrigger()` so a commit to `engagement_events` refreshes it,
9. waits on the materialized tables rather than sleeping.

## Related

* [`../src/crfs/feature_views.py`](../src/crfs/feature_views.py) — the definitions
* [`feature_versioning.md`](feature_versioning.md) — how to change one of these safely
* [`streaming_paths.md`](streaming_paths.md) — Stream Feature Views and why Kafka is the
  blocker for the sub-second path
* [`verification_log.md`](verification_log.md) — what was run, and what it returned
