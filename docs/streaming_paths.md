# Freshness paths: what this demo builds, and what it deliberately does not

The demo's freshness story is **Zerobus → Delta → `publish_table(CONTINUOUS)` → Lakebase**.
Everything below was checked against this workspace before that choice was made, so
the obvious follow-up questions have answers rather than opinions.

## What we build

| Stage | Mechanism | Notebook |
|---|---|---|
| Ingest | Zerobus gRPC, direct into a UC Delta table | `11_event_producer.py` (`mode=zerobus`) |
| Aggregate | Structured Streaming, `foreachBatch(fe.write_table(mode="merge"))`, 5s trigger | `10_streaming_continuous.py` |
| Sync | `publish_table(publish_mode="CONTINUOUS")` — a streaming pipeline, no refresh call per change | `10_streaming_continuous.py` |
| Contrast | `publish_mode="TRIGGERED"` — a refresh per change, on demand | `05_freshness_triggered.py` |

Both publish modes appear on purpose. Freshness is a per-feature-class decision:
title popularity does not need a streaming pipeline, and an in-session skip
cannot wait for a nightly job.

## Why not Zerobus straight into Lakebase

Zerobus writes to **Delta/UC tables only**. There is no Postgres target. The Delta
hop is not an inefficiency to be optimised away — it is where the governed,
point-in-time-correct copy lives, which is what lets the same definition serve
training and serving.

Separately, the published online tables are owned by their sync pipeline and are
read-only in Postgres. Writing to one directly breaks the sync.

## Why not Stream Feature Views (`StreamSource`)

This is the API that materialises features **online-only**, with no offline hop and
an advertised ~200 ms p99 — genuinely the closest thing to "write straight to the
online store". It requires Kafka.

`fe.create_stream()` accepts only `KafkaStreamConfig`: a topic subscription, a JSON
payload schema, and auth via a UC `KAFKA` connection or direct mTLS. There is no
Delta-source or Zerobus variant of `StreamSource`. Other Public Preview constraints:
`RollingWindow` aggregations only, a fixed operator set, no mixing with batch
features in one `materialize_features()` call, and an Enterprise-tier workspace with
a catalog on customer cloud storage.

Workspace state checked 2026-09-07: this catalog does have its own S3 storage root,
so it would pass that gate. Two `KAFKA` UC connections exist
(`eventhub_sfv_air_demo`, `realtime-rec-eventhub`) but both are `read_only: true` and
owned by other people, so neither is ours to produce into.

Standing this path up means provisioning a Kafka endpoint — Confluent Cloud, Azure
Event Hubs, Redpanda or MSK — and a UC `KAFKA` connection holding its credentials.
That is a decision about infrastructure, not about the feature store, which is why it
sits outside this demo.

## Why not Real-Time Mode

`trigger(realTime=...)` needs classic compute (serverless standalone RTM is
unsupported) and, more decisively, **Delta is not a supported RTM source**. Supported
sources are Kafka, Event Hubs, Kinesis, MSK and Rate; supported sinks are Kafka,
Event Hubs and `foreach`. A Delta-sourced RTM pipeline is not a thing that exists.

## Why not the native Lakebase streaming sink

`writeStream.format("postgresql")` writes straight into Lakebase Postgres with
upserts and a 100 ms default flush — but it requires **DBR 18.3+**, and the newest
runtime this workspace offers is 18.2. It also writes a hand-managed Postgres table
rather than a feature-store-published one, so automatic feature lookup could not
resolve it: the ranker would stop being able to fetch its own features. Worth
revisiting when the runtime is available, as a hot path *beside* the governed one.

## What serverless notebook compute will and will not do

Four constraints, each found by running this and each shaping the notebook. They are
worth knowing before designing a streaming demo on serverless.

| Constraint | Error | Consequence |
|---|---|---|
| No infinite triggers | `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED: Trigger type ProcessingTime is not supported for this cluster type. Use a different trigger type e.g. AvailableNow, Once.` | the aggregation runs as repeated `availableNow` drains; an always-on query needs classic compute or a Lakeflow pipeline |
| `append` is illegal for a non-windowed aggregation | `STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION: Invalid streaming output mode: append` | `outputMode("update")` — which is what a merge wants anyway. A watermark does not make `append` legal when there is no time window |
| No credentials inside `foreachBatch` | `ValueError: default auth: cannot configure default credentials`, raised in the foreachBatch Python process | no SDK-backed client there, so **`fe.write_table` is unavailable inside `foreachBatch`**. Upsert with Delta directly |
| No global temp views | `[NOT_SUPPORTED_WITH_SERVERLESS] GLOBAL TEMPORARY VIEW is not supported` | use the `DeltaTable` merge builder rather than a view plus SQL |

None of these touch the Lakebase leg. `publish_mode="CONTINUOUS"` is a streaming pipeline
the platform runs on its own compute, and it stays continuous regardless of how the
upstream aggregation is triggered.

## What the freshness number actually means

`10_streaming_continuous.py` reports three numbers, from three clocks, each labelled:

1. **Event → online-visible.** Every event carries `produced_epoch_ms`, the
   producer's own clock, and that value is aggregated forward into the online row.
   A psycopg reader polls the keyed row until it sees that value and subtracts.
   Both ends of the subtraction are the same clock, so there is no skew argument —
   and the poll interval is reported alongside, because it quantizes the answer.
2. **The platform's own decomposition**, from `GET /api/2.0/database/synced_tables/{name}`:
   `delta_commit_timestamp` versus `sync_end_timestamp`, i.e. compute-and-commit
   versus commit-to-Postgres.
3. **Pure keyed-read latency**, timed psycopg `SELECT ... WHERE viewer_id = %s`,
   reported separately from the notebook driver (in-region) and from a laptop.

For contrast the notebooks also print the same read via `spark.sql` on the FOREIGN
table, which lands around a second. That is SQL planning plus a federated read. The
first version of this demo reported that number as online-store latency; it is not,
and it is kept only to explain why the serving path does not go that way.
