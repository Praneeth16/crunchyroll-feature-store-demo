# Live walkthrough — Crunchyroll Feature Store on Lakebase

Companion to the README. 12 beats, 45–60 minutes. Times are targets; the cut list
is at the bottom.

**Before you start:** `make verify`. It fails if the demo clock has drifted, if the
new feature columns are missing, or if an endpoint is not READY. Then fire one
throwaway query to warm the ranker — endpoints scale to zero, and the first request
after idle takes 30–60 seconds.

| # | Beat | Time | On screen |
|---|------|------|-----------|
| 1 | The viewer decision | 2 min | Deck slides 3–4 |
| 2 | Architecture on one slide | 2 min | README diagram |
| 3 | Raw signals in Unity Catalog | 3 min | Catalog Explorer → `crunchyroll_demo` |
| 4 | One definition, two stores | 5 min | Notebook 01, then the online tables |
| 5 | Lakebase is a real database | 3 min | `scripts/lakebase_explore.sh`, psql |
| 6 | Point-in-time training | 4 min | Notebook 02 PIT proof + AUC |
| 7 | Automatic feature lookup | 5 min | Notebook 04: keys in, ranked titles out |
| 8 | Features the store cannot hold | 5 min | Notebook 06: the four on-demand UDFs |
| 9 | Features without a model | 4 min | Notebook 07: the Feature Serving endpoint |
| 10 | Two models, one feature layer | 5 min | Notebook 08: retrieval → ranking funnel |
| 11 | Freshness, measured | 6 min | The app's burst button; notebooks 05 and 10 |
| 12 | Operating it: monitoring and cost | 5 min | Dashboard, notebook 13 |

## Say-cues

### 1 · The viewer decision
> "Every Crunchyroll surface asks one question: given this viewer, this session and
> this catalog, what plays next? The hard part is not the model. It is that genre
> affinity builds over months while a skip matters in seconds, and those two need
> completely different plumbing. Teams end up building the joins twice — once for
> training, once for serving — and every difference between them is silent damage to
> the model in production."

### 2 · Architecture on one slide
> "Define a feature once. Use it everywhere. The same governed definitions produce
> historically correct training data offline and low-latency keyed reads online, and
> the serving endpoint fetches its own features because the feature spec travels
> inside the registered model. The online store here is Lakebase — managed Postgres,
> built for exactly this: small keyed reads, frequent small upserts."

### 3 · Raw signals in Unity Catalog
> "Nothing here is feature-store specific yet. Viewing events, catalog metadata,
> entitlements — the governed raw signals any media company already has. This is
> where the feature store starts, not where it ends."

### 4 · One definition, two stores
Show notebook 01. Point at `upsert_feature_table` and the publish loop.
> "Four feature tables. The viewer table exists twice on purpose: daily snapshots for
> point-in-time training, and a current mirror for serving. Same definitions, two
> destinations. Primary keys, non-null, Change Data Feed — that is the contract an
> online store needs. And notice what the wait is: not a sleep, but a poll of the
> sync API until the pipeline confirms it consumed the commit we just wrote."

### 5 · Lakebase is a real database
```bash
./scripts/lakebase_explore.sh <profile> <catalog>
\dt crunchyroll_demo.*
SELECT viewer_id, minutes_watched_24h, last_primary_genre
  FROM crunchyroll_demo.online_recent_behavior WHERE viewer_id = 'v0001';
```
> "This is Postgres. Your application can talk to it with a Postgres driver, and the
> feature store keeps it in sync from Delta. One caveat worth knowing: connect to the
> endpoint's direct host, not the pooler — the pooler rejects OAuth tokens."

### 6 · Point-in-time training
> "Those two columns differ because the viewer kept watching after that impression.
> The model trains on what we knew *then*, not what we know now. Hand-built training
> joins get this wrong constantly, and it always flatters the offline metrics.
> Here it is structural — `timestamp_lookup_key` and a timeseries feature table."

### 7 · Automatic feature lookup
Show the request payload, then the ranked output.
> "The application sends what only it knows: who, what, which surface, what time.
> Everything else is a governed lookup the endpoint performs itself. The app never
> rebuilds a join and never learns where features live. That is the whole mechanism:
> the model carries its dependencies."

### 8 · Features the store cannot hold
> "Some features cannot be precomputed. Affinity-times-genre is a viewer-by-title
> cross — precomputing it means one row per viewer per title. Session decay depends
> on the wall clock at request time. So they are Unity Catalog Python functions,
> evaluated inside the endpoint after the lookups, and they travel with the model
> exactly like the stored features do. Watch what happens when I change nothing but
> the request hour."

### 9 · Features without a model
> "Sometimes you do not want a prediction, you want the features — the scoring model
> lives outside Databricks, or the application needs the values for its own logic.
> Same governed definitions, same Lakebase reads, no model in the path. And notice
> the response includes the request-time computed values too."

### 10 · Two models, one feature layer
> "A recommender is never one model. Retrieval narrows the catalog, ranking orders
> what survives. Both read the same online store — and the retrieval model's own
> viewer representation is itself a feature table published to Lakebase by the same
> path. At your catalog size the item side moves to Vector Search; the request
> contract does not change."

### 11 · Freshness, measured
Use the app's button. Let the timer run — do not talk over it.
> "Three episodes complete right now. The events land, the feature recomputes, the
> online row changes, and the next request scores differently. Nothing about the model
> changed and nothing about the application changed. The number on screen is measured,
> not quoted: the events carry the producer's own clock and we read it back out of
> Postgres, so the poll interval is the only fuzz."

Then contrast the two publish modes.
> "Notebook 05 does that on demand — a refresh per change, which is right for a
> feature that moves a few times a day. Notebook 10 runs the same contract
> continuously, where a streaming pipeline keeps Lakebase current with no refresh call
> at all. Freshness is a per-feature-class decision, not one global switch. Title
> popularity does not need a streaming pipeline. An in-session skip cannot wait."

### 12 · Operating it: monitoring and cost
> "Every request and response is captured in an inference table, so tomorrow's
> retraining set and today's drift monitoring come from the same place. And the honest
> part: an online store cannot scale to zero, because a store that sleeps cannot answer
> a keyed read in milliseconds. Here is what it actually bills, straight from the
> billing tables, and here are the five levers in the order I would reach for them."

Close on `make teardown-cost`.
> "One command stops the meter and leaves the data. That matters more than it sounds
> — the fastest way to lose trust in a platform is a surprise invoice for a demo."

## Fallbacks

| If | Then |
|---|---|
| First query is slow | Cold start, 30–60s. Pre-warm during beat 3. |
| A notebook fails live | Open the last successful run of `crfs_end_to_end` — every task's output is preserved. |
| The app's Lakebase panel shows a yellow badge | Grants. `./scripts/grant_app_postgres.sh <profile>`. The panel is still correct — it is reading through the Feature Serving endpoint. |
| The burst button does not move the value | A TRIGGERED table only moves when 05 or 10 recomputes. Run `make streaming` for the continuous path, or run notebook 05. |
| Endpoint missing | `03_deploy_ranker_endpoint.py` is idempotent; re-run it. |
| Ranking barely moves | Say so. The point is that it moves at all within seconds, not that it reshuffles. |

## Cut list, in order

Beat 10 first, then 9, then 5. Never cut 4, 7 or 11 — they are the argument.
