# Live walkthrough script — Crunchyroll Feature Store on Lakebase

Companion to the README. Times are targets; cut step 7 if the room runs long.

| # | Beat | Time | Workspace object on screen |
|---|------|------|----------------------------|
| 1 | The viewer decision | 2 min | Deck slides 3–4 |
| 2 | Architecture on one slide | 2 min | README diagram / deck 07a–12 |
| 3 | Raw signals in Unity Catalog | 3 min | Catalog Explorer → `crunchyroll_demo` |
| 4 | One definition, two stores | 5 min | Feature tables + online tables |
| 5 | Point-in-time training | 4 min | Notebook 02 PIT proof + AUC |
| 6 | Automatic feature lookup | 5 min | Serving endpoint → query → ranked titles |
| 7 | Freshness loop live | 5 min | Notebook 05 before/after |
| 8 | Learning loop | 3 min | Inference table rows |
| 9 | Phased pilot | 2 min | Deck slides 18–19 |

## Say-cues per beat

### 1 · The viewer decision
> "Every Crunchyroll surface asks one question: given this viewer, this session and this catalog, what plays next? The hard part is not the model — it is that genre affinity builds over months while a skip matters in seconds."

### 2 · Architecture
> "One definition drives the batch path, the online path and serving. The lakehouse holds history; Lakebase holds only the serving-critical current values."

### 3 · Raw signals
> "Everything starts governed in Unity Catalog — events, catalog, entitlements. Nothing here is feature store specific yet."

### 4 · One definition, two stores
> "Same feature definitions produce point-in-time training data offline and latest keyed values online. The online store is Lakebase — managed Postgres, built for frequent small upserts and low-latency keyed reads."

### 5 · Point-in-time training
> "Watch the two affinity columns differ — the model trains on what we knew *at impression time*, not what we know today. That is leakage, gone by construction."

### 6 · Automatic lookup
> "The request carries only what the app knows: viewer, candidates, context. The endpoint fetches the rest from Lakebase because the feature spec travels inside the registered model. The app team never writes a feature join."

### 7 · Freshness loop
> "Viewer binges three sci-fi episodes. We refresh one feature row, re-publish, and the same candidate list re-ranks. Production runs this path streaming at 200 ms p99; here we run the identical contract in about two minutes."

### 8 · Learning loop
> "Every request and response lands in an inference table. Join it with plays and skips and you have tomorrow's retraining set — and your drift monitor."

### 9 · Pilot
> "Phase 1 is govern and reproduce. Phase 2 adds online reranking. Phase 3 goes session-aware. No phase replaces the one before it."

## Fallbacks (if something breaks live)

- **Endpoint cold start (scale-to-zero on):** first query takes ~30–60 s; narrate over it or pre-warm with one query during step 5.
- **Online store read shows stale value in step 7:** TRIGGERED sync takes up to a minute; re-run the read cell.
- **Notebook run fails mid-demo:** every notebook ends with a saved output cell — open the last successful job run under Workflows and show its output instead.
- **psql not reachable for the online store instance:** skip to the SQL editor read of the online table; same data, different door.
