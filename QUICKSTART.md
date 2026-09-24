# Quickstart — empty workspace to working demo

Two paths. The one-command path is what you want the first time; the step-by-step path
is what you want when something fails, because each step is separately runnable and
separately checkable.

Nothing here is pinned to the workspace this was built on. The workspace comes from your
CLI profile. The three ids that differ per workspace (SQL warehouse, Lakebase database
resource, billing endpoint uid) are discovered at run time. Nothing in the repo needs
editing.

---

## 0 · Prerequisites

| | Why it matters |
|---|---|
| **Databricks CLI ≥ 1.17.0** | 1.14.1 cannot update an app in a bundle ([risks.md](docs/risks.md) §8b). `brew upgrade databricks` |
| **Node.js ≥ 18** | `make deploy` builds the app's React frontend locally and ships the build; the app container does not run npm ([homepage_service.md](docs/homepage_service.md#files)). `brew install node` |
| **A serverless workspace** | every task in this repo is a serverless notebook task; there is no cluster config anywhere |
| **Unity Catalog, with a writable catalog** | all features, models and functions are UC objects |
| **Online Feature Store (Lakebase) enabled** | needed for the online path. The batch path ([batch_and_online.md](docs/batch_and_online.md)) does not need it |
| **A SQL warehouse** | the dashboard and the agent's `describe_title` tool use it |
| **Permission to create** catalogs' schemas, models, functions, jobs, endpoints, apps | the demo creates all of these |
| *optional* **Feature Views preview** | only for `make feature-views` — Previews page, per workspace |
| *optional* **AI Runtime preview** | only for `make gpu-train`; AWS regions only |

Authenticate and pick a profile — never let a command pick one for you:

```bash
databricks auth login --host https://<your-workspace-url> --profile <PROFILE>
databricks auth profiles          # confirm it is there
databricks current-user me --profile <PROFILE>
```

Pass `PROFILE=<PROFILE>` the first time. `setup.sh` / `make bootstrap` remember it in
`.crfs.vars` (gitignored), so later `make` targets can omit it. Nothing falls back to
the workspace this was built on: without a profile, every target and script stops and
asks for one.

**Pick the catalog.** Pass `--catalog <name>` to `setup.sh` for a catalog you can create
schemas in. Omitted, bootstrap picks the first managed catalog you can write to and
prints it. Everything goes into one schema, `crunchyroll_demo` by default (`--schema`
changes it).

---

## 1 · One command

```bash
./setup.sh --profile <PROFILE>
```

Eight stages: discover or create infrastructure → deploy the bundle → the horizontal
pipeline → the vertical pipeline → grants → the app → the benchmark → verify. It asks
before creating anything billable. Roughly 60–75 minutes, most of it the two pipelines.

Useful flags: `--stage <name>` to run one stage, `--skip-bench`, `--skip-app`,
`--catalog`, `--schema`, `--yes`.

**What it creates in your workspace**

| | |
|---|---|
| Unity Catalog | one schema: events and reference tables, feature tables, UC Python UDFs, three registered models (watch-next ranker, rail ranker, retriever), a checkpoint volume |
| Lakebase | one Online Feature Store (`crunchyroll-online-store`, `CU_1`) and its published online tables |
| Model Serving | `crunchyroll-watch-next-ranker`, `crunchyroll-rail-ranker`, `crunchyroll-candidate-retriever`, `crunchyroll-viewer-features` (feature serving) |
| Jobs | `crfs_end_to_end`, `crfs_vertical`, `crfs_benchmark` and the optional ones (`make help`), all serverless |
| App + dashboard | the `crfs-watch-next` Databricks App (FastAPI + React) and one AI/BI dashboard |

**What keeps billing after it finishes:** the online store (it cannot scale to zero;
measured $15.95/day at `CU_2` list price, and the default `CU_1` halves the compute
floor), plus the three request-path endpoints, which are configured without
scale-to-zero. `make teardown-cost` removes all of these and keeps the data.
[cost_and_sizing.md](docs/cost_and_sizing.md) has the measured figures.

---

## 2 · Step by step

```bash
make preflight PROFILE=<PROFILE>   # CLI, auth, schema write access, warehouse, store, ids
make deploy    PROFILE=<PROFILE>   # jobs, volume, dashboard, app config  (one bundle deploy)
make demo      PROFILE=<PROFILE>   # horizontal: shared features + watch-next ranker (~35 min)
make vertical  PROFILE=<PROFILE>   # vertical: rails, rail features, rail ranker  (~15 min)
make deploy-app PROFILE=<PROFILE>  # push the app's source, then its four grants
make bench     PROFILE=<PROFILE>   # load-test the rail endpoint, in region
make verify    PROFILE=<PROFILE>   # assert the whole thing is presentable
```

`make preflight` prints the two generated ids (`lakebase_db_resource`, the billing
endpoint uid). They cannot be guessed, which is why `scripts/bootstrap.sh` discovers them
into `.crfs.vars` — gitignored, and passed to every bundle command as `--var`.

**`make deploy` is the whole deploy.** Jobs, the checkpoint volume, the dashboard *and*
the app are bundle resources.

**On a genuinely empty workspace, order matters.** The app declares the Lakebase database
as a resource, and that database's id is *generated* when `fe.create_online_store` runs in
notebook 01 — so it does not exist before the first pipeline run. `make bootstrap` writes
the real id into `.crfs.vars`, and `./setup.sh` sequences all of this for you. Taking a
shortcut straight to `make deploy` on a workspace that has never run the pipeline binds
a placeholder database id, and the app runs on Feature Serving without Postgres until
bootstrap resolves the real one. `make preflight` says so explicitly when it cannot
resolve the id.

One more exception: an app that already exists outside the bundle has to be adopted once,
otherwise deploy tries to create it and gets 409:

```bash
databricks bundle deployment bind crfs_watch_next crfs-watch-next -t dev --profile <PROFILE>
```

Two things the bundle deliberately does **not** own, with the reasons in
[`databricks.yml`](databricks.yml): the **online store** (`fe.create_online_store` must
own the Lakebase project so serving metadata resolves) and the **published online
tables** (a raw synced table is not registered as a feature-store online table, so
automatic lookup would not resolve it).

---

## 3 · The advanced track — optional, both Public Preview

```bash
make probe         PROFILE=<PROFILE>   # does this workspace have the two previews? free
make feature-views PROFILE=<PROFILE>   # declare features, train from them, materialize
make versioning    PROFILE=<PROFILE>   # what a deployed model pins; a canary
make gpu-train     PROFILE=<PROFILE>   # torch on a serverless A10. billable
```

Run `make probe` first. It registers nothing, writes nothing, and tells you which of the
two previews this workspace actually has — which is faster than finding out 30 minutes
into a job. Docs: [feature_views.md](docs/feature_views.md),
[feature_versioning.md](docs/feature_versioning.md),
[gpu_training.md](docs/gpu_training.md).

---

## 4 · What it costs, and stopping it

The **online store cannot scale to zero** — it is the one always-on cost. The rail
ranker endpoint is deliberately configured without scale-to-zero because it sits in a
request path. Everything else is serverless and idles at nothing.

```bash
make cost          PROFILE=<PROFILE>   # today's DBU and list USD, from system.billing
make teardown-cost PROFILE=<PROFILE>   # delete endpoints, app, online store. KEEPS all data
make teardown      PROFILE=<PROFILE>   # same scope, from the shell, no confirmation
```

The only path that drops UC tables, models and functions is
`./scripts/teardown.sh <PROFILE> --full`, which is deliberately not a make target. Full
numbers and the five cost levers: [cost_and_sizing.md](docs/cost_and_sizing.md).

---

## 5 · When something fails

| Symptom | Where to look |
|---|---|
| a job task failed | the run page names the task; `databricks bundle run <job> --only <task_key>` re-runs just it |
| `ModuleNotFoundError: src` | the bundle's file tree did not sync; re-run `make deploy` |
| app panels are empty | UC grants. `./scripts/grant_app_uc.sh <PROFILE>` — an empty panel is not evidence of empty data |
| app cannot read Lakebase | `./scripts/grant_app_postgres.sh <PROFILE>`, and again after any new `publish_table` |
| endpoint 429s under load | expected past capacity — it sheds rather than queues. [serving_benchmark.md](docs/serving_benchmark.md) |
| a preview API is missing | `make probe` |
| anything else | [risks.md](docs/risks.md) lists the platform sharp edges hit while building this, with symptoms and fixes |

`make verify` is the single question "is this presentable" — 31 assertions across
tables, freshness, endpoints and sync state. Run it before any demo.

---

## 6 · Where to read next

[docs/README.md](docs/README.md) is the index. Shortest useful path:
[ask_alignment.md](docs/ask_alignment.md) for what was asked versus what exists, then
[vertical_ranking.md](docs/vertical_ranking.md) for the architecture and the measured
numbers, then [walkthrough.md](docs/walkthrough.md) if you want the guided tour of every
step.
