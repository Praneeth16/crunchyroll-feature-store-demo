# Crunchyroll Feature Store on Lakebase.
#
#   make up             one command, empty workspace to working demo
#   make preflight      check the workspace is ready and discover generated ids
#   make deploy         bundle deploy + Postgres grants + start the app
#   make demo           the horizontal (title) pipeline, end to end
#   make vertical       the vertical (rail) pipeline, end to end
#   make probe          does this workspace have the two previews the advanced track needs
#   make feature-views  Feature Views: declare features, train from them, materialize
#   make versioning     feature-definition versioning against the live endpoint
#   make canary         judge a challenger behind the live endpoint; promote or roll back
#   make gpu-train      train the rail ranker on a serverless GPU (billable)
#   make bench          measure the rail endpoint under load, in region
#   make bench-local    the same benchmark from this laptop, for contrast
#   make cost           what the online store is billing right now
#   make teardown-cost  stop the money, keep the data
#
# PROFILE is your ~/.databrickscfg profile. Pass it once -- `make up PROFILE=my-ws` or
# `make bootstrap PROFILE=my-ws` -- and it is remembered in .crfs.vars (gitignored).
# TARGET is overridable too:  make deploy PROFILE=my-ws TARGET=prod

PROFILE ?= $(shell test -f .crfs.vars && grep '^CRFS_PROFILE=' .crfs.vars | cut -d= -f2-)
ifeq ($(strip $(PROFILE)),)
ifneq ($(filter-out help,$(or $(MAKECMDGOALS),help)),)
$(error No profile. Pass PROFILE=<name> -- `databricks auth profiles` lists yours)
endif
endif
TARGET  ?= dev
DB      ?= databricks
BUNDLE   = $(DB) bundle
# Generated per-workspace ids, if scripts/bootstrap.sh has run. Absent, the bundle
# stops on the variables that have no default (warehouse_id, notification_email).
VARS    := $(shell test -f .crfs.vars && grep -v '^\#' .crfs.vars | grep -v '^CRFS_' | grep '=' | sed 's/^/--var=/' | tr '\n' ' ')
FLAGS    = -t $(TARGET) --profile $(PROFILE) $(VARS)

.DEFAULT_GOAL := help
.PHONY: help up bootstrap render preflight validate frontend deploy deploy-app demo vertical batch batch-incremental \
        probe feature-views versioning canary gpu-train bench bench-local \
        bench-pull streaming burst agent app-logs app-url cost verify teardown-cost teardown \
        destroy fmt

help:
	@grep -E '^[a-z-]+:.*?##' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

up: ## One command: discover, deploy, run everything, benchmark, verify
	./setup.sh --profile $(PROFILE) --target $(TARGET)

bootstrap: ## Discover or create the infrastructure; write .crfs.vars
	@./scripts/bootstrap.sh --profile $(PROFILE)

preflight: ## Check prerequisites; print the ids the bundle needs
	@./scripts/preflight.sh $(PROFILE)

render: ## Render the dashboard template for this workspace
	@./scripts/render_dashboard.sh

# check_notebooks.py runs first because it is free and catches what a job charges for: a
# markdown cell missing its `%md` executes as Python, and that was found 28 minutes into a
# run whose earlier cells had all passed.
validate: render ## Lint the notebooks, then strict bundle validation
	python3 scripts/check_notebooks.py
	$(BUNDLE) validate --strict $(FLAGS)

frontend: ## Build the app's React frontend into app/frontend/dist
	cd app/frontend && npm ci --no-audit --no-fund && npm run typecheck && npm run build

deploy: validate frontend ## Deploy jobs, volume, dashboard and the app
	$(BUNDLE) deploy $(FLAGS)

# The app is a bundle resource now, so `deploy` already created or updated it and
# `bundle run` is what pushes its source and restarts it. scripts/deploy_app.sh is
# gone: it existed only because CLI v1.14.1 could not update an existing app, and it
# had to render app.yaml's ${NAME} placeholders itself because Apps does not expand
# them. The bundle resolves those, so both jobs disappeared with it.
deploy-app: deploy ## Deploy the app's source from the bundle and grant it access
	$(BUNDLE) run crfs_watch_next $(FLAGS)
	@echo
	@echo "Granting Postgres read access. Re-run after any new publish_table --"
	@echo "ALTER DEFAULT PRIVILEGES covers future tables, a plain GRANT does not."
	-./scripts/grant_app_postgres.sh $(PROFILE)
	@echo
	@echo "Granting CAN_QUERY on the notebook-created endpoints, including the rail ranker."
	-./scripts/grant_app_endpoints.sh $(PROFILE)
	@echo
	@echo "Granting Unity Catalog read access. Without this every Delta-backed panel in"
	@echo "the app comes back empty -- and it looks like a missing pipeline, not a grant."
	-./scripts/grant_app_uc.sh $(PROFILE)

# These depend on `deploy` deliberately. `bundle run` does NOT upload source -- it runs
# whatever the workspace already has. Editing src/crfs/ops.py and typing `make vertical`
# therefore ran the OLD file, and the job failed on a bug that had already been fixed on
# disk. Fifteen minutes to discover, twice. Making the run targets depend on deploy costs
# a few seconds and removes a whole class of confusing failure.
demo: deploy ## Horizontal ranking: the shared feature layer and the watch-next ranker (~35 min)
	$(BUNDLE) run crfs_end_to_end $(FLAGS)

vertical: deploy ## Vertical ranking: rails, rail features, rail ranker, request-path endpoint (~15 min)
	$(BUNDLE) run crfs_vertical $(FLAGS)

batch: deploy ## The offline path: score every viewer with score_batch, no online store
	$(BUNDLE) run crfs_batch $(FLAGS)

batch-incremental: deploy ## Rescore only viewers whose features changed (CDF-driven)
	$(BUNDLE) run crfs_batch $(FLAGS) --batch_mode incremental

# ---------------------------------------------------------------- advanced track
# Everything here stands on a Public Preview API. `probe` answers whether this
# workspace has them before anything else is attempted.
probe: deploy ## Check this workspace has the Feature Views and serverless GPU previews
	$(BUNDLE) run crfs_preview_probe $(FLAGS)

feature-views: deploy ## Declarative authoring: Feature Views for training, then materialized
	$(BUNDLE) run crfs_feature_views $(FLAGS)

versioning: deploy ## What a deployed model pins, what an in-place change does, and a canary
	$(BUNDLE) run crfs_versioning $(FLAGS)

canary: deploy ## Canary gate: challenger at 10%, judged, then promote or roll back (dry run)
	$(BUNDLE) run crfs_canary $(FLAGS)

gpu-train: deploy ## Train the rail ranker on a serverless GPU (A10). Billable.
	$(BUNDLE) run crfs_gpu_train $(FLAGS)

bench: deploy ## Measure the rail endpoint under load from inside the region
	$(BUNDLE) run crfs_benchmark $(FLAGS)

bench-local: ## The same benchmark from this laptop -- the contrast is the point
	python3 scripts/benchmark_local.py --profile $(PROFILE)

# The schema fallback is resolved by make, not by a shell `||`: in
# `$(grep ... | cut ... || echo default)` the `||` tests the PIPELINE's status, which is
# `cut`'s, and cut succeeds on empty input. So with no .crfs.vars (the fresh-clone
# state -- the file is gitignored) both expanded to empty and the path became
# dbfs:/Volumes///crfs_ops/... Verified by running the pattern in isolation.
BENCH_CATALOG := $(shell test -f .crfs.vars && grep '^catalog=' .crfs.vars | cut -d= -f2-)
BENCH_SCHEMA  := $(shell test -f .crfs.vars && grep '^schema=' .crfs.vars | cut -d= -f2-)
BENCH_SCHEMA  := $(if $(BENCH_SCHEMA),$(BENCH_SCHEMA),crunchyroll_demo)

bench-pull: ## Copy the benchmark write-up out of the ops volume into docs/
	@test -n "$(BENCH_CATALOG)" || { echo "no catalog in .crfs.vars -- run make bootstrap first"; exit 1; }
	@$(DB) fs cp \
	  "dbfs:/Volumes/$(BENCH_CATALOG)/$(BENCH_SCHEMA)/crfs_ops/benchmark/serving_benchmark.md" \
	  docs/serving_benchmark.md --profile $(PROFILE) --overwrite \
	  && echo "wrote docs/serving_benchmark.md"

streaming: deploy ## Start the continuous freshness path (producer + streaming aggregate)
	$(BUNDLE) run crfs_streaming $(FLAGS) --no-wait

burst: deploy ## Fire a small burst of live events at viewer v0001
	$(BUNDLE) run crfs_event_burst $(FLAGS)

agent: deploy ## Log and deploy the explainer agent
	$(BUNDLE) run crfs_agent $(FLAGS)

app-url: ## Print the app URL and compute state
	@$(DB) apps get crfs-watch-next --profile $(PROFILE) -o json | python3 -c \
	  "import json,sys; d=json.load(sys.stdin); print('app:', d.get('url'), '|', (d.get('compute_status') or {}).get('state',''))"

app-logs: ## Tail the app logs
	$(DB) apps logs crfs-watch-next --profile $(PROFILE)

cost: ## Daily DBU and list USD for the online store and the serving endpoints
	@./scripts/cost.sh $(PROFILE)

verify: ## Assert the demo is in a good state
	@./scripts/verify.sh $(PROFILE)

# NOT `teardown-cost: deploy`. In a partially torn-down workspace a deploy can fail --
# recreating the app against an endpoint or Lakebase resource that is already gone -- and
# a failed prerequisite means the teardown never runs while the always-on online store
# keeps billing. The whole point of this target is to stop the meter, so it must not
# depend on anything that can fail first. `bundle run` uses whatever the workspace
# already has.
teardown-cost: ## Delete endpoints, the app and the online store. Keep all data.
	$(BUNDLE) run crfs_teardown $(FLAGS)

teardown: ## Full teardown including UC tables and models. Destructive.
	./scripts/teardown.sh $(PROFILE) --yes

destroy: ## Remove the bundle's own resources (jobs, volume, dashboard, app shell)
	$(BUNDLE) destroy $(FLAGS)
