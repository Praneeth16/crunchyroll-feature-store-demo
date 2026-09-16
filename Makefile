# Crunchyroll Feature Store on Lakebase.
#
#   make up             one command, empty workspace to working demo
#   make preflight      check the workspace is ready and discover generated ids
#   make deploy         bundle deploy + Postgres grants + start the app
#   make demo           the horizontal (title) pipeline, end to end
#   make vertical       the vertical (rail) pipeline, end to end
#   make bench          measure the rail endpoint under load, in region
#   make bench-local    the same benchmark from this laptop, for contrast
#   make cost           what the online store is billing right now
#   make teardown-cost  stop the money, keep the data
#
# PROFILE and TARGET are overridable:  make deploy PROFILE=my-ws TARGET=prod

PROFILE ?= fe-vm-lakebase-praneeth
TARGET  ?= dev
DB      ?= databricks
BUNDLE   = $(DB) bundle
# Generated per-workspace ids, if scripts/bootstrap.sh has run. Absent, the
# defaults in databricks.yml apply -- which are the ids of the workspace this was
# built on and will be wrong anywhere else.
VARS    := $(shell test -f .crfs.vars && grep -v '^\#' .crfs.vars | grep -v '^CRFS_' | grep '=' | sed 's/^/--var=/' | tr '\n' ' ')
FLAGS    = -t $(TARGET) --profile $(PROFILE) $(VARS)

.DEFAULT_GOAL := help
.PHONY: help up bootstrap render preflight validate deploy deploy-app demo vertical bench bench-local \
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

validate: render ## Strict bundle validation
	$(BUNDLE) validate --strict $(FLAGS)

deploy: validate ## Deploy jobs, volume and dashboard
	$(BUNDLE) deploy $(FLAGS)

deploy-app: ## Create/update the app, deploy its source, grant it access
	./scripts/deploy_app.sh $(PROFILE) $(TARGET)
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

bench: deploy ## Measure the rail endpoint under load from inside the region
	$(BUNDLE) run crfs_benchmark $(FLAGS)

bench-local: ## The same benchmark from this laptop -- the contrast is the point
	python3 scripts/benchmark_local.py --profile $(PROFILE)

# The catalog/schema fallbacks are resolved by make, not by a shell `||`: in
# `$(grep ... | cut ... || echo default)` the `||` tests the PIPELINE's status, which is
# `cut`'s, and cut succeeds on empty input. So with no .crfs.vars (the fresh-clone
# state -- the file is gitignored) both expanded to empty and the path became
# dbfs:/Volumes///crfs_ops/... Verified by running the pattern in isolation.
BENCH_CATALOG := $(shell test -f .crfs.vars && grep '^catalog=' .crfs.vars | cut -d= -f2-)
BENCH_SCHEMA  := $(shell test -f .crfs.vars && grep '^schema=' .crfs.vars | cut -d= -f2-)
BENCH_CATALOG := $(if $(BENCH_CATALOG),$(BENCH_CATALOG),serverless_lakebase_praneeth_catalog)
BENCH_SCHEMA  := $(if $(BENCH_SCHEMA),$(BENCH_SCHEMA),crunchyroll_demo)

bench-pull: ## Copy the benchmark write-up out of the ops volume into docs/
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

teardown-cost: deploy ## Delete endpoints, the app and the online store. Keep all data.
	$(BUNDLE) run crfs_teardown $(FLAGS)

teardown: ## Full teardown including UC tables and models. Destructive.
	./scripts/teardown.sh $(PROFILE) --yes

destroy: ## Remove the bundle's own resources (jobs, volume, dashboard, app shell)
	$(BUNDLE) destroy $(FLAGS)
