# Crunchyroll Feature Store on Lakebase.
#
#   make preflight      check the workspace is ready and discover generated ids
#   make deploy         bundle deploy + Postgres grants + start the app
#   make demo           run the whole pipeline end to end
#   make cost           what the online store is billing right now
#   make teardown-cost  stop the money, keep the data
#
# PROFILE and TARGET are overridable:  make deploy PROFILE=my-ws TARGET=prod

PROFILE ?= fe-vm-lakebase-praneeth
TARGET  ?= dev
DB      ?= databricks
BUNDLE   = $(DB) bundle
FLAGS    = -t $(TARGET) --profile $(PROFILE)

.DEFAULT_GOAL := help
.PHONY: help preflight validate deploy deploy-app demo streaming burst agent app-logs app-url \
        cost verify teardown-cost teardown destroy fmt

help:
	@grep -E '^[a-z-]+:.*?##' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

preflight: ## Check prerequisites; print the ids the bundle needs
	@./scripts/preflight.sh $(PROFILE)

validate: ## Strict bundle validation
	$(BUNDLE) validate --strict $(FLAGS)

deploy: validate ## Deploy jobs, volume and dashboard
	$(BUNDLE) deploy $(FLAGS)

deploy-app: ## Create/update the app, deploy its source, grant it access
	./scripts/deploy_app.sh $(PROFILE) $(TARGET)
	@echo
	@echo "Granting Postgres read access. Re-run after any new publish_table --"
	@echo "ALTER DEFAULT PRIVILEGES covers future tables, a plain GRANT does not."
	-./scripts/grant_app_postgres.sh $(PROFILE)

demo: ## Run the full pipeline (about 45 minutes)
	$(BUNDLE) run crfs_end_to_end $(FLAGS)

streaming: ## Start the continuous freshness path (producer + streaming aggregate)
	$(BUNDLE) run crfs_streaming $(FLAGS) --no-wait

burst: ## Fire a small burst of live events at viewer v0001
	$(BUNDLE) run crfs_event_burst $(FLAGS)

agent: ## Log and deploy the explainer agent
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

teardown-cost: ## Delete endpoints, the app and the online store. Keep all data.
	$(BUNDLE) run crfs_teardown $(FLAGS)

teardown: ## Full teardown including UC tables and models. Destructive.
	./scripts/teardown.sh $(PROFILE) --yes

destroy: ## Remove the bundle's own resources (jobs, volume, dashboard, app shell)
	$(BUNDLE) destroy $(FLAGS)
