#!/usr/bin/env bash
# One command, empty workspace to working demo.
#
#   ./setup.sh --profile <PROFILE>
#
# It discovers or creates the infrastructure, deploys the bundle, runs the whole
# pipeline (horizontal ranking, vertical rail ranking, the shared online store),
# deploys the app, benchmarks the serving endpoint and prints what it measured.
#
# Nothing about this script is specific to the workspace it was written in. The
# three ids that differ per workspace -- warehouse, Lakebase database resource,
# billing endpoint uid -- are resolved at run time by scripts/bootstrap.sh.
#
# Options:
#   --profile NAME     required; the ~/.databrickscfg profile to deploy into
#   --catalog NAME     Unity Catalog to use (default: first writable managed catalog)
#   --schema NAME      schema for every object (default: crunchyroll_demo)
#   --store NAME       online feature store name (default: crunchyroll-online-store)
#   --capacity CU_n    online store capacity class (default: CU_1)
#   --target dev|prod  bundle target (default: dev)
#   --stage NAME       run one stage only: bootstrap|deploy|data|vertical|serve|app|
#                      advanced|bench|verify
#   --with-advanced    also run the preview track: Feature Views and feature versioning
#   --with-gpu         --with-advanced plus GPU training (bills accelerator minutes)
#   --skip-bench       skip the load test (it puts real traffic on the endpoint)
#   --skip-app         skip the Databricks App
#   --yes              do not ask before creating billable infrastructure
set -uo pipefail
cd "$(dirname "$0")"

PROFILE=""; CATALOG=""; SCHEMA="crunchyroll_demo"; STORE="crunchyroll-online-store"
CAPACITY="CU_1"; TARGET="dev"; STAGE=""; SKIP_BENCH=0; SKIP_APP=0; ASSUME_YES=0
# The advanced track is opt-in: both of its APIs are Public Preview, and the GPU job
# bills accelerator minutes. Nothing about the demo depends on either.
WITH_ADVANCED=0; WITH_GPU=0

while [ $# -gt 0 ]; do
  case "$1" in
    --profile)    PROFILE="$2"; shift 2 ;;
    --catalog)    CATALOG="$2"; shift 2 ;;
    --schema)     SCHEMA="$2"; shift 2 ;;
    --store)      STORE="$2"; shift 2 ;;
    --capacity)   CAPACITY="$2"; shift 2 ;;
    --target)     TARGET="$2"; shift 2 ;;
    --stage)      STAGE="$2"; shift 2 ;;
    --with-advanced) WITH_ADVANCED=1; shift ;;
    --with-gpu)      WITH_ADVANCED=1; WITH_GPU=1; shift ;;
    --skip-bench) SKIP_BENCH=1; shift ;;
    --skip-app)   SKIP_APP=1; shift ;;
    --yes|-y)     ASSUME_YES=1; shift ;;
    -h|--help)    sed -n '2,30p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

DB=$(command -v databricks || echo /opt/homebrew/bin/databricks)
step() { printf '\n\033[1m==> %s\033[0m\n' "$1"; }
note() { printf '    %s\n' "$1"; }
die()  { printf '\n\033[31mFAILED: %s\033[0m\n' "$1"; exit 1; }
want() { [ -z "$STAGE" ] || [ "$STAGE" = "$1" ]; }

if [ -z "$PROFILE" ]; then
  echo "usage: ./setup.sh --profile <PROFILE>" >&2
  echo >&2
  echo "profiles in ~/.databrickscfg:" >&2
  "$DB" auth profiles -o json 2>/dev/null | python3 -c '
import json, sys
for p in json.load(sys.stdin).get("profiles", []):
    print("  %-32s %s" % (p.get("name"), p.get("host")), file=sys.stderr)
' || true
  exit 2
fi

START_TS=$(date +%s)

# ---------------------------------------------------------------------- consent
# The online feature store cannot scale to zero. Standing one up is a continuing
# charge, so it is not something a setup script should do silently.
if [ "$ASSUME_YES" != "1" ] && want bootstrap; then
  cat <<EOF

This creates billable Databricks infrastructure in profile '$PROFILE':

  * an Online Feature Store (Lakebase Postgres) at capacity $CAPACITY.
    It CANNOT scale to zero and bills continuously until deleted -- roughly
    \$11/day at CU_1 on the workspace this was measured on. 'make teardown-cost'
    stops it and keeps the data.
  * two Model Serving endpoints. The rail ranker is deliberately configured
    WITHOUT scale-to-zero, because it is meant to sit in a request path.
  * a serverless SQL warehouse (only if none exists), jobs, and a Databricks App.

EOF
  printf "Proceed? [y/N] "
  read -r reply
  case "$reply" in y|Y|yes|YES) ;; *) echo "aborted"; exit 1 ;; esac
fi

# -------------------------------------------------------------------- bootstrap
if want bootstrap; then
  step "1/8  Discovering the workspace"
  BOOT_ARGS=(--profile "$PROFILE" --schema "$SCHEMA" --store "$STORE" --capacity "$CAPACITY")
  [ -n "$CATALOG" ] && BOOT_ARGS+=(--catalog "$CATALOG")
  ./scripts/bootstrap.sh "${BOOT_ARGS[@]}" || die "bootstrap"
fi
[ -f .crfs.vars ] || die "no .crfs.vars -- run without --stage, or run --stage bootstrap first"

# Turn .crfs.vars into --var flags. Comment lines and the CRFS_ prefixed keys are
# not bundle variables. A while-read loop rather than mapfile: macOS still ships
# bash 3.2, where mapfile does not exist and this would silently produce no flags.
load_vars() {
  VARFLAGS=()
  while IFS= read -r line; do
    VARFLAGS+=("--var=$line")
  done < <(grep -v '^#' .crfs.vars | grep -v '^CRFS_' | grep '=')
}
load_vars
CATALOG=$(grep '^catalog=' .crfs.vars | cut -d= -f2-)
SCHEMA=$(grep '^schema=' .crfs.vars | cut -d= -f2-)
STORE=$(grep '^online_store=' .crfs.vars | cut -d= -f2-)
BUNDLE=(--profile "$PROFILE" --target "$TARGET" ${VARFLAGS[@]+"${VARFLAGS[@]}"})

# --------------------------------------------------------------------- deploy
if want deploy; then
  step "2/8  Validating and deploying the bundle"
  ./scripts/render_dashboard.sh || die "render dashboard"
  "$DB" bundle validate "${BUNDLE[@]}" --strict >/dev/null || die "bundle validate"
  note "validated"
  "$DB" bundle deploy "${BUNDLE[@]}" || die "bundle deploy"
  note "jobs, checkpoint volume and dashboard deployed"
fi

# ----------------------------------------------------------------------- data
if want data; then
  step "3/8  Building the shared feature layer and the horizontal ranker (~35 min)"
  note "raw signals -> feature tables -> Lakebase online store -> watch-next ranker"
  "$DB" bundle run crfs_end_to_end "${BUNDLE[@]}" || die "crfs_end_to_end"
fi

# ------------------------------------------------------------------- vertical
if want vertical; then
  step "4/8  Vertical rail ranking on the same feature store (~15 min)"
  note "rail catalog -> homepage log -> rail features -> rail ranker -> request-path endpoint"
  "$DB" bundle run crfs_vertical "${BUNDLE[@]}" || die "crfs_vertical"
fi

# ---------------------------------------------- re-resolve ids that now exist
if want serve || want app; then
  step "5/8  Re-resolving generated ids now that tables are published"
  BOOT_ARGS=(--profile "$PROFILE" --schema "$SCHEMA" --store "$STORE" --capacity "$CAPACITY"
             --catalog "$CATALOG" --no-create)
  ./scripts/bootstrap.sh "${BOOT_ARGS[@]}" >/dev/null 2>&1 || true
  load_vars
  BUNDLE=(--profile "$PROFILE" --target "$TARGET" ${VARFLAGS[@]+"${VARFLAGS[@]}"})
  if grep -q '^lakebase_db_resource=' .crfs.vars; then
    note "lakebase_db_resource = $(grep '^lakebase_db_resource=' .crfs.vars | cut -d= -f2-)"
  else
    note "lakebase_db_resource still unresolved; the app will fall back to Feature Serving"
  fi
fi

# ------------------------------------------------------------------------- app
if want app && [ "$SKIP_APP" != "1" ]; then
  step "6/8  Deploying the app"
  # The app is a bundle resource, so `bundle deploy` (stage 2) already created or
  # updated it and this pushes its source. An app that predates the bundle has to be
  # adopted once with `databricks bundle deployment bind crfs_watch_next <app-name>`.
  "$DB" bundle run crfs_watch_next "${BUNDLE[@]}" || die "app deploy"
  ./scripts/grant_app_postgres.sh "$PROFILE" || note "postgres grants failed; the app falls back to Feature Serving"
  ./scripts/grant_app_endpoints.sh "$PROFILE" || note "endpoint grants incomplete; endpoints created later need a re-run"
  ./scripts/grant_app_uc.sh "$PROFILE" || note "UC grants failed; every Delta-backed panel will come back empty"
fi

# ------------------------------------------------------------------ advanced
# Public Preview APIs, so this probes first and reports rather than dying: a workspace
# without the previews should not fail a setup whose GA demo is complete.
if want advanced && [ "$WITH_ADVANCED" = "1" ]; then
  step "6b/8  Advanced track (Public Preview APIs)"
  note "probing for Feature Views and serverless GPU before using either"
  if "$DB" bundle run crfs_preview_probe "${BUNDLE[@]}"; then
    "$DB" bundle run crfs_feature_views "${BUNDLE[@]}" || note "crfs_feature_views failed -- see the run page"
    "$DB" bundle run crfs_versioning "${BUNDLE[@]}"    || note "crfs_versioning failed -- see the run page"
    if [ "$WITH_GPU" = "1" ]; then
      note "training on a serverless A10 -- this bills accelerator minutes"
      "$DB" bundle run crfs_gpu_train "${BUNDLE[@]}" || note "crfs_gpu_train failed -- see the run page"
    else
      note "skipping GPU training; pass --with-gpu to include it"
    fi
  else
    note "the preview probe failed, so the advanced track was skipped. Run"
    note "'make probe' for what this workspace is missing."
  fi
fi

# ----------------------------------------------------------------- benchmark
if want bench && [ "$SKIP_BENCH" != "1" ]; then
  step "7/8  Benchmarking the rail-ranking endpoint, in region"
  note "fanout, concurrency ramp and a traffic spike -- run as a job so the client"
  note "is in the same region as the endpoint and the numbers are not measuring your wifi"
  "$DB" bundle run crfs_benchmark "${BUNDLE[@]}" || die "crfs_benchmark"
fi

# -------------------------------------------------------------------- verify
if want verify; then
  step "8/8  Verifying"
  ./scripts/verify.sh "$PROFILE" || die "verify"
fi

# ------------------------------------------------------------------- summary
if [ -z "$STAGE" ]; then
  ELAPSED=$(( ($(date +%s) - START_TS) / 60 ))
  step "Done in ${ELAPSED} min"
  RAIL_EP=$(grep '^rail_ranker_endpoint=' .crfs.vars 2>/dev/null | cut -d= -f2- || true)
  RAIL_EP=${RAIL_EP:-crunchyroll-rail-ranker}
  HOST=$("$DB" auth env --profile "$PROFILE" 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["env"]["DATABRICKS_HOST"])' 2>/dev/null)
  cat <<EOF
    catalog.schema      $CATALOG.$SCHEMA
    online store        $STORE  (always on -- 'make teardown-cost' stops it)
    rail ranker         $HOST/ml/endpoints/$RAIL_EP
    watch-next ranker   $HOST/ml/endpoints/crunchyroll-watch-next-ranker
    app                 make app-url PROFILE=$PROFILE
    measured numbers    docs/serving_benchmark.md  (written by the benchmark job)

    Next:
      make bench-local PROFILE=$PROFILE   # the same benchmark from this laptop, for contrast
      make verify      PROFILE=$PROFILE
      make cost        PROFILE=$PROFILE
      make probe       PROFILE=$PROFILE   # is the preview track available here?
      make teardown-cost PROFILE=$PROFILE # stop the meter, keep the data
EOF
fi
