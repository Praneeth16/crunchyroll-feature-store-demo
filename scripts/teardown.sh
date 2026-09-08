#!/usr/bin/env bash
# Stop the money. Money first, data last, idempotent throughout.
#
#   ./scripts/teardown.sh <profile>              # cost only, keeps all data (default)
#   ./scripts/teardown.sh <profile> --full       # also drops tables, models, functions
#   ./scripts/teardown.sh <profile> --yes        # skip the confirmation
#
# The order is not arbitrary. Endpoints go before the online store, or a serving
# endpoint is left doing feature lookups against a store that no longer exists.
# Synced tables go before their sources. And unpublishing has to drop the
# Postgres table too -- deleting a synced table leaves it behind, and the next
# publish then fails with AlreadyExists while UC shows nothing.
set -uo pipefail

PROFILE="${1:-fe-vm-lakebase-praneeth}"; shift || true
FULL=""; ASSUME_YES=""
for a in "$@"; do
  [ "$a" = "--full" ] && FULL=1
  [ "$a" = "--yes" ] && ASSUME_YES=1
done

CATALOG="${CATALOG:-serverless_lakebase_praneeth_catalog}"
SCHEMA="${SCHEMA:-crunchyroll_demo}"
STORE="${STORE:-crunchyroll-online-store}"
APP="${APP:-crfs-watch-next}"
ENDPOINTS="${ENDPOINTS:-crunchyroll-explainer-agent crunchyroll-viewer-features crunchyroll-candidate-retriever crunchyroll-watch-next-ranker}"
ONLINE="${ONLINE:-online_viewer_features online_title_features online_recent_behavior online_viewer_embedding online_session_features}"
DB=$(command -v databricks || echo /opt/homebrew/bin/databricks)
# Resolve the repo root from this script's location. Assuming cwd silently
# skipped step 4 (dropping synced tables) when run from elsewhere.
HERE="$(cd "$(dirname "$0")/.." && pwd)"

if [ -z "$ASSUME_YES" ]; then
  echo "About to delete from $CATALOG.$SCHEMA on profile $PROFILE:"
  echo "  app $APP, serving endpoints, synced tables, and the online store $STORE"
  [ -n "$FULL" ] && echo "  AND every UC table, registered model and UDF this demo created"
  printf 'Type yes to continue: '
  read -r reply
  [ "$reply" = "yes" ] || { echo "aborted"; exit 1; }
fi

step() { printf '\n== %s\n' "$1"; }
try()  { eval "$@" 2>&1 | tail -2 || true; }

step "1  app"
try "\"$DB\" apps delete \"$APP\" --profile \"$PROFILE\""

step "2  serving endpoints (agent first -- it calls the others)"
for ep in $ENDPOINTS; do
  echo "-- $ep"
  try "\"$DB\" serving-endpoints delete \"$ep\" --profile \"$PROFILE\""
done

step "3  cancel active crfs_* job runs (a CONTINUOUS publish holds a pipeline open)"
"$DB" jobs list --profile "$PROFILE" -o json 2>/dev/null | python3 -c '
import json, subprocess, sys
db, profile = sys.argv[1], sys.argv[2]
for j in json.load(sys.stdin):
    name = ((j.get("settings") or {}).get("name") or "")
    if not name.startswith("crfs_"):
        continue
    runs = subprocess.run([db, "jobs", "list-runs", "--job-id", str(j["job_id"]),
                           "--active-only", "--profile", profile, "-o", "json"],
                          capture_output=True, text=True)
    try:
        for r in json.loads(runs.stdout or "[]"):
            print("  cancelling", name, r["run_id"])
            subprocess.run([db, "jobs", "cancel-run", str(r["run_id"]),
                            "--profile", profile, "--no-wait"], capture_output=True)
    except Exception:
        pass
' "$DB" "$PROFILE"

step "4  synced tables + their Postgres tables"
python3 - "$HERE" "$PROFILE" "$CATALOG" "$SCHEMA" $ONLINE <<'PYEOF'
import sys
sys.path.insert(0, sys.argv[1])
profile, catalog, schema = sys.argv[2], sys.argv[3], sys.argv[4]
tables = sys.argv[5:]
try:
    from databricks.sdk import WorkspaceClient
    from src.crfs.config import Config, DEFAULTS
    from src.crfs import online, ops
except Exception as e:
    # Loud, not silent: a skipped step here leaves synced tables running and billing.
    print("  ERROR cannot import src.crfs / databricks-sdk:", str(e)[:160])
    print("  Synced tables were NOT deleted. Run this from the repo root, or delete them")
    print("  with: databricks api delete /api/2.0/database/synced_tables/<full_name>")
    raise SystemExit(1)

w = WorkspaceClient(profile=profile)
d = dict(DEFAULTS); d.update(catalog=catalog, schema=schema)
cfg = Config(extras={}, **d)
try:
    store = online.from_config(w, cfg)
except Exception as e:
    store = None
    print("  no Postgres handle:", str(e)[:120])
for t in tables:
    ops.drop_synced_if_exists(w, cfg.t(t), online_store=store)
if store:
    store.close()
PYEOF

step "5  the online store -- this is the always-on bill"
try "\"$DB\" api delete \"/api/2.0/feature-store/online-stores/$STORE\" --profile \"$PROFILE\""

if [ -z "$FULL" ]; then
  echo
  echo "Cost-only teardown done. UC tables, models and functions untouched."
  echo "Confirm the meter stopped tomorrow with:  make cost"
  exit 0
fi

step "6  feature spec and the request-time UDFs"
for fn in cr_genre_affinity_match cr_affinity_popularity_cross cr_hour_affinity_delta cr_session_decay; do
  try "\"$DB\" experimental aitools tools query \"DROP FUNCTION IF EXISTS $CATALOG.$SCHEMA.$fn\" --profile \"$PROFILE\""
done

step "7  UC tables and registered models"
for t in viewer_features_ts viewer_features_current title_features recent_behavior_current \
         viewer_embedding_current session_features_current engagement_events_stream \
         crfs_ops_sync_log titles viewers entitlements engagement_events; do
  try "\"$DB\" experimental aitools tools query \"DROP TABLE IF EXISTS $CATALOG.$SCHEMA.$t\" --profile \"$PROFILE\""
done
for m in crunchyroll_ranker crunchyroll_retriever crunchyroll_explainer_agent; do
  try "\"$DB\" registered-models delete \"$CATALOG.$SCHEMA.$m\" --profile \"$PROFILE\""
done

step "8  bundle resources"
echo "run:  databricks bundle destroy -t dev --profile $PROFILE"
echo
echo "Full teardown done."
