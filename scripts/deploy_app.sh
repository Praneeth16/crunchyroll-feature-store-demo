#!/usr/bin/env bash
# Create or update the watch-next app, and deploy its source.
#
# Why the app is not a bundle resource: with Databricks CLI v1.14.1 against this
# workspace, `bundle deploy` can create an app but can never update one. It sends
# `forward_user_access_token` in the update mask and the Apps API rejects it:
#
#   POST /api/2.0/apps/<app>/update -> 400 INVALID_PARAMETER_VALUE
#   Invalid update mask. Only description, budget_policy_id, usage_policy_id,
#   resources, user_api_scopes, compute_size, compute_min_instances,
#   compute_max_instances, git_repository, git_source, telemetry_export_destinations,
#   compatibility_flags are allowed. Supplied update mask: ... forward_user_access_token ...
#
# So every deploy after the first would fail and take the whole bundle with it.
# `databricks apps create-update` is the supported path and is what this uses.
# The reference bundle resource is kept at docs/app.resource.yml.reference for
# whenever the CLI catches up.
#
#   ./scripts/deploy_app.sh <profile> [target]
set -euo pipefail
PROFILE="${1:-fe-vm-lakebase-praneeth}"
TARGET="${2:-dev}"
APP="${APP:-crfs-watch-next}"
CATALOG="${CATALOG:-serverless_lakebase_praneeth_catalog}"
SCHEMA="${SCHEMA:-crunchyroll_demo}"
WAREHOUSE="${WAREHOUSE:-4d39ac2e32b72a3a}"
PROJECT="${PROJECT:-crunchyroll-online-store}"
BRANCH="${BRANCH:-production}"
LB_ENDPOINT="${LB_ENDPOINT:-primary}"
RANKER="${RANKER:-crunchyroll-watch-next-ranker}"
RETRIEVER="${RETRIEVER:-crunchyroll-candidate-retriever}"
FEATURES="${FEATURES:-crunchyroll-viewer-features}"
AGENT="${AGENT:-crunchyroll-explainer-agent}"
DB=$(command -v databricks || echo /opt/homebrew/bin/databricks)
HERE="$(cd "$(dirname "$0")/.." && pwd)"

echo "== resolving ids"
DB_RESOURCE=$("$DB" postgres list-databases "projects/$PROJECT/branches/$BRANCH" --profile "$PROFILE" -o json \
  | python3 -c "
import json,sys
want='$CATALOG'
for d in json.load(sys.stdin):
    if (d.get('status') or {}).get('postgres_database') == want:
        print(d['name']); break")
[ -n "$DB_RESOURCE" ] || { echo "could not resolve the Lakebase database resource for $CATALOG"; exit 1; }
echo "   lakebase database: $DB_RESOURCE"

BURST_JOB=$("$DB" jobs list --profile "$PROFILE" -o json | python3 -c "
import json,sys
for j in json.load(sys.stdin):
    n=((j.get('settings') or {}).get('name') or '')
    if n.endswith('crfs_event_burst'):
        print(j['job_id']); break")
echo "   burst job: ${BURST_JOB:-<not found; deploy the bundle first>}"

# Only declare endpoints that exist. Declaring a missing one makes the whole
# create/update fail with 404 RESOURCE_DOES_NOT_EXIST.
export PROFILE
RES_JSON=$(python3 - "$WAREHOUSE" "$PROJECT" "$BRANCH" "$DB_RESOURCE" "$BURST_JOB" "$RANKER" "$RETRIEVER" "$FEATURES" "$AGENT" <<'PYEOF'
import json, subprocess, sys, shutil
wh, project, branch, db_res, burst = sys.argv[1:6]
endpoints = sys.argv[6:]
db = shutil.which("databricks") or "/opt/homebrew/bin/databricks"
import os
profile = os.environ["PROFILE"]

res = [
    {"name": "warehouse", "sql_warehouse": {"id": wh, "permission": "CAN_USE"}},
    {"name": "postgres", "postgres": {
        "branch": f"projects/{project}/branches/{branch}",
        "database": db_res,
        "permission": "CAN_CONNECT_AND_CREATE"}},
]
if burst:
    res.append({"name": "burst_job", "job": {"id": burst, "permission": "CAN_MANAGE_RUN"}})
for i, ep in enumerate(endpoints):
    ok = subprocess.run([db, "serving-endpoints", "get", ep, "--profile", profile],
                        capture_output=True).returncode == 0
    if ok:
        res.append({"name": f"endpoint_{i}", "serving_endpoint": {"name": ep, "permission": "CAN_QUERY"}})
    else:
        print(f"   skipping {ep} (does not exist yet)", file=sys.stderr)
print(json.dumps(res))
PYEOF
)

DESC="Crunchyroll watch-next simulator on the Lakebase Online Feature Store"

# create-update needs the app to exist and its compute to be settled. Right after a
# `bundle destroy` (or a manual delete) it sits in DELETING for a while and the call is
# rejected with "App compute needs to be ACTIVE or STOPPED to update."
app_state() {
  if ! "$DB" apps get "$APP" --profile "$PROFILE" -o json >/tmp/crfs_app_get.json 2>/dev/null; then
    printf 'ABSENT'
    return
  fi
  python3 -c 'import json
try:
    d = json.load(open("/tmp/crfs_app_get.json"))
    print(((d.get("compute_status") or {}).get("state") or "UNKNOWN").strip(), end="")
except Exception:
    print("ABSENT", end="")'
}

STATE=$(app_state)
tries=0
while [ "$STATE" = "DELETING" ] || [ "$STATE" = "STARTING" ] || [ "$STATE" = "UPDATING" ]; do
  tries=$((tries + 1))
  [ "$tries" -gt 40 ] && break
  echo "   app compute is $STATE, waiting..."
  sleep 15
  STATE=$(app_state)
done
echo "   app state: $STATE"

if [ "$STATE" = "ABSENT" ]; then
  echo "== create $APP"
  cat > /tmp/crfs_app.json <<JSONEOF
{
  "name": "$APP",
  "description": "$DESC",
  "resources": $RES_JSON
}
JSONEOF
  "$DB" apps create --json @/tmp/crfs_app.json --profile "$PROFILE" --no-wait 2>&1 | tail -3
else
  echo "== create-update $APP"
  cat > /tmp/crfs_app.json <<JSONEOF
{
  "update_mask": "description,resources",
  "app": {
    "description": "$DESC",
    "resources": $RES_JSON
  }
}
JSONEOF
  "$DB" apps create-update "$APP" --json @/tmp/crfs_app.json --profile "$PROFILE" 2>&1 | tail -3
fi

# The app has to be RUNNING before source can be deployed:
#   Cannot deploy app <app> as it is not in RUNNING state. Please start the app first.
STATE=$(app_state)
if [ "$STATE" != "ACTIVE" ]; then
  echo "== starting $APP (state $STATE)"
  "$DB" apps start "$APP" --profile "$PROFILE" --no-wait >/dev/null 2>&1 || true
  for _ in $(seq 1 40); do
    STATE=$(app_state)
    [ "$STATE" = "ACTIVE" ] && break
    [ "$STATE" = "ERROR" ] && { echo "   app compute is ERROR - check: $DB apps logs $APP"; break; }
    echo "   compute $STATE, waiting..."
    sleep 15
  done
  echo "   app state: $STATE"
fi

echo "== syncing app source and deploying"
WS_PATH="/Workspace/Users/$("$DB" current-user me --profile "$PROFILE" -o json | python3 -c 'import json,sys; print(json.load(sys.stdin)["userName"])')/crfs_app"
"$DB" workspace import-dir "$HERE/app" "$WS_PATH" --overwrite --profile "$PROFILE" >/dev/null
"$DB" apps deploy "$APP" --source-code-path "$WS_PATH" --profile "$PROFILE" 2>&1 | tail -4

echo "== url"
"$DB" apps get "$APP" --profile "$PROFILE" -o json | python3 -c '
import json,sys; d=json.load(sys.stdin)
print("  ", d.get("url"))
print("   compute:", (d.get("compute_status") or {}).get("state"))
print("   sp:", d.get("service_principal_client_id"))'
echo
echo "Now grant it read access to the online tables:"
echo "  ./scripts/grant_app_postgres.sh $PROFILE $APP"
