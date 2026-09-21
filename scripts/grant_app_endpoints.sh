#!/usr/bin/env bash
# Grant the app's service principal CAN_QUERY on the endpoints that are created
# by notebooks rather than by the bundle.
#
# Why this is a script and not an app resource: declaring a serving_endpoint
# resource for an endpoint that does not exist yet makes app creation fail with
# 404 RESOURCE_DOES_NOT_EXIST, so `bundle deploy` would be impossible on a fresh
# workspace until the whole pipeline had run. Re-run this after notebooks 07, 08,
# 12 and 23 have created their endpoints.
set -uo pipefail
PROFILE="${1:-fe-vm-lakebase-praneeth}"
APP="${APP:-crfs-watch-next}"
# crunchyroll-rail-ranker is included: the app's vertical-ranking page cannot render
# without CAN_QUERY on it, and it is created by notebook 23 rather than by the bundle.
ENDPOINTS="${ENDPOINTS:-crunchyroll-rail-ranker crunchyroll-candidate-retriever crunchyroll-viewer-features crunchyroll-explainer-agent}"
DB=$(command -v databricks || echo /opt/homebrew/bin/databricks)

SP=$("$DB" apps get "$APP" --profile "$PROFILE" -o json 2>/dev/null \
     | python3 -c 'import json,sys; d=sys.stdin.read().strip(); print(json.loads(d).get("service_principal_client_id","") if d else "")')
if [ -z "$SP" ]; then
  echo "app $APP not found or has no service principal yet - deploy the app first"
  exit 1
fi
echo "app service principal: $SP"

for ep in $ENDPOINTS; do
  # `json.load` on the empty stdin of a 404 raises, and the traceback reads like the
  # script broke rather than like the endpoint simply is not there yet.
  id=$("$DB" serving-endpoints get "$ep" --profile "$PROFILE" -o json 2>/dev/null \
       | python3 -c 'import json,sys; d=sys.stdin.read().strip(); print(json.loads(d).get("id","") if d else "")')
  if [ -z "$id" ]; then
    echo "  skip $ep (does not exist yet)"
    continue
  fi
  "$DB" serving-endpoints update-permissions "$id" --profile "$PROFILE" --json "{
    \"access_control_list\": [
      {\"service_principal_name\": \"$SP\", \"permission_level\": \"CAN_QUERY\"}
    ]}" >/dev/null 2>&1 \
    && echo "  granted CAN_QUERY on $ep" \
    || echo "  FAILED to grant on $ep"
done
