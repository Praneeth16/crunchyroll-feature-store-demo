#!/usr/bin/env bash
# Grant the app's service principal the Unity Catalog access it needs to read the
# Delta tables behind its panels.
#
# Why this exists: app deployment granted Postgres (grant_app_postgres.sh) and endpoint
# CAN_QUERY (grant_app_endpoints.sh), and nothing granted Unity Catalog. The app's
# Delta-backed panels therefore came back empty, and because app.py's _sql() treated a
# failed statement as zero rows, the page rendered "No rail catalog yet - run
# `make vertical`" against a fully populated table. The missing privilege looked like a
# missing pipeline. Verified: the app SP was absent from the catalog's grant list.
#
#   ./scripts/grant_app_uc.sh [profile] [app]
set -uo pipefail
PROFILE="${1:-fe-vm-lakebase-praneeth}"
APP="${2:-${APP:-crfs-watch-next}}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"
if [ -f "$HERE/.crfs.vars" ]; then
  CRFS_CATALOG=$(grep '^catalog=' "$HERE/.crfs.vars" | cut -d= -f2- || true)
  CRFS_SCHEMA=$(grep '^schema=' "$HERE/.crfs.vars" | cut -d= -f2- || true)
fi
CATALOG="${CATALOG:-${CRFS_CATALOG:-serverless_lakebase_praneeth_catalog}}"
SCHEMA="${SCHEMA:-${CRFS_SCHEMA:-crunchyroll_demo}}"
DB=$(command -v databricks || echo /opt/homebrew/bin/databricks)

SP=$("$DB" apps get "$APP" --profile "$PROFILE" -o json 2>/dev/null \
     | python3 -c 'import json,sys; print(json.load(sys.stdin).get("service_principal_client_id",""))')
if [ -z "$SP" ]; then
  echo "app $APP not found or has no service principal yet - deploy the app first"
  exit 1
fi
echo "app service principal: $SP"

# SELECT is granted at the SCHEMA level, not per table, on purpose: the pipeline uses
# CREATE OR REPLACE TABLE, which replaces the securable and would discard table-level
# grants on every rebuild.
grant() {
  local kind="$1" name="$2" privs="$3"
  "$DB" grants update "$kind" "$name" --profile "$PROFILE" --json "{
    \"changes\": [{\"principal\": \"$SP\", \"add\": [$privs]}]}" >/dev/null 2>&1 \
    && echo "  granted $(echo "$privs" | tr -d '\"') on $kind $name" \
    || echo "  FAILED to grant on $kind $name"
}

grant CATALOG "$CATALOG" '"USE_CATALOG"'
grant SCHEMA  "$CATALOG.$SCHEMA" '"USE_SCHEMA", "SELECT"'

echo
echo "verifying the SP can now be seen on the schema:"
"$DB" grants get SCHEMA "$CATALOG.$SCHEMA" --profile "$PROFILE" -o json 2>/dev/null \
  | python3 -c "
import json,sys
sp='$SP'
d=json.load(sys.stdin)
hit=[a for a in (d.get('privilege_assignments') or []) if a.get('principal')==sp]
print('  ', hit[0]['principal'], '->', ','.join(hit[0]['privileges'])) if hit else print('   NOT PRESENT')
"
