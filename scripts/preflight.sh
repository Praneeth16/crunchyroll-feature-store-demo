#!/usr/bin/env bash
# Check the workspace can run this demo, and print the generated ids the bundle
# cannot guess. Read-only unless --resize is passed.
set -uo pipefail

PROFILE="${1:-$(grep -s '^CRFS_PROFILE=' "$(dirname "$0")/../.crfs.vars" | cut -d= -f2- || true)}"
[ -n "$PROFILE" ] || { echo "usage: $0 <PROFILE>   (databricks auth profiles lists yours)" >&2; exit 2; }
shift || true
RESIZE=""
MIN_CU="${MIN_CU:-4}"
MAX_CU="${MAX_CU:-8}"
for arg in "$@"; do
  [ "$arg" = "--resize" ] && RESIZE=1
done

# Prefer whatever scripts/bootstrap.sh discovered. Falling back to this workspace's
# ids is fine for the author and wrong for everyone else, so the fallback is last.
HERE="$(cd "$(dirname "$0")/.." && pwd)"
if [ -f "$HERE/.crfs.vars" ]; then
  CRFS_CATALOG=$(grep '^catalog=' "$HERE/.crfs.vars" | cut -d= -f2-)
  CRFS_SCHEMA=$(grep '^schema=' "$HERE/.crfs.vars" | cut -d= -f2-)
  CRFS_STORE=$(grep '^online_store=' "$HERE/.crfs.vars" | cut -d= -f2-)
fi
CATALOG="${CATALOG:-${CRFS_CATALOG:-}}"
[ -n "$CATALOG" ] || { echo "no catalog: set CATALOG=... or run scripts/bootstrap.sh first" >&2; exit 2; }
SCHEMA="${SCHEMA:-${CRFS_SCHEMA:-crunchyroll_demo}}"
STORE="${STORE:-${CRFS_STORE:-crunchyroll-online-store}}"
PROJECT="${PROJECT:-$STORE}"
BRANCH="${BRANCH:-production}"
ENDPOINT="${ENDPOINT:-primary}"
LLM="${LLM:-databricks-claude-sonnet-4-5}"

DB=$(command -v databricks || echo /opt/homebrew/bin/databricks)
fail=0
ok()   { printf '  \033[32mok\033[0m    %s\n' "$1"; }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; fail=1; }
warn() { printf '  \033[33mwarn\033[0m  %s\n' "$1"; }

echo "preflight  profile=$PROFILE  target=$CATALOG.$SCHEMA"
echo

ver=$("$DB" --version 2>/dev/null | tr -d 'v' | awk '{print $NF}')
if [ -z "$ver" ]; then
  bad "databricks CLI not found"
elif python3 -c "import sys; sys.exit(tuple(map(int, '$ver'.split('.')[:3])) < (1, 17, 0))" 2>/dev/null; then
  ok "databricks CLI $ver"
else
  # 1.14.1 cannot update an app in a bundle (docs/risks.md §8b).
  bad "databricks CLI $ver is older than 1.17.0 - brew upgrade databricks"
fi

node_major=$(node --version 2>/dev/null | tr -d 'v' | cut -d. -f1)
if [ -n "$node_major" ] && [ "$node_major" -ge 18 ] && command -v npm >/dev/null; then
  ok "node $(node --version) + npm (builds the app frontend)"
else
  bad "Node.js >= 18 with npm is required to build the app frontend - brew install node"
fi

if user=$("$DB" current-user me --profile "$PROFILE" -o json 2>/dev/null \
          | python3 -c 'import json,sys; print(json.load(sys.stdin)["userName"])' 2>/dev/null); then
  ok "authenticated as $user"
else
  bad "cannot authenticate with profile $PROFILE"; echo; exit 1
fi

command -v psql >/dev/null && ok "psql present" || warn "psql not on PATH (only needed for scripts/lakebase_explore.sh)"
python3 -c 'import psycopg' 2>/dev/null && ok "psycopg importable locally" \
  || warn "psycopg missing locally (pip install 'psycopg[binary]') - needed by scripts/measure_online_latency.py"

if "$DB" schemas get "$CATALOG.$SCHEMA" --profile "$PROFILE" >/dev/null 2>&1; then
  ok "schema $CATALOG.$SCHEMA exists"
else
  warn "schema $CATALOG.$SCHEMA missing - notebook 00 creates it"
fi

wh=$("$DB" experimental aitools tools get-default-warehouse --profile "$PROFILE" 2>/dev/null | tr -d '"')
[ -n "$wh" ] && ok "default warehouse $wh" || warn "no default warehouse resolved"

store_json=$("$DB" api get "/api/2.0/feature-store/online-stores/$STORE" --profile "$PROFILE" 2>/dev/null)
if [ -n "$store_json" ]; then
  echo "$store_json" | python3 -c '
import json,sys
d = json.load(sys.stdin)
print("  ok    online store %s: state=%s capacity=%s replicas=%s"
      % (d["name"], d["state"], d["capacity"], d.get("read_replica_count")))
print("        online stores cannot scale to zero - this compute bills continuously")
'
else
  warn "online store $STORE not found - notebook 01 creates it"
fi

ep_path="projects/$PROJECT/branches/$BRANCH/endpoints/$ENDPOINT"
ep_json=$("$DB" postgres get-endpoint "$ep_path" --profile "$PROFILE" -o json 2>/dev/null)
if [ -n "$ep_json" ]; then
  echo "$ep_json" | python3 -c '
import json,sys
s = json.load(sys.stdin)["status"]
print("  ok    lakebase endpoint %s: %s %s-%s CU"
      % (s["endpoint_id"], s["current_state"],
         s.get("autoscaling_limit_min_cu"), s.get("autoscaling_limit_max_cu")))
'
  uid=$(echo "$ep_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["uid"])')
  host=$(echo "$ep_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"]["hosts"]["host"])')
  echo "        endpoint_uid = $uid   (use this to filter system.billing.usage)"
  echo "        host         = $host"
  echo "        note: the -pooler host rejects OAuth tokens (SASL authentication failed)."
else
  warn "lakebase endpoint $ep_path not found"
fi

db_res=$("$DB" postgres list-databases "projects/$PROJECT/branches/$BRANCH" --profile "$PROFILE" -o json 2>/dev/null \
  | python3 -c "
import json,sys
want='$CATALOG'
for d in json.load(sys.stdin):
    if (d.get('status') or {}).get('postgres_database') == want:
        print(d['name']); break
" 2>/dev/null)
if [ -n "$db_res" ]; then
  ok "lakebase database resource resolved"
  echo "        set this in databricks.yml as var.lakebase_db_resource:"
  echo "        $db_res"
else
  warn "could not resolve the Lakebase database resource for $CATALOG"
  # This is the state a genuinely empty workspace is in, and it matters for more than the
  # app's env: resources/app.yml binds this id as a `postgres` resource, so a deploy now
  # would bind whatever default databricks.yml carries -- another workspace's database.
  echo "        On a fresh workspace this is expected: the id is generated by"
  echo "        fe.create_online_store in notebook 01. Order of operations:"
  echo "          make bootstrap   # create or find the store, write the real id"
  echo "          make demo        # notebook 01 creates the store on first run"
  echo "          make deploy      # only now does the app bind the right database"
  echo "        setup.sh already does this in the right order."
fi

if "$DB" serving-endpoints get "$LLM" --profile "$PROFILE" >/dev/null 2>&1; then
  ok "LLM endpoint $LLM reachable"
else
  warn "LLM endpoint $LLM not found - set var.llm_endpoint to one from: databricks serving-endpoints list"
fi

if [ -n "$RESIZE" ]; then
  echo
  echo "resizing $ep_path to ${MIN_CU}-${MAX_CU} CU"
  echo "  note: the online store's capacity class governs the endpoint floor."
  echo "  Changing the class (fe.update_online_store) moves these bounds by itself."
  "$DB" postgres update-endpoint "$ep_path" \
    --json "{\"autoscaling_limit_min_cu\": $MIN_CU, \"autoscaling_limit_max_cu\": $MAX_CU}" \
    --profile "$PROFILE" 2>&1 | tail -5
fi

echo
[ "$fail" -eq 0 ] && echo "preflight passed" || echo "preflight FAILED"
exit "$fail"
