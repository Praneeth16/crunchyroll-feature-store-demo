#!/usr/bin/env bash
# Check the workspace can run this demo, and print the generated ids the bundle
# cannot guess. Read-only unless --resize is passed.
set -uo pipefail

PROFILE="${1:-fe-vm-lakebase-praneeth}"
shift || true
RESIZE=""
MIN_CU="${MIN_CU:-4}"
MAX_CU="${MAX_CU:-8}"
for arg in "$@"; do
  [ "$arg" = "--resize" ] && RESIZE=1
done

CATALOG="${CATALOG:-serverless_lakebase_praneeth_catalog}"
SCHEMA="${SCHEMA:-crunchyroll_demo}"
STORE="${STORE:-crunchyroll-online-store}"
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
if [ -n "$ver" ]; then ok "databricks CLI $ver"; else bad "databricks CLI not found"; fi

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
