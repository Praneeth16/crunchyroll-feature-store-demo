#!/usr/bin/env bash
# Assert the demo is in a state worth presenting. Read-only.
set -uo pipefail
PROFILE="${1:-fe-vm-lakebase-praneeth}"
CATALOG="${CATALOG:-serverless_lakebase_praneeth_catalog}"
SCHEMA="${SCHEMA:-crunchyroll_demo}"
STORE="${STORE:-crunchyroll-online-store}"
DB=$(command -v databricks || echo /opt/homebrew/bin/databricks)
fail=0
ok()  { printf '  \033[32mok\033[0m    %s\n' "$1"; }
bad() { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; fail=1; }

q() { "$DB" experimental aitools tools query "$1" --profile "$PROFILE" 2>/dev/null; }

echo "verify  $CATALOG.$SCHEMA"

# The data clock: the first version of this demo shipped features that were a
# week older than the wall clock the freshness beat used.
freshness=$(q "SELECT datediff(current_date(), max(to_date(event_ts))) AS d FROM $CATALOG.$SCHEMA.engagement_events" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["d"])' 2>/dev/null)
if [ -n "$freshness" ] && [ "$freshness" -le 2 ]; then
  ok "history ends ${freshness}d ago"
else
  bad "history ends ${freshness:-?}d ago - re-run notebook 00 (the demo clock has drifted)"
fi

for t in viewer_features_current recent_behavior_current title_features viewer_features_ts; do
  n=$(q "SELECT count(*) AS n FROM $CATALOG.$SCHEMA.$t" | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["n"])' 2>/dev/null)
  [ -n "$n" ] && [ "$n" -gt 0 ] && ok "$t: $n rows" || bad "$t: empty or missing"
done

cols=$(q "SELECT count(*) AS n FROM $CATALOG.information_schema.columns WHERE table_schema='$SCHEMA' AND table_name='viewer_features_current' AND column_name IN ('typical_watch_hour','hour_concentration')" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["n"])' 2>/dev/null)
[ "$cols" = "2" ] && ok "viewer features carry the on-demand inputs" \
  || bad "viewer_features_current missing typical_watch_hour/hour_concentration - re-run notebook 01"

for fn in cr_genre_affinity_match cr_affinity_popularity_cross cr_hour_affinity_delta cr_session_decay; do
  q "DESCRIBE FUNCTION $CATALOG.$SCHEMA.$fn" >/dev/null 2>&1 && ok "udf $fn" || bad "udf $fn missing"
done

state=$("$DB" api get "/api/2.0/feature-store/online-stores/$STORE" --profile "$PROFILE" 2>/dev/null \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["state"], d["capacity"])' 2>/dev/null)
[ -n "$state" ] && ok "online store: $state" || bad "online store $STORE unavailable"

for ep in crunchyroll-watch-next-ranker crunchyroll-viewer-features crunchyroll-candidate-retriever; do
  s=$("$DB" serving-endpoints get "$ep" --profile "$PROFILE" -o json 2>/dev/null \
    | python3 -c 'import json,sys; d=json.load(sys.stdin); print((d.get("state") or {}).get("ready",""))' 2>/dev/null)
  [ "$s" = "READY" ] && ok "endpoint $ep READY" || bad "endpoint $ep: ${s:-missing}"
done

echo
[ "$fail" -eq 0 ] && echo "verify passed" || echo "verify FAILED"
exit "$fail"
