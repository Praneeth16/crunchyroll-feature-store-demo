#!/usr/bin/env bash
# Assert the demo is in a state worth presenting. Read-only.
set -uo pipefail
PROFILE="${1:-$(grep -s '^CRFS_PROFILE=' "$(dirname "$0")/../.crfs.vars" | cut -d= -f2- || true)}"
[ -n "$PROFILE" ] || { echo "usage: $0 <PROFILE>   (databricks auth profiles lists yours)" >&2; exit 2; }
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

# recent_behavior_ts and rail_features_ts are the point-in-time sources the rail ranker's
# feature spec resolves. Without them the training lookups fall back to nothing -- and the
# demo's headline metrics were invalid while they were missing.
for t in viewer_features_current recent_behavior_current title_features viewer_features_ts recent_behavior_ts; do
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

# READY is not the same as working. The retriever endpoint reported READY while every
# query to it failed with `Error ''` (a NameError inside the model, invisible from the
# endpoint state), and this script passed it -- so a green verify certified a
# non-functional endpoint. Each request-path endpoint now gets one real query.
echo
echo "endpoints answer a real request"
# Two attempts, because a scale-to-zero endpoint's FIRST request after idle legitimately
# takes 30-60s+ while a container starts, and the CLI gives up after 60s of inactivity.
# One attempt made this check fail on a cold retriever while the endpoint was perfectly
# healthy -- a correctness check that reports a cold start as a defect trains people to
# ignore it. The first attempt doubles as the warm-up.
query_endpoint() {
  local ep="$1" payload="$2" attempt out
  for attempt in 1 2; do
    out=$("$DB" api post "/serving-endpoints/$ep/invocations" --profile "$PROFILE" \
          --json "$payload" 2>&1)
    if printf '%s' "$out" | grep -qE '"(predictions|outputs)"'; then
      if [ "$attempt" = "1" ]; then
        ok "$ep answered"
      else
        ok "$ep answered on the second attempt (first request warmed a scaled-to-zero container)"
      fi
      return
    fi
    [ "$attempt" = "1" ] && printf '        %s\n' "$ep did not answer within the client timeout; retrying once after the warm-up"
  done
  bad "$ep is READY but does not answer: $(printf '%s' "$out" | tr '\n' ' ' | cut -c1-180)"
}
query_endpoint crunchyroll-candidate-retriever \
  '{"dataframe_records":[{"viewer_id":"v0001","top_k":10}]}'
RAIL_EP_Q="${RAIL_EP:-crunchyroll-rail-ranker}"
query_endpoint "$RAIL_EP_Q" \
  '{"dataframe_records":[{"viewer_id":"v0001","rail_id":"r_trending","device":"tv","locale":"en-US","hour_of_day":21,"day_of_week":5,"request_epoch_s":1788210000}]}'

# ---------------------------------------------------------------- vertical path
echo
echo "vertical (rail) ranking"

for t in rails rail_impressions rail_position_propensity rail_features rail_features_ts viewer_rail_features_ts; do
  n=$(q "SELECT count(*) AS n FROM $CATALOG.$SCHEMA.$t" | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["n"])' 2>/dev/null)
  [ -n "$n" ] && [ "$n" -gt 0 ] && ok "$t: $n rows" || bad "$t: empty or missing - run 'make vertical'"
done

# The whole one-table architecture rests on this: the published copy must hold
# exactly one row per (viewer_id, rail_id), not every snapshot.
dedup=$(q "SELECT (SELECT count(*) FROM $CATALOG.$SCHEMA.online_viewer_rail) AS online, (SELECT count(*) FROM (SELECT DISTINCT viewer_id, rail_id FROM $CATALOG.$SCHEMA.viewer_rail_features_ts)) AS keys" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin)[0]; print(d["online"], d["keys"])' 2>/dev/null)
set -- ${dedup:-0 1}
if [ "$1" = "$2" ] && [ "$1" != "0" ]; then
  ok "online_viewer_rail is one row per (viewer, rail): $1"
else
  bad "online_viewer_rail has $1 rows for $2 keys - the time series publish did not deduplicate"
fi

for fn in cr_rail_taste_match cr_rail_click_recency cr_device_rail_fit; do
  q "DESCRIBE FUNCTION $CATALOG.$SCHEMA.$fn" >/dev/null 2>&1 && ok "udf $fn" || bad "udf $fn missing"
done

RAIL_EP="${RAIL_EP:-crunchyroll-rail-ranker}"
rail_json=$("$DB" serving-endpoints get "$RAIL_EP" --profile "$PROFILE" -o json 2>/dev/null)
if [ -n "$rail_json" ]; then
  # The endpoint JSON goes through a temp file and the Python goes in a *quoted*
  # heredoc, deliberately. This block was previously `python3 -c '...'` and contained
  # f-strings like f"endpoint {d.get('name')}" -- whose inner single quotes terminate
  # the shell's own single-quoted string, so the shell handed Python the source
  # `d.get(name)` and every run died with `NameError: name 'name' is not defined`.
  # The rail ranker's endpoint assertions therefore never ran, while the rest of
  # verify still printed green. A quoted heredoc has no such hazard.
  printf '%s' "$rail_json" > /tmp/crfs_rail_ep.json
  python3 <<'PYEOF' || fail=1
import json, sys
d = json.load(open("/tmp/crfs_rail_ep.json"))
ready = (d.get("state") or {}).get("ready", "")
ents = ((d.get("config") or {}).get("served_entities") or [{}])
e = ents[0]
mark = lambda good, msg: print(("  \033[32mok\033[0m    " if good else "  \033[31mFAIL\033[0m  ") + msg)
warn = lambda msg: print("  \033[33mwarn\033[0m  " + msg)
name = d.get("name") or "unknown"
mark(ready == "READY", "endpoint {}: {}".format(name, ready or "unknown"))
# The one setting that decides whether this endpoint belongs in a request path, and
# the only endpoint property this script is willing to fail on.
stz = e.get("scale_to_zero_enabled")
mark(stz is False, "scale_to_zero_enabled={} (must be false for a request path)".format(stz))
# The served model version, so a stale endpoint pinned to an old version is visible
# rather than passing quietly because it happens to be READY.
mark(bool(e.get("entity_version")),
     "serving {} version {}".format(e.get("entity_name"), e.get("entity_version")))
# Route optimization is a warning, not a failure: it is create-time only and it was
# rejected on the workspace this was built against, so a correct, working, non-route-
# optimized endpoint must not fail a setup. The benchmark reports which path it used.
if d.get("route_optimized"):
    mark(True, "route_optimized=True")
else:
    warn("route_optimized=False - not enabled on this workspace, or the endpoint "
         "predates it. Latency carries the standard workspace request path overhead; "
         "the benchmark records this beside its numbers.")
minc, maxc = e.get("min_provisioned_concurrency"), e.get("max_provisioned_concurrency")
if minc is not None:
    mark(True, "provisioned concurrency {}-{}".format(minc, maxc))
else:
    mark(True, "workload_size={} (no explicit provisioned concurrency)".format(e.get("workload_size")))
inf = ((d.get("ai_gateway") or {}).get("inference_table_config") or {})
mark(bool(inf.get("enabled")), "inference table enabled={}".format(inf.get("enabled")))
sys.exit(0 if ready == "READY" and stz is False else 1)
PYEOF
  rm -f /tmp/crfs_rail_ep.json
else
  bad "endpoint $RAIL_EP missing - run 'make vertical'"
fi

model_ok=$(q "SELECT count(*) AS n FROM $CATALOG.information_schema.tables WHERE table_schema='$SCHEMA' AND table_name='crfs_serving_benchmark'" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["n"])' 2>/dev/null)
[ "$model_ok" = "1" ] && ok "benchmark results table present" \
  || printf '  \033[33mwarn\033[0m  no benchmark results yet - run '"'"'make bench'"'"'\n'

echo
[ "$fail" -eq 0 ] && echo "verify passed" || echo "verify FAILED"
exit "$fail"
