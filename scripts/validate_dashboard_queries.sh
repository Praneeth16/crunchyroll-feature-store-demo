#!/usr/bin/env bash
# Run every dataset query in the dashboard and report which ones execute.
#
# The previous version of this script kept its own hand-typed copy of each query,
# which is a guarantee that the two drift: a dashboard dataset could be edited and
# still "validate" against the old SQL. This reads the queries out of the rendered
# dashboard file, so it can only pass if the thing that gets deployed works.
#
#   scripts/validate_dashboard_queries.sh [profile]
#
# Queries against tables a stage has not created yet are reported as SKIP, not
# FAIL -- the benchmark datasets are legitimately empty until `make bench` runs.
set -uo pipefail
PROFILE="${1:-fe-vm-lakebase-praneeth}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"
DASH="$HERE/dashboards/generated/crfs_feature_ops.lvdash.json"
DB=$(command -v databricks || echo /opt/homebrew/bin/databricks)

if [ ! -f "$DASH" ]; then
  echo "no rendered dashboard at $DASH -- run scripts/render_dashboard.sh first" >&2
  exit 2
fi

names=$(python3 -c "
import json, sys
d = json.load(open('$DASH'))
print('\n'.join(x['name'] for x in d['datasets']))
")

fail=0; skipped=0; passed=0
echo "validating dashboard queries  profile=$PROFILE"
echo

for ds in $names; do
  python3 -c "
import json
d = json.load(open('$DASH'))
for x in d['datasets']:
    if x['name'] == '$ds':
        print(''.join(x['queryLines']))
" > /tmp/crfs_dash_query.sql

  # Run from outside the bundle directory: inside it the CLI resolves auth from
  # databricks.yml and errors with "multiple profiles matched" when more than one
  # profile points at the same host. And pass the SQL after `--` because a query
  # that starts with a comment otherwise looks like a flag.
  out=$(cd /tmp && "$DB" experimental aitools tools query --profile "$PROFILE" \
        -- "$(cat /tmp/crfs_dash_query.sql)" 2>&1)
  if printf '%s' "$out" | grep -q "TABLE_OR_VIEW_NOT_FOUND"; then
    printf '  \033[33mskip\033[0m  %-24s table not created yet\n' "$ds"
    skipped=$((skipped + 1))
  # Match the CLI's own failure prefixes, not the word "error" anywhere in the
  # result -- the serving_latency dataset returns a column called error_pct, and a
  # loose grep marked a passing query as failed.
  elif printf '%s' "$out" | grep -qE "^Error:|^ *Error:|query failed:|AnalysisException|PARSE_SYNTAX_ERROR|UNRESOLVED_COLUMN"; then
    printf '  \033[31mFAIL\033[0m  %-24s %s\n' "$ds" \
      "$(printf '%s' "$out" | head -2 | tr '\n' ' ' | cut -c1-160)"
    fail=$((fail + 1))
  else
    rows=$(printf '%s' "$out" | grep -c '^  {' || true)
    printf '  \033[32mok\033[0m    %-24s %s rows\n' "$ds" "$rows"
    passed=$((passed + 1))
  fi
done

rm -f /tmp/crfs_dash_query.sql
echo
echo "$passed ok, $skipped skipped, $fail failed"
[ "$fail" -eq 0 ] || exit 1
