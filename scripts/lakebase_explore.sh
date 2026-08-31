#!/usr/bin/env bash
# Lakebase exploration for the Crunchyroll Feature Store demo.
#
# The Online Feature Store (created by fe.create_online_store in notebook 01)
# is backed by a Lakebase Autoscaling instance. This script finds it and
# opens psql so you can read online feature tables straight from Postgres —
# the same values Model Serving reads at request time.
#
# Usage:
#   ./scripts/lakebase_explore.sh [profile]
# Requires: databricks CLI v1.x authenticated, psql client (brew install postgresql@16)

set -euo pipefail
PROFILE="${1:-fe-vm-lakebase-praneeth}"

echo "== Lakebase projects in workspace =="
databricks postgres list-projects -p "$PROFILE" -o json \
  | python3 -c "import json,sys; [print(p['project_id'], '|', p['status'].get('display_name',''), '|', p['status'].get('pg_version','')) for p in json.load(sys.stdin)]"

echo
PROJECT="${PROJECT:-crunchyroll-online-store}"
DB="${DB:-serverless_lakebase_praneeth_catalog}"

BRANCH="${BRANCH:-production}"
ENDPOINT="${ENDPOINT:-primary}"

HOST=$(databricks postgres list-endpoints "projects/$PROJECT/branches/$BRANCH" \
  -p "$PROFILE" -o json | python3 -c "import json,sys; print(json.load(sys.stdin)[0]['status']['hosts']['host'])")
TOKEN=$(databricks postgres generate-database-credential \
  "projects/$PROJECT/branches/$BRANCH/endpoints/$ENDPOINT" \
  -p "$PROFILE" -o json | python3 -c "import json,sys; print(json.load(sys.stdin)['token'])")
EMAIL=$(databricks current-user me -p "$PROFILE" -o json \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['userName'])")

echo
echo "== connecting to $HOST / $DB as $EMAIL =="
echo "OAuth token valid 1 hour. Example queries once inside psql:"
echo "  \\dt crunchyroll_demo.*"
echo "  SELECT viewer_id, minutes_watched_24h, last_primary_genre"
echo "    FROM crunchyroll_demo.online_recent_behavior WHERE viewer_id = 'v0001';"
echo
PGPASSWORD="$TOKEN" psql "host=$HOST port=5432 dbname=$DB user=$EMAIL sslmode=require"
