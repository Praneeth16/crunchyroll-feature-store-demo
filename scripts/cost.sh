#!/usr/bin/env bash
# What this demo is billing. List prices, so read them as an upper bound.
set -uo pipefail
PROFILE="${1:-$(grep -s '^CRFS_PROFILE=' "$(dirname "$0")/../.crfs.vars" | cut -d= -f2- || true)}"
[ -n "$PROFILE" ] || { echo "usage: $0 <PROFILE>   (databricks auth profiles lists yours)" >&2; exit 2; }
DAYS="${DAYS:-14}"
PROJECT="${PROJECT:-crunchyroll-online-store}"
BRANCH="${BRANCH:-production}"
ENDPOINT="${ENDPOINT:-primary}"
DB=$(command -v databricks || echo /opt/homebrew/bin/databricks)

UID_=$("$DB" postgres get-endpoint "projects/$PROJECT/branches/$BRANCH/endpoints/$ENDPOINT" \
  --profile "$PROFILE" -o json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["uid"])')
echo "lakebase endpoint uid: $UID_"

"$DB" experimental aitools tools query "
SELECT u.usage_date,
       u.sku_name,
       ROUND(SUM(u.usage_quantity), 2)                     AS dbu,
       ROUND(SUM(u.usage_quantity * p.pricing.default), 2) AS usd_list
FROM system.billing.usage u
JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
 AND u.usage_end_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
WHERE (u.usage_metadata.endpoint_id = '$UID_'
    OR u.usage_metadata.endpoint_name LIKE 'crunchyroll-%')
  AND u.usage_date >= current_date() - $DAYS
GROUP BY u.usage_date, u.sku_name
ORDER BY u.usage_date DESC, usd_list DESC
" --profile "$PROFILE"
