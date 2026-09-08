#!/usr/bin/env bash
# Give the app's service principal read access to the online feature tables.
#
# Why this is needed even though the app declares a `postgres` resource: that
# resource creates the SP's Postgres *role*, but the schema and its synced tables
# are owned by whoever published them. A role that can connect still cannot
# select, and the app reports `permission denied for schema <schema>`.
#
# ALTER DEFAULT PRIVILEGES is the part people miss. A plain
# GRANT SELECT ON ALL TABLES covers only the tables that exist right now, so the
# next publish_table produces a table the app cannot read. Re-running this script
# after a publish also works; the default privileges make that unnecessary.
#
#   ./scripts/grant_app_postgres.sh <profile> [app-name]
set -uo pipefail
PROFILE="${1:-fe-vm-lakebase-praneeth}"
APP="${2:-${APP:-crfs-watch-next}}"
CATALOG="${CATALOG:-serverless_lakebase_praneeth_catalog}"
SCHEMA="${SCHEMA:-crunchyroll_demo}"
DB=$(command -v databricks || echo /opt/homebrew/bin/databricks)
HERE="$(cd "$(dirname "$0")/.." && pwd)"

SP=$("$DB" apps get "$APP" --profile "$PROFILE" -o json 2>/dev/null \
     | python3 -c 'import json,sys; print(json.load(sys.stdin).get("service_principal_client_id",""))')
if [ -z "$SP" ]; then
  echo "app '$APP' not found, or it has no service principal yet."
  echo "Deploy the app first:  databricks bundle deploy -t dev"
  exit 1
fi
echo "app:                $APP"
echo "service principal:  $SP"

PROFILE="$PROFILE" CATALOG="$CATALOG" SCHEMA="$SCHEMA" SP="$SP" \
python3 - "$HERE" <<'PYEOF'
import os, sys
sys.path.insert(0, sys.argv[1])
from databricks.sdk import WorkspaceClient
from src.crfs.config import Config, DEFAULTS
from src.crfs import online

profile, catalog, schema, sp = (os.environ["PROFILE"], os.environ["CATALOG"],
                                os.environ["SCHEMA"], os.environ["SP"])
w = WorkspaceClient(profile=profile)
d = dict(DEFAULTS); d.update(catalog=catalog, schema=schema)
cfg = Config(extras={}, **d)
store = online.from_config(w, cfg)

owner = w.current_user.me().user_name
roles, _ = store.query("SELECT 1 FROM pg_roles WHERE rolname = %s", (sp,))
if not roles:
    print(f"  the SP has no Postgres role yet. The app's `postgres` resource creates it")
    print(f"  on first start -- run `databricks bundle run crfs_watch_next` and retry.")
    store.close(); raise SystemExit(1)

statements = [
    (f'GRANT USAGE ON SCHEMA "{schema}" TO "{sp}"', "usage on schema"),
    (f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{sp}"', "select on existing tables"),
    (f'ALTER DEFAULT PRIVILEGES FOR ROLE "{owner}" IN SCHEMA "{schema}" '
     f'GRANT SELECT ON TABLES TO "{sp}"', "select on tables published later"),
]
for sql, label in statements:
    try:
        store.query(sql)
        print(f"  granted {label}")
    except Exception as e:
        print(f"  FAILED {label}: {str(e)[:160]}")

granted, _ = store.query(
    """SELECT table_name FROM information_schema.table_privileges
       WHERE grantee = %s AND table_schema = %s AND privilege_type = 'SELECT'
       ORDER BY table_name""", (sp, schema))
print(f"  SP can now SELECT: {[r[0] for r in granted] or 'nothing yet'}")
store.close()
PYEOF
