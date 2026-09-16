#!/usr/bin/env bash
# Render the dashboard template for this workspace.
#
# Lakeview dashboard files are uploaded verbatim by `bundle deploy` -- Databricks
# Asset Bundles do NOT interpolate ${var.x} inside them. Verified 2026-09-16:
#
#   Error: invalid dependency "${var.catalog}", no such node ""
#
# So the catalog and schema have to be substituted before the deploy, which is what
# this does. The template is the tracked file; the rendered copy is generated and
# gitignored, so nobody commits one workspace's catalog name into the other's demo.
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
TMPL="$HERE/dashboards/crfs_feature_ops.lvdash.json.tmpl"
OUT_DIR="$HERE/dashboards/generated"
OUT="$OUT_DIR/crfs_feature_ops.lvdash.json"

CATALOG="${CATALOG:-}"
SCHEMA="${SCHEMA:-}"
if [ -z "$CATALOG" ] && [ -f "$HERE/.crfs.vars" ]; then
  # `|| true` is required, not defensive noise. Under `set -e` with `pipefail` a
  # .crfs.vars that exists but carries no catalog= line makes this substitution exit
  # non-zero and kills the script -- so the defaults two lines below were unreachable,
  # and because `make validate`/`make deploy` depend on `render`, the deploy failed
  # instead of degrading to the default. Verified by running the pattern in isolation.
  CATALOG=$(grep '^catalog=' "$HERE/.crfs.vars" | cut -d= -f2- || true)
  SCHEMA=$(grep '^schema=' "$HERE/.crfs.vars" | cut -d= -f2- || true)
fi
CATALOG="${CATALOG:-serverless_lakebase_praneeth_catalog}"
SCHEMA="${SCHEMA:-crunchyroll_demo}"

mkdir -p "$OUT_DIR"
python3 - "$TMPL" "$OUT" "$CATALOG" "$SCHEMA" <<'PYEOF'
import json, sys
tmpl, out, catalog, schema = sys.argv[1:5]
text = open(tmpl).read().replace("__CATALOG__", catalog).replace("__SCHEMA__", schema)
# Parse before writing: a dashboard that is not valid JSON fails at deploy time with
# a much less helpful message than this one.
json.loads(text)
open(out, "w").write(text)
print(f"rendered dashboard for {catalog}.{schema} -> {out}")
PYEOF
