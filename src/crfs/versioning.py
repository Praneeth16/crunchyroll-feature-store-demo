"""Feature-definition versioning: what a model pinned, and whether it still holds.

The question this answers is Crunchyroll's: *how do we change a feature definition
for training without breaking what is already serving?*

The mechanism is already in the platform and this module makes it legible. When
`fe.log_model` logs a model it writes a **feature spec** into the model artifact: the
tables, the lookup keys, the feature columns and the on-demand functions that model
was trained against. A serving endpoint reads that spec, not today's definitions. So a
deployed model is pinned to the definitions it trained on, and changing a definition
cannot silently change what an endpoint returns.

What the platform does *not* give you is an answer to "which deployed models does this
change affect, and has anything drifted since?". That is what the three functions here
are for:

  feature_spec_of(model_uri)        what this model version pinned
  definition_fingerprint(spark, s)  what those same objects look like right now
  drift_report(spark, model_uri)    the difference, per table and per function

A fingerprint is deliberately computed from the *schema and the function body*, not
from a source file: the question is whether the object a model resolves at serving
time still means what it meant at training time, and an edit to features.py that has
not been re-run changes no object.

Driver-side only, like the rest of src/crfs/ -- nothing here is imported by a served
model.
"""
import hashlib
import json
import os
import re

SPEC_FILENAMES = ("feature_spec.yaml", "feature_spec.yml")


# --------------------------------------------------------------- reading the spec
def _find_spec_file(root: str):
    for dirpath, _dirnames, filenames in os.walk(root):
        for fn in filenames:
            if fn in SPEC_FILENAMES:
                return os.path.join(dirpath, fn)
    return None


def feature_spec_of(model_uri: str) -> dict:
    """The feature spec a model version carries, as a dict.

    Downloads only the artifact tree, which for these models is a few KB of YAML plus
    the pickles. Raises if the model has no spec -- a model logged with
    `mlflow.pyfunc.log_model` instead of `fe.log_model` has none, and that difference
    is exactly what a caller needs to hear about rather than see as an empty dict.
    """
    import mlflow
    import yaml

    local = mlflow.artifacts.download_artifacts(artifact_uri=model_uri)
    path = _find_spec_file(local)
    if path is None:
        raise ValueError(
            f"{model_uri} carries no feature spec. It was not logged with "
            "FeatureEngineeringClient.log_model, so a serving endpoint cannot look "
            "features up for it and there is nothing pinned to compare against.")
    with open(path) as fh:
        return yaml.safe_load(fh) or {}


def _walk(node, key: str, out: list):
    """Collect every value stored under `key`, at any depth.

    The spec's nesting has changed shape across versions of the client; reading it by
    key rather than by path means a layout change does not silently return nothing.
    """
    if isinstance(node, dict):
        for k, v in node.items():
            if k == key and isinstance(v, str):
                out.append(v)
            else:
                _walk(v, key, out)
    elif isinstance(node, list):
        for v in node:
            _walk(v, key, out)
    return out


def spec_tables(spec: dict) -> list:
    """Three-level names of every feature table or feature view the spec looks up."""
    names = set(_walk(spec, "table_name", [])) | set(_walk(spec, "feature_table", []))
    return sorted(n for n in names if n.count(".") == 2)


def spec_functions(spec: dict) -> list:
    """Three-level names of every on-demand UDF the spec evaluates at request time."""
    names = set(_walk(spec, "udf_name", [])) | set(_walk(spec, "function_name", []))
    return sorted(n for n in names if n.count(".") == 2)


def spec_features(spec: dict) -> list:
    names = set(_walk(spec, "feature_name", [])) | set(_walk(spec, "output_name", []))
    return sorted(names)


def render_spec(spec: dict) -> str:
    tables, funcs, feats = spec_tables(spec), spec_functions(spec), spec_features(spec)
    lines = [f"  tables    ({len(tables)}):"] + [f"    {t}" for t in tables]
    lines += [f"  functions ({len(funcs)}):"] + [f"    {f}" for f in funcs]
    lines += [f"  features  ({len(feats)}): " + ", ".join(feats[:12])
              + (" ..." if len(feats) > 12 else "")]
    return "\n".join(lines)


# ------------------------------------------------------------------ fingerprinting
def _table_fingerprint(spark, name: str) -> str:
    """Schema of a table, as a stable string. Column order does not matter; names and
    types do, because those are what a lookup resolves."""
    cols = sorted((f.name, f.dataType.simpleString()) for f in spark.table(name).schema.fields)
    return json.dumps(cols, separators=(",", ":"))


# DESCRIBE FUNCTION EXTENDED prints provenance that changes without the definition
# changing -- owner, create time, the catalog's own comment formatting. Only the body
# and the signature decide what the function computes.
_FN_KEEP = re.compile(r"^(Body|Input|Returns|Type|Deterministic)", re.IGNORECASE)


def _function_fingerprint(spark, name: str) -> str:
    rows = spark.sql(f"DESCRIBE FUNCTION EXTENDED {name}").collect()
    kept = [r[0].strip() for r in rows if r[0] and _FN_KEEP.match(r[0].strip())]
    return json.dumps(kept, separators=(",", ":"))


def table_fingerprint_hash(fp: dict) -> str:
    return _hash({"tables": fp.get("tables", {})})


def function_fingerprint_hash(fp: dict) -> str:
    return _hash({"functions": fp.get("functions", {})})


def definition_fingerprint(spark, spec: dict) -> dict:
    """What the objects this spec depends on look like right now.

    Missing objects are recorded as `None` rather than raising: a dropped table is a
    finding, not an error, and the caller wants the whole picture in one pass.
    """
    out = {"tables": {}, "functions": {}}
    for t in spec_tables(spec):
        try:
            out["tables"][t] = _table_fingerprint(spark, t)
        except Exception:
            out["tables"][t] = None
    for f in spec_functions(spec):
        try:
            out["functions"][f] = _function_fingerprint(spark, f)
        except Exception:
            out["functions"][f] = None
    return out


def _hash(obj) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:12]


def fingerprint_hash(fp: dict) -> str:
    """Twelve hex characters, short enough to be a model tag and a commit message.

    **What this hash does and does not cover.** It is computed from table *schemas* and
    function *bodies*. A function body change is therefore detected. A table that is
    republished with the same column names and types but different maths is **not** --
    the schema is identical, so the hash is identical, and `drift_report` will say the
    definitions are unchanged. That is the exact case `docs/feature_versioning.md` tells
    you to avoid by versioning a changed definition under a new name, and the reason the
    advice is a rule rather than a preference: the tooling cannot catch it for you.

    Detecting it would need provenance the platform does not attach to a table -- which
    pipeline wrote it, from which source revision. The honest position is to say so here
    rather than to let a matching hash imply more than it knows.
    """
    return _hash(fp)


def spec_hash(spec: dict) -> str:
    """Hash of the *shape* a model pinned: which tables, functions and features.

    Separate from `fingerprint_hash`, which covers what those objects contain. A model
    whose spec_hash matches but whose fingerprint_hash does not is the interesting
    case: the same lookups, against a definition that has since been redefined.
    """
    shape = {"tables": spec_tables(spec), "functions": spec_functions(spec),
             "features": spec_features(spec)}
    return hashlib.sha256(json.dumps(shape, sort_keys=True).encode()).hexdigest()[:12]


# ------------------------------------------------------------------------- drift
TAG_FINGERPRINT = "feature_definition_fingerprint"
TAG_SPEC = "feature_spec_hash"


def training_tags(spark, spec: dict) -> dict:
    """The two tags to set on a model version at training time.

    Without them there is nothing to compare against later: the spec says which
    objects a model uses, but not what they looked like when it was trained.
    """
    fp = definition_fingerprint(spark, spec)
    return {TAG_SPEC: spec_hash(spec), TAG_FINGERPRINT: fingerprint_hash(fp)}


def drift_report(spark, model_uri: str, recorded_fingerprint: str = None) -> dict:
    """Has anything this model resolves at serving time changed since training?

    `recorded_fingerprint` is the `feature_definition_fingerprint` tag written when the
    model was trained. Without it this still reports missing objects, which is the
    failure that takes an endpoint down rather than merely making it stale.
    """
    spec = feature_spec_of(model_uri)
    fp = definition_fingerprint(spark, spec)
    now = fingerprint_hash(fp)

    missing_tables = [t for t, v in fp["tables"].items() if v is None]
    missing_funcs = [f for f, v in fp["functions"].items() if v is None]

    findings = []
    for t in missing_tables:
        findings.append(f"BROKEN   table {t} no longer exists or is unreadable -- "
                        "automatic feature lookup for this model will fail")
    for f in missing_funcs:
        findings.append(f"BROKEN   function {f} no longer exists -- request-time "
                        "features for this model will fail")
    # A changed TABLE and a changed FUNCTION have opposite consequences, so they cannot
    # share one message. The endpoint resolves table lookups from the spec inside the
    # model version, but it resolves UC functions BY NAME per request -- notebook 31
    # measures exactly that. Reporting both as "behaviour has not changed" would hand out
    # a false all-clear during live drift.
    changed = bool(recorded_fingerprint) and recorded_fingerprint != now
    if changed and not (missing_tables or missing_funcs):
        if spec_functions(spec):
            findings.append(
                f"CHANGED  definitions differ from training ({recorded_fingerprint} -> "
                f"{now}), and this model pins {len(spec_functions(spec))} on-demand "
                "function(s). Measured on this workspace (verification_log V83), a live "
                "endpoint did NOT pick up a redefined function within five minutes -- so it "
                "is resolved at deploy or cached, and current traffic is probably still "
                "seeing the old definition. Treat that as observed behaviour, not a "
                "guarantee: the next deploy or container replacement will pick the change "
                "up, and fe.score_batch uses the new definition immediately.")
        else:
            findings.append(
                f"CHANGED  definitions differ from training ({recorded_fingerprint} -> "
                f"{now}). This model pins no functions, so the endpoint still serves its "
                "pinned spec and behaviour has not changed; a retrain would learn "
                "something different.")

    status = "ok"
    if missing_tables or missing_funcs:
        status = "broken"
    elif changed:
        status = "changed"
    elif not recorded_fingerprint:
        # No baseline means no comparison was possible. Reporting that as ok made the
        # fleet view mark every untagged model healthy, which is the opposite of what an
        # absent baseline means.
        status = "unverifiable"
        findings.append(
            "UNVERIFIABLE  no feature_definition_fingerprint tag, so nothing can be "
            "compared against training. Present objects are all that was checked. Tag "
            "the version (versioning.training_tags) to get a baseline from now on.")

    return {
        "model_uri": model_uri,
        "spec_hash": spec_hash(spec),
        "fingerprint_now": now,
        "table_fingerprint": table_fingerprint_hash(fp),
        "function_fingerprint": function_fingerprint_hash(fp),
        "fingerprint_at_training": recorded_fingerprint,
        "tables": spec_tables(spec),
        "functions": spec_functions(spec),
        "missing_tables": missing_tables,
        "missing_functions": missing_funcs,
        "status": status,
        "ok": status == "ok",
        "findings": findings,
    }


def render_drift(report: dict) -> str:
    head = (f"{report['model_uri']}\n"
            f"  spec        {report['spec_hash']}\n"
            f"  definitions {report['fingerprint_at_training'] or '(not tagged)'}"
            f" -> {report['fingerprint_now']}")
    head += f"\n  status      {report.get('status', 'unknown')}"
    if report["ok"]:
        return head + "\n  OK       every table and function the spec pins is present and unchanged"
    return head + "\n  " + "\n  ".join(report["findings"])
