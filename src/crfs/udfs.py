"""On-demand (request-time) features as Unity Catalog Python UDFs.

A feature belongs here only if it genuinely cannot be precomputed: it depends on
a value that arrives in the request, or it is a viewer x title cross whose
precomputation is a cartesian product. All four below qualify.

Every UDF handles None explicitly. FeatureFunction inputs arrive as NaN during
online serving and None in batch scoring when a lookup key is missing, and an
unguarded UDF raises *inside model serving* -- which surfaces as a 500 on the
endpoint, not as a training error.

Epochs are BIGINT, not TIMESTAMP, so the app, the endpoint and the training set
cannot disagree about a timezone.

Integer parameters are declared BIGINT throughout, because FeatureFunction checks
the argument column's type against the parameter's type exactly and pandas int64
columns become Delta bigint.

Every body is written WITHOUT indented blocks -- no if/for suites, only
single-line statements and conditional expressions. Verified 2026-09-07:
leading whitespace does not survive the round trip into the function body
(DESCRIBE FUNCTION EXTENDED shows an indented `return` stored at column 0),
which turns any indented suite into an IndentationError raised inside the
executor as UDF_USER_CODE_ERROR. Keeping bodies flat sidesteps it entirely.
"""
import textwrap

from .config import GENRES

AFFINITY_ARGS = [f"aff_{g} DOUBLE" for g in GENRES]
# BIGINT, not INT. FeatureFunction type-matches the argument column against the
# parameter exactly, and a pandas int64 column lands in Delta as bigint:
#   ValueError: FeatureFunction argument column 'genre_action' ... has type 'bigint'
#   and parameter 'g_action' has type 'int'.
# The same applies to hour_of_day arriving in the request payload.
GENRE_ARGS = [f"g_{g} BIGINT" for g in GENRES]

_MATCH_BODY = """
  a = [{aff_list}]
  g = [{g_list}]
  a = [0.0 if x is None else float(x) for x in a]
  g = [0 if x is None else int(x) for x in g]
  return float(sum(x * y for x, y in zip(a, g)))
""".format(aff_list=", ".join(f"aff_{g}" for g in GENRES),
           g_list=", ".join(f"g_{g}" for g in GENRES))

_CROSS_BODY = """
  a = [{aff_list}]
  g = [{g_list}]
  a = [0.0 if x is None else float(x) for x in a]
  g = [0 if x is None else int(x) for x in g]
  match = float(sum(x * y for x, y in zip(a, g)))
  p = 0.0 if popularity_30d is None else float(popularity_30d)
  return float(match * (0.5 + p))
""".format(aff_list=", ".join(f"aff_{g}" for g in GENRES),
           g_list=", ".join(f"g_{g}" for g in GENRES))


def _fn(name, args, returns, comment, body):
    # Double any apostrophe: the COMMENT is a single-quoted SQL string, and an
    # unescaped one in "viewer's" is a parse error, not a runtime error.
    comment = comment.replace("'", "''")
    # UDF bodies must sit at column 0 -- a leading indent is an IndentationError
    # raised inside the executor, surfacing as UDF_USER_CODE_ERROR at query time.
    body = textwrap.dedent(body).strip("\n")
    return f"""CREATE OR REPLACE FUNCTION {{fq}}.{name}(
  {args}
)
RETURNS {returns}
LANGUAGE PYTHON
COMMENT '{comment}'
AS $$
{body}
$$"""


# name -> (output feature name, DDL template with a {fq} placeholder)
UDFS = {
    "cr_genre_affinity_match": (
        "affinity_match",
        _fn("cr_genre_affinity_match",
            ", ".join(AFFINITY_ARGS + GENRE_ARGS),
            "DOUBLE",
            "Request-time dot product of the viewer affinity vector with the title genre one-hot. "
            "Cannot be precomputed: one row per viewer x title.",
            _MATCH_BODY),
    ),
    "cr_affinity_popularity_cross": (
        "affinity_x_popularity",
        _fn("cr_affinity_popularity_cross",
            ", ".join(AFFINITY_ARGS + GENRE_ARGS + ["popularity_30d DOUBLE"]),
            "DOUBLE",
            "Affinity match scaled by title popularity - lets a strong match on an obscure title "
            "rank differently from the same match on a hit.",
            _CROSS_BODY),
    ),
    "cr_hour_affinity_delta": (
        "hour_affinity",
        _fn("cr_hour_affinity_delta",
            "hour_of_day BIGINT, typical_watch_hour DOUBLE, hour_concentration DOUBLE",
            "DOUBLE",
            "How close the request hour is to when this viewer usually watches, weighted by how "
            "concentrated their habit is. hour_of_day only exists in the request.",
            """
h = 0.0 if hour_of_day is None else float(hour_of_day)
t = -1.0 if typical_watch_hour is None else float(typical_watch_hour)
c = 0.0 if hour_concentration is None else float(hour_concentration)
raw = abs(h - t) % 24.0
d = min(raw, 24.0 - raw)
return 0.0 if t < 0.0 else float((1.0 - d / 12.0) * (0.5 + 0.5 * c))
"""),
    ),
    "cr_session_decay": (
        "session_decay",
        _fn("cr_session_decay",
            "request_epoch_s BIGINT, last_event_epoch_s BIGINT",
            "DOUBLE",
            "Exponential decay since the viewer's last watch event, half-life about 21 minutes. "
            "Depends on the wall clock at request time, so it can only be computed on demand.",
            """
import math
r = 0 if request_epoch_s is None else int(request_epoch_s)
p = 0 if last_event_epoch_s is None else int(last_event_epoch_s)
mins = max(0.0, (r - p) / 60.0)
return 0.0 if (r == 0 or p == 0 or mins > 1440.0) else float(math.exp(-mins / 30.0))
"""),
    ),
}

# The features these UDFs produce, in the order they join the model matrix.
ONDEMAND_OUTPUTS = ["affinity_match", "affinity_x_popularity", "hour_affinity", "session_decay"]


def ddl(fq: str):
    """CREATE OR REPLACE statements, ready to spark.sql()."""
    return [tmpl.format(fq=fq) for _, tmpl in UDFS.values()]


def drop_ddl(fq: str):
    return [f"DROP FUNCTION IF EXISTS {fq}.{name}" for name in UDFS]


def feature_functions(fq: str):
    """FeatureFunction objects for create_training_set / create_feature_spec.

    input_bindings values are either request columns (hour_of_day,
    request_epoch_s) or columns produced by the FeatureLookups above them -- both
    are legal, and the endpoint evaluates the UDFs after the online lookups.
    """
    from databricks.feature_engineering import FeatureFunction

    affinity_bindings = {f"aff_{g}": f"genre_affinity_{g}" for g in GENRES}
    genre_bindings = {f"g_{g}": f"genre_{g}" for g in GENRES}

    return [
        FeatureFunction(
            udf_name=f"{fq}.cr_genre_affinity_match",
            input_bindings={**affinity_bindings, **genre_bindings},
            output_name="affinity_match",
        ),
        FeatureFunction(
            udf_name=f"{fq}.cr_affinity_popularity_cross",
            input_bindings={**affinity_bindings, **genre_bindings,
                            "popularity_30d": "popularity_30d"},
            output_name="affinity_x_popularity",
        ),
        FeatureFunction(
            udf_name=f"{fq}.cr_hour_affinity_delta",
            input_bindings={"hour_of_day": "hour_of_day",
                            "typical_watch_hour": "typical_watch_hour",
                            "hour_concentration": "hour_concentration"},
            output_name="hour_affinity",
        ),
        FeatureFunction(
            udf_name=f"{fq}.cr_session_decay",
            input_bindings={"request_epoch_s": "request_epoch_s",
                            "last_event_epoch_s": "last_event_epoch_s"},
            output_name="session_decay",
        ),
    ]
