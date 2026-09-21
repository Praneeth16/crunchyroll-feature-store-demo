"""Feature View definitions -- the declarative authoring path.

`features.py` holds the GA path: pandas that computes feature *values*, which a
notebook then writes and publishes. This holds definitions of the same shape of
signal, expressed as `Feature` objects that the platform computes itself.

Both exist on purpose. The GA path can express anything Python can compute; this
path can express what the aggregation DSL covers, and gets backfill, refresh and the
online copy for free. `docs/feature_views.md` is the comparison.

Pure definitions. No client, no widgets, no printing -- same contract as features.py
and rails.py, so notebook 30 and anything after it share one source of truth.

Three DSL facts that shaped what is here, each verified by `29_preview_probe` on a
live workspace rather than taken from the docs:

  * 18 aggregation operators exist (Sum, Avg, Count, Min, Max, First, Last, FirstN,
    LastN, FirstDistinct, LastDistinct, ApproxCountDistinct, ApproxPercentile,
    PercentileApprox, StddevSamp, StddevPop, VarSamp, VarPop) -- not the three the
    published limitations list.
  * `SlidingWindow` is used throughout rather than `RollingWindow`: batch
    rolling-window features cannot be materialized, so a rolling definition trains
    and then cannot be served, which is the worst possible time to find out.
  * Nothing here derives from `played`. It is the label in notebook 30 and a column of
    the source table, and the docs require a label not to exist in a feature source.
"""
from datetime import timedelta

SOURCE_TABLE = "engagement_events"

# (name, operator, input column, window duration, slide) -- one row per feature, so
# the set is reviewable at a glance and the windows can be compared.
#
# The durations deliberately mirror the GA tables these features rhyme with:
# recent_behavior_current is a 24h table, viewer_features_current is 7d/30d.
SPECS = [
    ("fv_watch_seconds_24h",   "Sum",                 "watch_seconds", timedelta(days=1),  timedelta(hours=6)),
    ("fv_events_24h",          "Count",               "event_id",      timedelta(days=1),  timedelta(hours=6)),
    ("fv_titles_24h",          "ApproxCountDistinct", "title_id",      timedelta(days=1),  timedelta(hours=6)),
    ("fv_watch_seconds_7d",    "Sum",                 "watch_seconds", timedelta(days=7),  timedelta(days=1)),
    ("fv_events_7d",           "Count",               "event_id",      timedelta(days=7),  timedelta(days=1)),
    ("fv_avg_watch_seconds_7d", "Avg",                "watch_seconds", timedelta(days=7),  timedelta(days=1)),
    ("fv_watch_seconds_30d",   "Sum",                 "watch_seconds", timedelta(days=30), timedelta(days=1)),
]


# Title-grain features over the same source. A viewer-only feature set cannot answer
# "which of these titles did this viewer play" -- every candidate row of one impression
# shares the viewer, so the model has nothing to discriminate on and the first run of
# notebook 30 scored a holdout AUC of 0.4992, i.e. exactly random. Popularity and reach
# are the title-side signal, and both are point-in-time by virtue of the window.
#
# Nothing here uses `played`: it is the label, and the docs require a label not to be a
# column of a feature source.
TITLE_SPECS = [
    ("fvt_events_7d",         "Count",               "event_id",      timedelta(days=7),  timedelta(days=1)),
    ("fvt_watch_seconds_7d",  "Sum",                 "watch_seconds", timedelta(days=7),  timedelta(days=1)),
    ("fvt_viewers_7d",        "ApproxCountDistinct", "viewer_id",     timedelta(days=7),  timedelta(days=1)),
    ("fvt_events_30d",        "Count",               "event_id",      timedelta(days=30), timedelta(days=1)),
    ("fvt_watch_seconds_30d", "Sum",                 "watch_seconds", timedelta(days=30), timedelta(days=1)),
]


def viewer_features(catalog: str, schema: str) -> list:
    """The viewer-grain features, keyed by `viewer_id` on `event_ts`.

    Imports inside the function because `databricks-feature-engineering>=0.16.0` is
    installed by the notebook that calls this, not by the repo -- importing at module
    scope would break every other notebook that imports this package.
    """
    from databricks.feature_engineering import entities as E

    source = E.DeltaTableSource(
        catalog_name=catalog,
        schema_name=schema,
        table_name=SOURCE_TABLE,
    )
    out = []
    for name, op, col, duration, slide in SPECS:
        out.append(E.Feature(
            name=name,
            source=source,
            entity=["viewer_id"],
            timeseries_column="event_ts",
            function=E.AggregationFunction(
                getattr(E, op)(input=col),
                E.SlidingWindow(window_duration=duration, slide_duration=slide),
            ),
            description=f"{op}({col}) over {_human(duration)}, sliding every {_human(slide)}",
        ))
    return out


def _human(td: timedelta) -> str:
    hours = int(td.total_seconds() // 3600)
    if hours % 24 == 0:
        days = hours // 24
        return f"{days}d" if days != 1 else "1d"
    return f"{hours}h"


def describe(feature) -> str:
    """One line per feature, for a notebook to print. Reads the definition rather
    than a parallel description that could drift from it."""
    fn = getattr(feature, "function", None)
    op = type(getattr(fn, "operator", fn)).__name__ if fn is not None else "?"
    win = getattr(fn, "time_window", None)
    dur = getattr(win, "window_duration", None)
    entity = ",".join(getattr(feature, "entity", []) or [])
    return f"{op:20s} entity={entity:10s} window={_human(dur) if dur else '-'}"


def register_all(fe, features, catalog: str, schema: str, log=print) -> list:
    """Register each feature and return the REGISTERED objects.

    This matters more than it looks. A locally-built `Feature` carries no catalog or
    schema, and anything that needs its full name -- `create_training_set(features=...)`,
    `materialize_features` -- fails with

        ValueError: Feature does not have a catalog and schema.
        Provide catalog_name and schema_name, or call register_feature().

    `register_feature` returns a new object that does carry them, so the returned list is
    the one to use downstream. Passing the local definitions instead is the bug this
    function exists to prevent; it cost one 25-minute job run to find.

    Idempotent: an already-registered feature is fetched rather than re-registered, so
    re-running the notebook is safe. `get_feature`'s keyword name has moved between
    client versions, so the three plausible spellings are tried in turn rather than
    guessed.
    """
    out = []
    for f in features:
        full = f"{catalog}.{schema}.{f.name}"
        try:
            out.append(fe.register_feature(feature=f, catalog_name=catalog,
                                           schema_name=schema))
            log(f"registered {full}")
            continue
        except Exception as e:
            if not _already_exists(e):
                raise
        got = _get_feature(fe, full, catalog, schema, f.name)
        if got is None:
            raise RuntimeError(
                f"{full} already exists but could not be fetched back. Delete it with "
                f"fe.delete_feature and re-run, or inspect fe.list_features().")
        out.append(got)
        log(f"exists     {full}")
    return out


def _already_exists(e: Exception) -> bool:
    s = str(e).lower()
    return "already exists" in s or "already_exists" in s


def _get_feature(fe, full: str, catalog: str, schema: str, name: str):
    for kwargs in ({"name": full},
                   {"catalog_name": catalog, "schema_name": schema, "name": name},
                   {"full_name": full}):
        try:
            return fe.get_feature(**kwargs)
        except TypeError:
            continue          # wrong keyword for this client version
        except Exception:
            return None       # the call shape was right; the feature is not readable
    return None


def title_features(catalog: str, schema: str) -> list:
    """Title-grain features, keyed by `title_id` on `event_ts`.

    Same source and same windows as the viewer features -- the entity is what differs.
    One `create_training_set` call can mix both, because the label frame carries both
    keys, and that is the shape any ranking model needs: viewer signal x item signal.
    """
    from databricks.feature_engineering import entities as E

    source = E.DeltaTableSource(
        catalog_name=catalog,
        schema_name=schema,
        table_name=SOURCE_TABLE,
    )
    out = []
    for name, op, col, duration, slide in TITLE_SPECS:
        out.append(E.Feature(
            name=name,
            source=source,
            entity=["title_id"],
            timeseries_column="event_ts",
            function=E.AggregationFunction(
                getattr(E, op)(input=col),
                E.SlidingWindow(window_duration=duration, slide_duration=slide),
            ),
            description=f"{op}({col}) per title over {_human(duration)}, sliding every {_human(slide)}",
        ))
    return out


def all_features(catalog: str, schema: str) -> list:
    """Both grains, which is what notebook 30 trains on."""
    return viewer_features(catalog, schema) + title_features(catalog, schema)


def materializations_of(fe, full_name: str) -> list:
    """Every materialization of one feature, or [] if it has none.

    `list_materialized_features` is **per feature**, not a catalog-wide listing:
        TypeError: list_materialized_features() missing 1 required keyword-only
        argument: 'feature_name'
    Calling it without one looks like an empty result if the exception is swallowed,
    which is how a first version of this reported "nothing is materialized" about a
    schema with twelve materialized features.
    """
    try:
        return list(fe.list_materialized_features(feature_name=full_name) or [])
    except Exception:
        return []


def already_materialized(fe, features, catalog: str, schema: str) -> set:
    """Full names of features that already have a materialization."""
    out = set()
    for f in features:
        full = f"{catalog}.{schema}.{f.name}"
        if materializations_of(fe, full):
            out.add(full)
    return out


def _names_in_error(e: Exception, features, catalog: str, schema: str) -> set:
    msg = str(e)
    return {f"{catalog}.{schema}.{f.name}" for f in features
            if f"{catalog}.{schema}.{f.name}" in msg}


def materialize_new(fe, features, catalog: str, schema: str, log=print, **kwargs):
    """Materialize only the features that are not materialized yet.

    `materialize_features` is **not** idempotent: a second call for the same feature
    raises `ResourceAlreadyExists: Materialized features already exist for features
    '...'`, which failed a re-run of notebook 30 outright. Every other build step in this
    repo is safe to re-run (see `upsert_feature_table` in notebook 01), and this makes
    this one match.

    Two passes, because the listing and the error are independent sources of truth and
    either can be the one that knows: skip what the listing reports, then if the call
    still complains, subtract exactly the names it named and retry once with the rest.

    Returns (created, skipped) as lists of full names.
    """
    existing = already_materialized(fe, features, catalog, schema)
    todo = [f for f in features if f"{catalog}.{schema}.{f.name}" not in existing]
    skipped = sorted(existing)
    for name in skipped:
        log(f"already materialized: {name}")
    if not todo:
        log("nothing new to materialize")
        return [], skipped

    try:
        fe.materialize_features(features=todo, **kwargs)
        created = [f"{catalog}.{schema}.{f.name}" for f in todo]
    except Exception as e:
        if "AlreadyExists" not in type(e).__name__ and not _already_exists(e):
            raise
        named = _names_in_error(e, todo, catalog, schema)
        retry = [f for f in todo if f"{catalog}.{schema}.{f.name}" not in named]
        skipped += sorted(named)
        for name in sorted(named):
            log(f"already materialized (reported by the API): {name}")
        if not retry:
            return [], sorted(set(skipped))
        fe.materialize_features(features=retry, **kwargs)
        created = [f"{catalog}.{schema}.{f.name}" for f in retry]

    for name in created:
        log(f"materialized: {name}")
    return created, sorted(set(skipped))
