"""Vertical ranking: the rail catalog, its features, and the homepage log.

Horizontal ranking orders titles *inside* a rail. Vertical ranking orders the
rails themselves -- which row goes first on the Crunchyroll homepage for this
viewer, on this device, at this hour. It is the same feature layer either way,
and that is the point of this file: `viewer_features_current`,
`recent_behavior_current`, `session_features_current` and `title_features` are
reused verbatim by both models. Only two feature tables are new --
`rail_features` (per rail) and `viewer_rail_features_ts` (per viewer x rail, a
time series table that is point-in-time offline and latest-per-key online, so
there is no separate `_current` mirror) -- plus the request-time UDFs in udfs.py.

Three things live here, in the order the pipeline needs them:

  1. the rail catalog and which titles each rail can draw from,
  2. a synthetic homepage impression log with position bias baked in, which is
     what a real ranker has to learn from and correct for,
  3. the feature builders for the two new tables.

Pure pandas / pyspark. No widgets, no clients, no printing -- same contract as
features.py.
"""
import hashlib

import numpy as np
import pandas as pd

from .config import GENRES

# --------------------------------------------------------------- rail catalog
# (rail_id, rail_name, rail_type, rail_genre, is_personalized, editorial_rank)
#
# editorial_rank is the *logging policy*: the order the current rule-based
# homepage renders rails in. The learned ranker has to beat it, and the training
# labels are only ever observed at the positions this policy produced -- which is
# where position bias comes from.
RAIL_SPECS = [
    ("r_continue",      "Continue Watching",          "continue_watching", None,            True,  1),
    ("r_new_eps",       "New Episodes for You",       "new_release",       None,            True,  2),
    ("r_because",       "Because You Watched",        "personalized",      None,            True,  3),
    ("r_simulcast",     "This Season's Simulcasts",   "new_release",       None,            False, 4),
    ("r_top10",         "Top 10 in Your Country",     "trending",          None,            False, 5),
    ("r_watchlist",     "Your Watchlist",             "watchlist",         None,            True,  6),
    ("r_trending",      "Trending Now",               "trending",          None,            False, 7),
    ("r_action",        "Action & Adventure",         "genre",             "action",        False, 8),
    ("r_fantasy",       "Fantasy Worlds",             "genre",             "fantasy",       False, 9),
    ("r_scifi",         "Sci-Fi & Mecha",             "genre",             "sci_fi",        False, 10),
    ("r_romance",       "Romance",                    "genre",             "romance",       False, 11),
    ("r_slice",         "Slice of Life",              "genre",             "slice_of_life", False, 12),
    ("r_drama",         "Drama",                      "genre",             "drama",         False, 13),
    ("r_sports",        "Sports",                     "genre",             "sports",        False, 14),
    ("r_movies",        "Anime Movies",               "editorial",         None,            False, 15),
    ("r_classics",      "Classics & Legends",         "editorial",         None,            False, 16),
]

RAIL_TYPES = ["continue_watching", "new_release", "personalized", "trending",
              "watchlist", "genre", "editorial"]

# Rails whose eligibility depends on viewer state, not on the catalog. A request
# that carries no in-progress titles must not be offered Continue Watching, so
# the eligible set genuinely varies per request -- which is what the serving
# contract has to handle.
STATE_DEPENDENT = {"r_continue": "has_inprogress",
                   "r_watchlist": "has_watchlist",
                   "r_because": "has_history",
                   "r_new_eps": "has_simulcast_history"}

# Columns on rail_features (PK rail_id). Catalog-side and audience-side signals
# about the rail itself, shared by every viewer.
RAIL_FEATURE_COLS = [
    "rail_ctr_30d", "rail_impressions_30d", "rail_clicks_30d",
    "rail_avg_watch_minutes_30d", "rail_titles_available",
    "rail_avg_popularity", "rail_avg_rating", "rail_content_age_days",
    "rail_simulcast_share", "rail_is_personalized", "rail_type_idx",
    "rail_genre_idx", "rail_editorial_rank",
]

# Columns on viewer_rail_features_ts (entity keys viewer_id, rail_id; ts is the
# time series key).
# This is the personalization signal that vertical ranking lives on: how this
# viewer has treated this rail historically.
VIEWER_RAIL_FEATURE_COLS = [
    "vr_impressions_30d", "vr_clicks_30d", "vr_ctr_30d", "vr_clicks_7d",
    "vr_watch_minutes_30d", "vr_avg_position_30d", "vr_last_click_epoch_s",
]

# Context the request carries. hour_of_day and device are not stored anywhere --
# they only exist at request time, which is why the on-demand UDFs need them.
RAIL_REQUEST_KEYS = ["viewer_id", "rail_id", "device", "locale", "hour_of_day",
                     "day_of_week", "request_epoch_s"]

DEVICES = ["tv", "mobile", "web", "console"]


def _genre_idx(genre) -> int:
    """Rail genre as an integer the model can split on. -1 means 'not a genre rail'."""
    if genre is None or (isinstance(genre, float) and np.isnan(genre)) or genre == "":
        return -1
    return GENRES.index(genre) if genre in GENRES else -1


def _stable_unit(*parts) -> float:
    """Deterministic [0, 1) draw from a key -- so a rerun of the generator with
    the same seed produces the same viewer taste, and shard order cannot change it."""
    h = hashlib.sha256("|".join(str(p) for p in parts).encode()).hexdigest()
    return int(h[:12], 16) / float(16 ** 12)


def rails_frame() -> pd.DataFrame:
    """The rail catalog, one row per rail."""
    df = pd.DataFrame(RAIL_SPECS, columns=[
        "rail_id", "rail_name", "rail_type", "rail_genre",
        "is_personalized", "editorial_rank"])
    df["rail_genre"] = df["rail_genre"].fillna("")
    df["max_titles"] = 20
    return df


# ------------------------------------------------------------ rail x title map
def rail_title_map(titles_pdf: pd.DataFrame, rails_pdf: pd.DataFrame,
                   as_of: pd.Timestamp, per_rail: int = 20) -> pd.DataFrame:
    """Which titles each *catalog-driven* rail can draw from.

    Personalized rails (Continue Watching, Because You Watched, Watchlist, New
    Episodes) are resolved per viewer at request time, so they get no static
    membership here -- the map is only used to derive each rail's content
    features and to render the demo homepage.
    """
    t = titles_pdf.copy()
    t["release_ts"] = pd.to_datetime(t["release_year"].astype(int).astype(str) + "-07-01")
    t["age_days"] = (as_of - t["release_ts"]).dt.days
    rows = []

    def take(rail_id, frame, order_col="intrinsic_popularity", ascending=False):
        sel = frame.sort_values(order_col, ascending=ascending).head(per_rail)
        for rank, (_, r) in enumerate(sel.iterrows(), start=1):
            rows.append({"rail_id": rail_id, "title_id": r["title_id"], "rank_in_rail": rank})

    for _, rail in rails_pdf.iterrows():
        rid, rtype, rgenre = rail["rail_id"], rail["rail_type"], rail["rail_genre"]
        if rtype == "genre":
            pool = t[(t["primary_genre"] == rgenre) | (t["secondary_genre"] == rgenre)]
            take(rid, pool)
        elif rid == "r_simulcast":
            take(rid, t[t["is_simulcast"] & (t["release_year"] >= as_of.year - 1)])
        elif rid == "r_trending" or rid == "r_top10":
            take(rid, t)
        elif rid == "r_movies":
            take(rid, t[t["episode_count"] <= 2])
        elif rid == "r_classics":
            take(rid, t[t["release_year"] <= 2012])
        # personalized rails: resolved per viewer at request time.

    out = pd.DataFrame(rows, columns=["rail_id", "title_id", "rank_in_rail"])
    return out.astype({"rank_in_rail": "int32"})


# ------------------------------------------------------------- viewer's taste
def observed_genre_affinity(events_pdf: pd.DataFrame, titles_pdf: pd.DataFrame,
                            through: pd.Timestamp) -> pd.DataFrame:
    """Each viewer's genre mix, measured over an *early* slice of history only.

    Used to generate the homepage labels. Deliberately restricted to events
    before `through` so the simulated preference is not a function of the same
    days the model is scored on -- otherwise the labels would leak the answer
    into every later day and the reported AUC would be fiction.
    """
    ev = events_pdf[pd.to_datetime(events_pdf["event_ts"]) <= through].copy()
    ev = ev[ev["watch_seconds"].fillna(0) > 0]
    genre = titles_pdf.set_index("title_id")["primary_genre"]
    ev["primary_genre"] = ev["title_id"].map(genre)
    mins = (ev.assign(m=ev["watch_seconds"] / 60.0)
            .pivot_table(index="viewer_id", columns="primary_genre", values="m",
                         aggfunc="sum", fill_value=0.0))
    for g in GENRES:
        if g not in mins.columns:
            mins[g] = 0.0
    mins = mins[GENRES]
    total = mins.sum(axis=1).replace(0, np.nan)
    share = mins.div(total, axis=0).fillna(1.0 / len(GENRES))
    share.columns = [f"taste_{g}" for g in GENRES]
    return share.reset_index()


# -------------------------------------------------------- homepage impressions
# Probability a rail at position p is actually looked at. Geometric decay is the
# standard shape for a vertically scrolled feed, and it is the whole reason the
# training labels need a propensity correction.
def view_probability(position: np.ndarray) -> np.ndarray:
    return np.clip(0.97 * np.exp(-0.16 * (position - 1)), 0.05, 1.0)


def generate_rail_impressions(viewers_pdf: pd.DataFrame, titles_pdf: pd.DataFrame,
                              events_pdf: pd.DataFrame, rails_pdf: pd.DataFrame,
                              start: pd.Timestamp, end: pd.Timestamp,
                              seed: int = 4242, taste_window_days: int = 30,
                              sessions_per_active_day: float = 1.6) -> pd.DataFrame:
    """A homepage render log: one row per rail shown in one homepage session.

    Every row carries the position the rail was rendered at, whether it was
    looked at, and whether the viewer engaged with it. Engagement is driven by a
    latent viewer x rail utility built from taste, rail type and device -- signal
    the model can only recover through the feature tables, never from the row
    itself.

    Fully vectorised: one frame of (session x eligible rail), then numpy over it.
    A per-session Python loop on 90 days of history is minutes, not seconds.
    """
    rng = np.random.default_rng(seed)
    start, end = pd.Timestamp(start).normalize(), pd.Timestamp(end).normalize()
    events_pdf = events_pdf.copy()
    events_pdf["event_ts"] = pd.to_datetime(events_pdf["event_ts"])

    taste = observed_genre_affinity(
        events_pdf, titles_pdf, through=start + pd.Timedelta(days=taste_window_days))
    viewers = viewers_pdf.merge(taste, on="viewer_id", how="left")
    for g in GENRES:
        viewers[f"taste_{g}"] = viewers[f"taste_{g}"].fillna(1.0 / len(GENRES))

    # ---- which viewers are on the homepage on which days -------------------
    days = pd.date_range(start, end, freq="D")
    grid = viewers[["viewer_id", "activity_level", "country", "language"]].merge(
        pd.DataFrame({"day": days}), how="cross")
    lam = np.clip(grid["activity_level"].to_numpy(dtype=float) * sessions_per_active_day, 0.05, 4.0)
    grid["n_sessions"] = rng.poisson(lam)
    grid = grid[grid["n_sessions"] > 0]
    sessions = grid.loc[grid.index.repeat(grid["n_sessions"])].copy()
    sessions["session_seq"] = sessions.groupby(["viewer_id", "day"]).cumcount()
    n = len(sessions)

    # Session-level context. Hour is device-correlated: TV in the evening, mobile
    # through the day -- so hour_of_day and device are not independent noise.
    device_p = np.array([0.34, 0.36, 0.22, 0.08])
    dev_idx = rng.choice(len(DEVICES), size=n, p=device_p)
    sessions["device"] = np.array(DEVICES)[dev_idx]
    base_hour = np.where(np.isin(sessions["device"], ["tv", "console"]), 20.0, 15.0)
    sessions["hour_of_day"] = np.clip(
        np.rint(rng.normal(base_hour, 3.4)), 0, 23).astype("int64")
    sessions["rendered_ts"] = (
        sessions["day"]
        + pd.to_timedelta(sessions["hour_of_day"], unit="h")
        + pd.to_timedelta(rng.integers(0, 3600, size=n), unit="s"))
    sessions["locale"] = np.where(
        sessions["language"].eq("Japanese"), "ja-JP",
        np.where(sessions["country"].eq("BR"), "pt-BR",
                 np.where(sessions["country"].eq("MX"), "es-MX", "en-US")))
    sessions["session_id"] = (
        "hs_" + sessions["viewer_id"].astype(str) + "_"
        + sessions["day"].dt.strftime("%Y%m%d") + "_"
        + sessions["session_seq"].astype(str))

    # ---- viewer state that gates the personalized rails --------------------
    watched = events_pdf[events_pdf["watch_seconds"].fillna(0) > 0]
    first_watch = watched.groupby("viewer_id")["event_ts"].min()
    simulcast_ids = set(titles_pdf.loc[titles_pdf["is_simulcast"], "title_id"])
    first_simulcast = (watched[watched["title_id"].isin(simulcast_ids)]
                       .groupby("viewer_id")["event_ts"].min())
    # A viewer has something in progress if they watched in the previous 7 days.
    watch_days = (watched.assign(d=watched["event_ts"].dt.normalize())
                  .drop_duplicates(["viewer_id", "d"])[["viewer_id", "d"]])

    sessions["has_history"] = (
        sessions["viewer_id"].map(first_watch) <= sessions["rendered_ts"]).fillna(False)
    sessions["has_simulcast_history"] = (
        sessions["viewer_id"].map(first_simulcast) <= sessions["rendered_ts"]).fillna(False)
    # Watchlist is a stable per-viewer trait, not something we simulate a feed for.
    sessions["has_watchlist"] = sessions["viewer_id"].map(
        lambda v: _stable_unit("watchlist", v) < 0.62)

    recent = watch_days.rename(columns={"d": "day"})
    inprog = set()
    for off in range(0, 8):
        shifted = recent.copy()
        shifted["day"] = shifted["day"] + pd.Timedelta(days=off)
        inprog.update(map(tuple, shifted[["viewer_id", "day"]].to_numpy()))
    sessions["has_inprogress"] = [
        (v, d) in inprog for v, d in zip(sessions["viewer_id"], sessions["day"])]

    # ---- explode to (session x eligible rail) ------------------------------
    rails = rails_pdf.copy()
    imp = sessions.merge(rails, how="cross")
    gate = imp["rail_id"].map(STATE_DEPENDENT)
    eligible = np.ones(len(imp), dtype=bool)
    for rail_id, flag in STATE_DEPENDENT.items():
        mask = imp["rail_id"].to_numpy() == rail_id
        eligible &= ~mask | imp[flag].to_numpy(dtype=bool)
    imp = imp[eligible].copy()
    del gate

    # ---- the logging policy decides positions -----------------------------
    # Editorial order plus jitter, ranked per session. Jitter matters: with a
    # fixed order, position and rail identity would be collinear and the
    # propensity correction would be unidentifiable.
    imp["_policy_score"] = (imp["editorial_rank"].to_numpy(dtype=float)
                            + rng.normal(0.0, 1.6, size=len(imp)))
    imp = imp.sort_values(["session_id", "_policy_score"])
    imp["rail_position"] = imp.groupby("session_id").cumcount() + 1

    # ---- latent utility: what the model has to recover ---------------------
    genre_idx = imp["rail_genre"].map(_genre_idx).to_numpy()
    taste_mat = viewers.set_index("viewer_id")[[f"taste_{g}" for g in GENRES]]
    taste_lookup = taste_mat.reindex(imp["viewer_id"]).to_numpy()
    rail_taste = np.where(genre_idx >= 0,
                          taste_lookup[np.arange(len(imp)), np.clip(genre_idx, 0, None)],
                          1.0 / len(GENRES))

    # Stable viewer x rail preference. Past clicks and future clicks share this
    # draw, which is exactly why vr_ctr_30d is predictive rather than noise.
    vr_pref = np.array([_stable_unit("vr", v, r)
                        for v, r in zip(imp["viewer_id"], imp["rail_id"])])

    rtype = imp["rail_type"].to_numpy()
    type_base = pd.Series(rtype).map({
        "continue_watching": 0.95, "new_release": 0.55, "personalized": 0.60,
        "trending": 0.40, "watchlist": 0.50, "genre": 0.25, "editorial": 0.10,
    }).to_numpy(dtype=float)

    # TV sessions lean to long-form and continue-watching; mobile to short and
    # trending. A device x rail_type interaction the on-demand layer can express.
    dev = imp["device"].to_numpy()
    device_bonus = np.where((dev == "tv") & np.isin(rtype, ["continue_watching", "new_release"]), 0.30,
                     np.where((dev == "mobile") & np.isin(rtype, ["trending", "editorial"]), 0.22, 0.0))

    hour = imp["hour_of_day"].to_numpy(dtype=float)
    evening = np.exp(-((hour - 21.0) ** 2) / (2 * 4.0 ** 2))
    hour_bonus = np.where(np.isin(rtype, ["continue_watching", "new_release"]), 0.35 * evening, 0.0)

    utility = (2.7 * rail_taste
               + 1.05 * type_base
               + 1.25 * (vr_pref - 0.5)
               + device_bonus
               + hour_bonus
               - 1.85
               + rng.normal(0.0, 0.32, size=len(imp)))
    p_engage = 1.0 / (1.0 + np.exp(-utility))

    p_view = view_probability(imp["rail_position"].to_numpy(dtype=float))
    imp["was_viewport"] = rng.random(len(imp)) < p_view
    imp["engaged"] = (imp["was_viewport"].to_numpy() & (rng.random(len(imp)) < p_engage)).astype("int32")

    # Depth of engagement, for the graded-label variant and for rail features.
    titles_played = np.where(imp["engaged"].to_numpy() == 1,
                             1 + rng.poisson(0.45, size=len(imp)), 0)
    imp["titles_played"] = titles_played.astype("int32")
    imp["watch_seconds_from_rail"] = np.where(
        titles_played > 0,
        np.rint(titles_played * rng.gamma(shape=2.1, scale=430.0, size=len(imp))),
        0).astype("int64")
    imp["dwell_ms"] = np.where(
        imp["was_viewport"].to_numpy(),
        np.rint(rng.gamma(shape=2.0, scale=900.0, size=len(imp))), 0).astype("int64")

    imp["impression_id"] = ("hi_" + imp["session_id"].astype(str) + "_" + imp["rail_id"].astype(str))
    imp["surface"] = "home"
    imp["day_of_week"] = imp["rendered_ts"].dt.dayofweek.astype("int64")

    cols = ["impression_id", "viewer_id", "rail_id", "session_id", "rendered_ts",
            "rail_position", "surface", "device", "locale", "hour_of_day",
            "day_of_week", "was_viewport", "engaged", "titles_played",
            "watch_seconds_from_rail", "dwell_ms"]
    return imp[cols].sort_values("rendered_ts").reset_index(drop=True)


# ------------------------------------------------------------ position propensity
def position_propensity(impressions_pdf: pd.DataFrame) -> pd.DataFrame:
    """Empirical P(viewport | position), and the IPS weight that undoes it.

    Training on a homepage log without this teaches the model that position 1 is
    good, which is circular -- position 1 is where the *old* policy put things.
    Weighting each row by 1 / P(view | position) recovers an estimate of what the
    click rate would have been if every rail had had an equal chance of being
    seen. Clipped, because an unclipped inverse propensity on a long tail is
    variance with no bound.
    """
    g = (impressions_pdf.groupby("rail_position")
         .agg(impressions=("impression_id", "count"),
              viewports=("was_viewport", "sum"))
         .reset_index())
    g["p_view"] = (g["viewports"] / g["impressions"]).clip(lower=0.02)
    g["ips_weight"] = (1.0 / g["p_view"]).clip(upper=10.0).round(4)
    g["p_view"] = g["p_view"].round(4)
    return g[["rail_position", "impressions", "p_view", "ips_weight"]]


# --------------------------------------------------------------- rail features
# Aggregated in Spark, not pandas. The result is sixteen rows either way, but the
# input is the whole homepage log -- 363k impressions in this demo and billions in
# production. Pulling that to the driver with toPandas() killed the serverless
# kernel here on 2026-09-16 ("Fatal error: The Python kernel is unresponsive")
# after the table had already been written, which is the worst place to run out of
# memory. Everything downstream of this stays pandas because it is 16 rows.
RAIL_AUDIENCE_SQL = """
SELECT rail_id,
       COUNT(*)                          AS rail_impressions_30d,
       SUM(engaged)                      AS rail_clicks_30d,
       SUM(watch_seconds_from_rail)      AS rail_watch_seconds_30d,
       SUM(titles_played)                AS rail_titles_played_30d
FROM {impressions}
WHERE rendered_ts > TIMESTAMP '{cutoff}'
GROUP BY rail_id
"""


def rail_audience(spark, impressions_table: str, as_of: pd.Timestamp) -> pd.DataFrame:
    """Last-30-day audience behaviour per rail. One row per rail."""
    cutoff = (pd.Timestamp(as_of) - pd.Timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    return spark.sql(RAIL_AUDIENCE_SQL.format(impressions=impressions_table,
                                              cutoff=cutoff)).toPandas()


def build_rail_features(rails_pdf: pd.DataFrame, rail_titles_pdf: pd.DataFrame,
                        title_features_pdf: pd.DataFrame,
                        audience: pd.DataFrame) -> pd.DataFrame:
    """Per-rail features: the pre-aggregated audience behaviour from
    `rail_audience` joined to the content each rail carries.

    Sixteen rows in, sixteen rows out -- pandas is the right tool at this size and
    the wrong one for the log the aggregate came from.

    Content stats come from the **`title_features` feature table**, not from the raw
    `titles` table. That is the whole point of the shared feature store and it was not
    true in the first version of this function, which read `titles` directly and so
    shared title signal at source-data level only (verification_log V57). Three
    consequences of the change, all improvements:

      * `rail_content_age_days` now averages the governed `days_since_release` instead
        of recomputing age from `release_year` against a July-1 approximation -- one
        definition of content age instead of two that can drift;
      * `rail_avg_popularity` now averages the observed `popularity_30d` rather than
        the generator's latent `intrinsic_popularity`, which also removes a mild
        leakage: intrinsic_popularity is a parameter that *produced* the engagement
        this model is trained to predict;
      * `avg_rating` and `is_simulcast` come from the same table the watch-next ranker
        reads, so the two models cannot disagree about what those mean.

    Both popularity columns are on a 0-1 scale (verified: intrinsic 0.11-0.99 mean
    0.55, observed 0.01-1.00 mean 0.26), so this changes the values without changing
    the feature's range or sign.
    """
    members = rail_titles_pdf.merge(title_features_pdf, on="title_id", how="left")

    content = (members.groupby("rail_id")
               .agg(rail_titles_available=("title_id", "count"),
                    rail_avg_popularity=("popularity_30d", "mean"),
                    rail_avg_rating=("avg_rating", "mean"),
                    rail_content_age_days=("days_since_release", "mean"),
                    rail_simulcast_share=("is_simulcast", "mean"))
               .reset_index())

    out = rails_pdf.merge(audience, on="rail_id", how="left").merge(content, on="rail_id", how="left")
    out["rail_impressions_30d"] = out["rail_impressions_30d"].fillna(0).astype("int64")
    out["rail_clicks_30d"] = out["rail_clicks_30d"].fillna(0).astype("int64")
    out["rail_ctr_30d"] = (out["rail_clicks_30d"] /
                           out["rail_impressions_30d"].replace(0, np.nan)).fillna(0.0).round(5)
    out["rail_avg_watch_minutes_30d"] = (
        out["rail_watch_seconds_30d"].fillna(0) / 60.0
        / out["rail_clicks_30d"].replace(0, np.nan)).fillna(0.0).round(3)

    # Personalized rails carry no static membership, so their content stats are
    # not missing data -- they are "resolved per viewer". Zero is the honest fill
    # and rail_is_personalized is the flag that tells the model to read it that way.
    out["rail_titles_available"] = out["rail_titles_available"].fillna(0).astype("int64")
    for c in ("rail_avg_popularity", "rail_avg_rating", "rail_content_age_days",
              "rail_simulcast_share"):
        out[c] = out[c].fillna(0.0).round(4)

    out["rail_is_personalized"] = out["is_personalized"].astype(int)
    out["rail_type_idx"] = out["rail_type"].map({t: i for i, t in enumerate(RAIL_TYPES)}).astype("int64")
    out["rail_genre_idx"] = out["rail_genre"].map(_genre_idx).astype("int64")
    out["rail_editorial_rank"] = out["editorial_rank"].astype("int64")
    return out[["rail_id"] + RAIL_FEATURE_COLS].copy()


# -------------------------------------------------- viewer x rail, point in time
VIEWER_RAIL_TS_SQL = """
WITH daily AS (
  SELECT viewer_id,
         rail_id,
         date_trunc('DAY', rendered_ts)                       AS day,
         COUNT(*)                                             AS impressions,
         SUM(engaged)                                         AS clicks,
         SUM(rail_position)                                   AS position_sum,
         SUM(watch_seconds_from_rail)                         AS watch_seconds,
         MAX(CASE WHEN engaged = 1
                  THEN CAST(unix_timestamp(rendered_ts) AS BIGINT) END) AS last_click_epoch_s
  FROM {impressions}
  GROUP BY 1, 2, 3
),
-- Dense (viewer, rail, day) grid. Without it a 30-day rolling window would
-- silently mean "the last 30 rows we happen to have", which is a different
-- feature on a sparse viewer than on a heavy one.
grid AS (
  SELECT vr.viewer_id, vr.rail_id, d.day
  FROM (SELECT DISTINCT viewer_id, rail_id FROM daily) vr
  CROSS JOIN (SELECT DISTINCT day FROM daily) d
),
dense AS (
  SELECT g.viewer_id, g.rail_id, g.day,
         COALESCE(daily.impressions, 0)   AS impressions,
         COALESCE(daily.clicks, 0)        AS clicks,
         COALESCE(daily.position_sum, 0)  AS position_sum,
         COALESCE(daily.watch_seconds, 0) AS watch_seconds,
         daily.last_click_epoch_s
  FROM grid g
  LEFT JOIN daily
    ON daily.viewer_id = g.viewer_id AND daily.rail_id = g.rail_id AND daily.day = g.day
),
rolled AS (
  SELECT viewer_id, rail_id, day,
         SUM(impressions)   OVER w30 AS vr_impressions_30d,
         SUM(clicks)        OVER w30 AS vr_clicks_30d,
         SUM(clicks)        OVER w7  AS vr_clicks_7d,
         SUM(position_sum)  OVER w30 AS position_sum_30d,
         SUM(watch_seconds) OVER w30 AS watch_seconds_30d,
         MAX(last_click_epoch_s) OVER wall AS vr_last_click_epoch_s
  FROM dense
  WINDOW
    w30 AS (PARTITION BY viewer_id, rail_id ORDER BY CAST(day AS DATE)
            RANGE BETWEEN 29 PRECEDING AND CURRENT ROW),
    w7  AS (PARTITION BY viewer_id, rail_id ORDER BY CAST(day AS DATE)
            RANGE BETWEEN 6 PRECEDING AND CURRENT ROW),
    wall AS (PARTITION BY viewer_id, rail_id ORDER BY CAST(day AS DATE)
             RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT viewer_id,
       rail_id,
       -- The snapshot is stamped at the *end* of the day it summarises, so a
       -- point-in-time lookup for an impression on day D can only ever see
       -- day D-1 and earlier. Stamping it at day-start would let the training
       -- row read clicks that happened after it.
       day + INTERVAL 1 DAY                                   AS ts,
       -- Every feature column below is DOUBLE, including the counts and the epoch,
       -- and that is a deliberate serving decision rather than carelessness about
       -- types. This is the only lookup in the model that MISSES: a (viewer, rail)
       -- pair with no snapshot yet returns NULL. With BIGINT columns there is no
       -- correct option --
       --   * leave the NULLs and pandas widens the column to float64, so the logged
       --     signature says `double` and serving refuses to narrow the real int64
       --     ("Can not safely convert int64 to float64");
       --   * fill the NULLs to keep int64 and the signature says `long (required)`,
       --     which cannot represent the NULL a real lookup miss produces.
       -- Both surface from the endpoint as an empty `Error ''`. DOUBLE end to end
       -- means source, signature and serving agree and NULL stays representable.
       -- Counts as doubles are fine: these are model inputs, not ledger entries.
       CAST(vr_impressions_30d AS DOUBLE)                     AS vr_impressions_30d,
       CAST(vr_clicks_30d AS DOUBLE)                          AS vr_clicks_30d,
       ROUND(vr_clicks_30d / NULLIF(vr_impressions_30d, 0), 5) AS vr_ctr_30d,
       CAST(vr_clicks_7d AS DOUBLE)                           AS vr_clicks_7d,
       ROUND(watch_seconds_30d / 60.0, 3)                     AS vr_watch_minutes_30d,
       ROUND(position_sum_30d / NULLIF(vr_impressions_30d, 0), 3) AS vr_avg_position_30d,
       CAST(COALESCE(vr_last_click_epoch_s, 0) AS DOUBLE)     AS vr_last_click_epoch_s
FROM rolled
"""


def build_viewer_rail_timeseries(spark, impressions_table: str):
    """Daily viewer x rail snapshots -- the point-in-time source for vertical training.

    Spark, not pandas: this is a range window over a dense (viewer, rail, day)
    grid, which is 300 x 16 x 90 rows here and viewers x rails x days in
    production. A pandas groupby-rolling over that grid is the one part of this
    pipeline that would not survive real cardinality.
    """
    from pyspark.sql import functions as F
    df = spark.sql(VIEWER_RAIL_TS_SQL.format(impressions=impressions_table))
    return df.withColumn("vr_ctr_30d", F.coalesce("vr_ctr_30d", F.lit(0.0))) \
             .withColumn("vr_watch_minutes_30d", F.coalesce("vr_watch_minutes_30d", F.lit(0.0))) \
             .withColumn("vr_avg_position_30d", F.coalesce("vr_avg_position_30d", F.lit(0.0)))


# ------------------------------------------------------- request-time eligibility
# The window here MUST match the one generate_rail_impressions used, and it must be
# measured from the same clock. Two bugs live in this one detail:
#
#   * a 30-day window at serving against the generator's 7 days makes every rail
#     eligible for every viewer, so the "eligible set varies per request" property
#     the serving contract exists to handle silently stops being true;
#   * `current_timestamp()` against generated history that ends days ago answers a
#     question about wall clock, not about the data. Every other part of this repo
#     anchors on the newest event (`Config.demo_now`) and this has to as well, or
#     the eligible set drifts a little further from the training log every day.
#
# Observed 2026-09-16 before the fix: all 16 rails eligible for all 5 viewers
# sampled, against a homepage log in which r_continue and r_watchlist genuinely
# carried fewer impressions than the always-eligible rails.
INPROGRESS_DAYS = 7

ELIGIBLE_RAILS_SQL = """
WITH clock AS (
  {clock_expr}
),
watched AS (
  SELECT title_id, MAX(event_ts) AS last_ts
  FROM {fq}.engagement_events
  WHERE viewer_id = '{viewer_id}' AND watch_seconds > 0
  GROUP BY title_id
),
state AS (
  SELECT
    (SELECT COUNT(*) FROM watched, clock
      WHERE last_ts > clock.as_of - INTERVAL {inprogress_days} DAY) > 0 AS has_inprogress,
    (SELECT COUNT(*) FROM watched) > 0                                  AS has_history,
    (SELECT COUNT(*) FROM watched w JOIN {fq}.titles t ON t.title_id = w.title_id
      WHERE t.is_simulcast) > 0                                         AS has_simulcast_history
)
SELECT r.rail_id, r.rail_name, r.rail_type, r.rail_genre, r.editorial_rank
FROM {fq}.rails r CROSS JOIN state s
WHERE CASE r.rail_id
        WHEN 'r_continue'  THEN s.has_inprogress
        WHEN 'r_because'   THEN s.has_history
        WHEN 'r_new_eps'   THEN s.has_simulcast_history
        WHEN 'r_watchlist' THEN {has_watchlist}
        ELSE true
      END
ORDER BY r.editorial_rank
"""


def has_watchlist(viewer_id: str) -> bool:
    """Same stable trait the generator used, so the demo's eligible set matches the
    log the model was trained on. About 60% of viewers have one."""
    return _stable_unit("watchlist", viewer_id) < 0.62


def eligible_rails(spark, fq: str, viewer_id: str, as_of=None) -> pd.DataFrame:
    """The eligible rail set for one viewer, as of the demo clock.

    This is the hard filter that runs *before* scoring, the vertical-ranking twin of
    the entitlement join in candidates.py. Eligibility is policy, so it is not a
    model feature -- a ranker must not be able to trade away a rail the viewer is
    not allowed to see.

    `as_of` defaults to the newest engagement event, which is what makes the
    eligible set here the same one the training log was generated against.
    """
    # Built in Python rather than COALESCEd in SQL: `TIMESTAMP ''` is a cast error,
    # not a null, so an empty literal would fail the whole query rather than fall
    # back to the data clock.
    if as_of is None:
        clock_expr = f"SELECT MAX(event_ts) AS as_of FROM {fq}.engagement_events"
    else:
        ts = pd.Timestamp(as_of).strftime("%Y-%m-%d %H:%M:%S")
        clock_expr = f"SELECT TIMESTAMP '{ts}' AS as_of"
    sql = ELIGIBLE_RAILS_SQL.format(
        fq=fq, viewer_id=viewer_id,
        clock_expr=clock_expr,
        inprogress_days=INPROGRESS_DAYS,
        has_watchlist="true" if has_watchlist(viewer_id) else "false")
    return spark.sql(sql).toPandas()


def rail_request_records(viewer_id: str, rail_ids, device: str = "tv",
                         locale: str = "en-US", hour_of_day: int = None,
                         day_of_week: int = None, request_epoch_s: int = None):
    """The vertical-ranking request payload: one row per eligible rail.

    All rows share the viewer and the context; only rail_id varies. That shape is
    what lets the endpoint do automatic feature lookup for every candidate rail
    in a single request, and it is the shape the latency benchmark sweeps.
    """
    import time as _time
    now = int(_time.time())
    epoch = now if request_epoch_s is None else int(request_epoch_s)
    lt = _time.localtime(epoch)
    hour = lt.tm_hour if hour_of_day is None else int(hour_of_day)
    dow = ((lt.tm_wday) if day_of_week is None else int(day_of_week))
    return [
        {"viewer_id": viewer_id, "rail_id": str(r), "device": device,
         "locale": locale, "hour_of_day": int(hour), "day_of_week": int(dow),
         "request_epoch_s": epoch}
        for r in rail_ids
    ]


# ------------------------------------------------------------------ set-wide eligibility
ELIGIBLE_RAILS_ALL_SQL = """
WITH clock AS (
  {clock_expr}
),
watched AS (
  SELECT viewer_id, title_id, MAX(event_ts) AS last_ts
  FROM {fq}.engagement_events
  WHERE watch_seconds > 0
  GROUP BY viewer_id, title_id
),
state AS (
  SELECT v.viewer_id,
         COALESCE(MAX(CASE WHEN w.last_ts > c.as_of - INTERVAL {inprogress_days} DAY
                           THEN 1 ELSE 0 END), 0) = 1                       AS has_inprogress,
         COALESCE(MAX(CASE WHEN w.title_id IS NOT NULL THEN 1 ELSE 0 END), 0) = 1
                                                                            AS has_history,
         COALESCE(MAX(CASE WHEN t.is_simulcast THEN 1 ELSE 0 END), 0) = 1    AS has_simulcast_history
  FROM {fq}.viewers v
  CROSS JOIN clock c
  LEFT JOIN watched w ON w.viewer_id = v.viewer_id
  LEFT JOIN {fq}.titles t ON t.title_id = w.title_id
  {viewer_filter}
  GROUP BY v.viewer_id
)
SELECT s.viewer_id, r.rail_id
FROM state s
CROSS JOIN {fq}.rails r
LEFT JOIN viewer_watchlist_flag f ON f.viewer_id = s.viewer_id
WHERE CASE r.rail_id
        WHEN 'r_continue'  THEN s.has_inprogress
        WHEN 'r_because'   THEN s.has_history
        WHEN 'r_new_eps'   THEN s.has_simulcast_history
        WHEN 'r_watchlist' THEN COALESCE(f.has_watchlist, false)
        ELSE true
      END
"""


def eligible_rails_all(spark, fq: str, viewers=None, as_of=None):
    """Eligible (viewer_id, rail_id) pairs for many viewers, as a Spark DataFrame.

    The batch twin of `eligible_rails`. That one interpolates a single viewer id into
    the SQL and returns pandas, which is right for one homepage and wrong for scoring
    a whole population -- 300 separate queries at demo scale, millions at Crunchyroll's.

    Same gating rules, evaluated set-wide, so a batch-scored table and a live request
    agree about which collections a viewer is allowed to see. If these two ever
    disagreed, batch and online would differ for reasons that have nothing to do with
    the model.

    `has_watchlist` is a deterministic function of the viewer id in this demo, so it is
    materialised into a small helper table rather than reimplemented in SQL -- one
    definition, same answer in both paths.
    """
    from pyspark.sql import functions as F

    clock_expr = (f"SELECT TIMESTAMP'{as_of}' AS as_of" if as_of is not None
                  else f"SELECT MAX(event_ts) AS as_of FROM {fq}.engagement_events")

    # Materialise the watchlist trait as a temp view so the SQL below and the
    # single-viewer path read the same Python definition instead of two copies.
    vids = [r["viewer_id"] for r in spark.sql(f"SELECT viewer_id FROM {fq}.viewers").collect()]
    spark.createDataFrame(
        [(v, bool(has_watchlist(v))) for v in vids],
        ["viewer_id", "has_watchlist"]).createOrReplaceTempView("viewer_watchlist_flag")

    # `viewers is None` means every viewer; an empty list means no viewer. Testing
    # truthiness collapses those two, and the collapse is silent: an incremental run
    # that found nothing to rescore would rescore the whole population and still print
    # "0 viewers to rescore".
    viewer_filter = ""
    if viewers is not None:
        if not viewers:
            viewer_filter = "WHERE 1 = 0"
        else:
            ids = ",".join(f"'{v}'" for v in viewers)
            viewer_filter = f"WHERE v.viewer_id IN ({ids})"

    return spark.sql(ELIGIBLE_RAILS_ALL_SQL.format(
        fq=fq, clock_expr=clock_expr, inprogress_days=INPROGRESS_DAYS,
        viewer_filter=viewer_filter))


# ------------------------------------------------------- the model's column taxonomy
def model_columns():
    """Which columns of a rail training set are numeric features, which are
    categorical, and which are deliberately neither.

    Defined once here because two notebooks now fit models on this training set --
    notebook 22 (scikit-learn) and notebook 32 (torch on GPU) -- and a second copy of
    this list is exactly the drift this repo argues against. The GPU notebook first
    derived it by exclusion instead and produced two bugs at once: it fed the raw
    epochs to the model, and it crashed on `could not convert string to float:
    'sci_fi'` because `last_primary_genre` is a string.

    Two rules are encoded here and both matter more than they look:

      * **rendered position is never a feature.** `rail_position` and `was_viewport`
        are label-side: at request time the position is the output, not an input.
      * **raw epochs are never features.** `last_event_epoch_s` and
        `vr_last_click_epoch_s` exist to feed the decay UDFs, which turn them into
        something with meaning. Fed to a model directly they are a proxy for calendar
        date, and they poison anything trained in one window and served in another.

    Returns (numeric, categorical, not_features).
    """
    from . import features as F
    from . import udfs as U

    viewer_num = list(F.VIEWER_FEATURE_COLS) + [
        c for c in F.RECENT_FEATURE_COLS
        if c != "last_primary_genre" and not c.endswith("_epoch_s")]
    rail_num = list(RAIL_FEATURE_COLS)
    vr_num = [c for c in VIEWER_RAIL_FEATURE_COLS if not c.endswith("_epoch_s")]
    context_num = ["hour_of_day", "day_of_week"]
    ondemand = list(U.RAIL_ONDEMAND_OUTPUTS)

    numeric = viewer_num + rail_num + vr_num + context_num + ondemand
    categorical = ["device", "locale", "last_primary_genre"]
    not_features = ["viewer_id", "rail_id", "engaged", "request_epoch_s",
                    "last_event_epoch_s", "vr_last_click_epoch_s",
                    "rail_position", "was_viewport", "sample_weight", "ts"]
    return numeric, categorical, not_features


def check_model_columns(columns, numeric, categorical, not_features):
    """Assert the training frame has what the model expects, and report anything it
    carries that nobody claimed -- a new feature column silently going unused is the
    failure mode this catches."""
    missing = [c for c in numeric + categorical if c not in columns]
    unused = sorted(set(columns) - set(numeric) - set(categorical) - set(not_features))
    return missing, unused
