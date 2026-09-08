"""Feature definitions, defined exactly once.

Notebook 01 (batch build), 05 (triggered recompute) and 10 (streaming) all call
into here. The first version of this demo duplicated the recent-behavior math
between 01 and 05 -- which is the training/serving skew the demo argues against,
committed in the demo's own source. Everything shared now lives here.

Pure pandas / pyspark. No widgets, no clients, no printing.
"""
import numpy as np
import pandas as pd

from .config import GENRES, MATURITY_RANK

# Columns carried by each feature table, so notebooks and the app agree on the
# contract without re-deriving it from a DataFrame.
VIEWER_FEATURE_COLS = (
    ["minutes_watched_7d", "plays_7d", "completion_rate_30d", "avg_watch_minutes_30d",
     "skips_7d", "typical_watch_hour", "hour_concentration"]
    + [f"genre_affinity_{g}" for g in GENRES]
)
TITLE_FEATURE_COLS = (
    ["popularity_30d", "plays_30d", "avg_rating", "days_since_release",
     "maturity_rank", "is_simulcast", "episodes_log"]
    + [f"genre_{g}" for g in GENRES]
)
RECENT_FEATURE_COLS = ["minutes_watched_24h", "skips_24h", "active_titles_24h",
                       "last_primary_genre", "last_event_epoch_s"]
SESSION_FEATURE_COLS = ["session_seconds", "session_skips", "session_events",
                        "last_event_epoch_s", "src_event_epoch_ms"]


def title_genre_map(titles_pdf: pd.DataFrame) -> dict:
    return titles_pdf.set_index("title_id")["primary_genre"].to_dict()


def _prepare_events(events_pdf: pd.DataFrame, titles_pdf: pd.DataFrame) -> pd.DataFrame:
    ev = events_pdf.copy()
    ev["event_ts"] = pd.to_datetime(ev["event_ts"])
    ev["date"] = ev["event_ts"].dt.normalize()
    ev["primary_genre"] = ev["title_id"].map(title_genre_map(titles_pdf))
    return ev


# --------------------------------------------------------------------- viewer
def build_viewer_timeseries(events_pdf: pd.DataFrame, titles_pdf: pd.DataFrame) -> pd.DataFrame:
    """Daily viewer snapshots -- the point-in-time training source.

    Rolling 7d/30d windows recomputed as of every day, so training can ask what
    we knew at impression time. Watch-hour habit is a *circular* mean: hours 23
    and 1 are two hours apart, not twenty-two, so summing sin/cos and taking
    atan2 is the only correct way to roll it up.
    """
    ev = _prepare_events(events_pdf, titles_pdf)
    watched = ev[ev["watch_seconds"].fillna(0) > 0].copy()
    watched["minutes"] = watched["watch_seconds"] / 60.0

    ang = 2.0 * np.pi * watched["event_ts"].dt.hour / 24.0
    watched["hour_sin"] = np.sin(ang)
    watched["hour_cos"] = np.cos(ang)

    daily = (watched.groupby(["viewer_id", "date"])
             .agg(minutes=("minutes", "sum"),
                  plays=("event_id", "count"),
                  skips=("event_type", lambda s: int((s == "skip").sum())),
                  completes=("event_type", lambda s: int((s == "complete").sum())),
                  hour_sin=("hour_sin", "sum"),
                  hour_cos=("hour_cos", "sum"),
                  hour_n=("hour_sin", "count"))
             .reset_index())

    genre_minutes = (watched.pivot_table(index=["viewer_id", "date"], columns="primary_genre",
                                         values="minutes", aggfunc="sum", fill_value=0)
                     .rename(columns=lambda g: f"gm_{g}").reset_index())
    daily = daily.merge(genre_minutes, on=["viewer_id", "date"], how="left").fillna(0)

    all_dates = pd.date_range(ev["date"].min(), ev["date"].max(), freq="D")
    # Roll only the numeric fact columns. Leaving viewer_id in the frame makes
    # rolling().sum() raise "DataError: Cannot aggregate non-numeric type: object"
    # on pandas 2.x, which is what the serverless runtime ships.
    fact_cols = [c for c in daily.columns if c not in ("viewer_id", "date")]
    # Coerce here rather than relying on what groupby.agg inferred: the lambda
    # aggregations over a string column can come back as object dtype, and pandas
    # 2.x raises rather than skipping.
    for c in fact_cols:
        daily[c] = pd.to_numeric(daily[c], errors="coerce").fillna(0.0).astype("float64")

    snapshots = []
    for vid, g in daily.groupby("viewer_id"):
        g = (g.set_index("date")[fact_cols]
             .reindex(all_dates, fill_value=0.0)
             .astype("float64"))
        g.index.name = "ts"
        assert not any(str(t) == "object" for t in g.dtypes), \
            f"non-numeric fact column survived coercion: {dict(g.dtypes)}"
        r30 = g.rolling(30, min_periods=1).sum()
        r7 = g.rolling(7, min_periods=1).sum()
        total_min = r30["minutes"].replace(0, np.nan)

        n = r30["hour_n"].replace(0, np.nan)
        mean_ang = np.arctan2(r30["hour_sin"], r30["hour_cos"])
        typical_hour = ((mean_ang * 24.0 / (2.0 * np.pi)) % 24.0).where(n.notna(), 0.0)
        resultant = np.sqrt(r30["hour_sin"] ** 2 + r30["hour_cos"] ** 2) / n

        row = pd.DataFrame({
            "viewer_id": vid,
            "ts": g.index,
            "minutes_watched_7d": r7["minutes"].round(2),
            "plays_7d": r7["plays"].round(0).astype(int),
            "completion_rate_30d": (r30["completes"] / r30["plays"].replace(0, np.nan)).fillna(0).round(4),
            "avg_watch_minutes_30d": (r30["minutes"] / r30["plays"].replace(0, np.nan)).fillna(0).round(2),
            "skips_7d": r7["skips"].astype(int),
            "typical_watch_hour": typical_hour.fillna(0.0).round(3),
            "hour_concentration": resultant.fillna(0.0).round(4),
        })
        for gen in GENRES:
            col = f"gm_{gen}"
            share = (r30[col] / total_min).fillna(0) if col in r30.columns else 0.0
            row[f"genre_affinity_{gen}"] = np.round(share, 4)
        snapshots.append(row.reset_index(drop=True))

    viewer_ts = pd.concat(snapshots, ignore_index=True)
    viewer_ts["ts"] = pd.to_datetime(viewer_ts["ts"])
    return viewer_ts


def viewer_current_from_ts(viewer_ts: pd.DataFrame) -> pd.DataFrame:
    """The online mirror: newest snapshot per viewer, same definitions."""
    return (viewer_ts.sort_values("ts").groupby("viewer_id").tail(1)
            .drop(columns=["ts"]).reset_index(drop=True))


# ---------------------------------------------------------------------- title
def build_title_features(titles_pdf: pd.DataFrame, events_pdf: pd.DataFrame,
                         as_of: pd.Timestamp) -> pd.DataFrame:
    ev = _prepare_events(events_pdf, titles_pdf)
    last30 = ev[ev["event_ts"] > as_of - pd.Timedelta(days=30)]
    plays30 = (last30[last30["event_type"].isin(["complete", "skip"])]
               .groupby("title_id").size().rename("plays_30d"))

    tf = titles_pdf.copy()
    tf = tf.merge(plays30, left_on="title_id", right_index=True, how="left").fillna({"plays_30d": 0})
    tf["plays_30d"] = tf["plays_30d"].astype(int)
    tf["popularity_30d"] = (tf["plays_30d"] / max(tf["plays_30d"].max(), 1)).round(4)
    tf["days_since_release"] = tf["release_year"].map(
        lambda y: (as_of - pd.Timestamp(f"{int(y)}-07-01")).days)
    tf["maturity_rank"] = tf["maturity_rating"].map(MATURITY_RANK).astype(int)
    tf["is_simulcast"] = tf["is_simulcast"].astype(int)
    tf["episodes_log"] = np.log1p(tf["episode_count"]).round(4)
    for gen in GENRES:
        tf[f"genre_{gen}"] = (tf["primary_genre"] == gen).astype(int)
    return tf[["title_id"] + TITLE_FEATURE_COLS].copy()


# -------------------------------------------------------------- recent behavior
def build_recent_behavior(events_pdf: pd.DataFrame, titles_pdf: pd.DataFrame,
                          as_of: pd.Timestamp, viewer_ids=None) -> pd.DataFrame:
    """Last-24h behaviour as of `as_of`.

    Vectorised, unlike the original per-viewer Python loop, and it carries
    last_event_epoch_s so the on-demand session-decay UDF has something to
    subtract the request clock from. Pass viewer_ids to recompute a subset
    (notebook 05 does exactly one viewer).
    """
    ev = _prepare_events(events_pdf, titles_pdf)
    if viewer_ids is not None:
        wanted = pd.Index(pd.unique(pd.Series(list(viewer_ids))))
        ev = ev[ev["viewer_id"].isin(wanted)]
    else:
        wanted = pd.Index(pd.unique(events_pdf["viewer_id"]))

    win = ev[ev["event_ts"] > as_of - pd.Timedelta(hours=24)]
    watched = win[win["watch_seconds"].fillna(0) > 0]

    agg = pd.DataFrame(index=wanted)
    agg.index.name = "viewer_id"
    agg["minutes_watched_24h"] = (watched.groupby("viewer_id")["watch_seconds"].sum() / 60.0).round(2)
    agg["skips_24h"] = win[win["event_type"] == "skip"].groupby("viewer_id").size()
    agg["active_titles_24h"] = watched.groupby("viewer_id")["title_id"].nunique()

    if len(watched):
        last = watched.sort_values("event_ts").groupby("viewer_id").tail(1).set_index("viewer_id")
        agg["last_primary_genre"] = last["primary_genre"]
        agg["last_event_epoch_s"] = (last["event_ts"].astype("int64") // 10**9)
    else:
        agg["last_primary_genre"] = np.nan
        agg["last_event_epoch_s"] = np.nan

    agg = agg.reset_index()
    agg["minutes_watched_24h"] = agg["minutes_watched_24h"].fillna(0.0).astype(float)
    agg["skips_24h"] = agg["skips_24h"].fillna(0).astype(int)
    agg["active_titles_24h"] = agg["active_titles_24h"].fillna(0).astype(int)
    agg["last_primary_genre"] = agg["last_primary_genre"].fillna("none").astype(str)
    agg["last_event_epoch_s"] = agg["last_event_epoch_s"].fillna(0).astype("int64")
    return agg[["viewer_id"] + RECENT_FEATURE_COLS]


# ------------------------------------------------------------ streaming session
def session_aggregate(stream_df, watermark: str = "10 minutes"):
    """Per-viewer live session features off engagement_events_stream.

    Spark, not pandas -- this one runs continuously in notebook 10. Carries
    src_event_epoch_ms (the producer's own clock) forward so freshness can be
    measured by subtracting two readings of one clock.
    """
    from pyspark.sql import functions as F
    return (stream_df
            .withWatermark("event_ts", watermark)
            .groupBy("viewer_id")
            .agg(F.sum(F.coalesce(F.col("watch_seconds"), F.lit(0))).cast("double").alias("session_seconds"),
                 F.sum(F.when(F.col("event_type") == "skip", 1).otherwise(0)).cast("int").alias("session_skips"),
                 F.count("*").cast("int").alias("session_events"),
                 F.max(F.unix_timestamp("event_ts")).cast("long").alias("last_event_epoch_s"),
                 F.max("produced_epoch_ms").cast("long").alias("src_event_epoch_ms")))
