# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Feature engineering + Lakebase Online Feature Store
# MAGIC
# MAGIC Turns raw `crunchyroll_demo` signals into governed features, following the
# MAGIC deck's feature map — freshness is chosen **per feature class**, not globally.
# MAGIC
# MAGIC | Feature table | PK | Freshness | Store |
# MAGIC |---|---|---|---|
# MAGIC | `viewer_features_ts` | viewer_id + ts | daily snapshots | offline only (point-in-time training) |
# MAGIC | `viewer_features_current` | viewer_id | latest | offline + **online (Lakebase)** |
# MAGIC | `title_features` | title_id | daily | offline + **online (Lakebase)** |
# MAGIC | `recent_behavior_current` | viewer_id | periodic refresh | offline + **online (Lakebase)** |
# MAGIC
# MAGIC One definition feeds both stores: historically correct training offline,
# MAGIC latest keyed values online. No rebuilt joins, no training-serving skew.
# COMMAND ----------
# MAGIC %pip install databricks-feature-engineering --quiet
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
CATALOG = "serverless_lakebase_praneeth_catalog"
SCHEMA = "crunchyroll_demo"
ONLINE_STORE = "crunchyroll-online-store"

spark.sql(f"USE {CATALOG}.{SCHEMA}")

import pandas as pd
import numpy as np
import datetime as dt

END_TS = pd.Timestamp("2026-08-31 23:59:59")
GENRES = ["action", "adventure", "fantasy", "sci_fi", "sports", "drama", "romance", "slice_of_life"]

events = spark.table(f"{CATALOG}.{SCHEMA}.engagement_events").toPandas()
titles = spark.table(f"{CATALOG}.{SCHEMA}.titles").toPandas()
print("events:", len(events), "| titles:", len(titles))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Viewer features — daily snapshots for point-in-time training
# MAGIC
# MAGIC Long-horizon signals (genre affinity over 30d, completion propensity,
# MAGIC watch frequency) recomputed **as of each day** so training can ask
# MAGIC "what did we know about this viewer at impression time?"
# COMMAND ----------
ev = events.copy()
ev["date"] = pd.to_datetime(ev["event_ts"]).dt.normalize()
title_genre = titles.set_index("title_id")["primary_genre"].to_dict()
ev["primary_genre"] = ev["title_id"].map(title_genre)

# daily facts per viewer: minutes, plays, skips, completes, minutes per genre
watched = ev[ev["watch_seconds"].fillna(0) > 0].copy()
watched["minutes"] = watched["watch_seconds"] / 60.0
daily = (watched.groupby(["viewer_id", "date"])
         .agg(minutes=("minutes", "sum"), plays=("event_id", "count"),
              skips=("event_type", lambda s: int((s == "skip").sum())),
              completes=("event_type", lambda s: int((s == "complete").sum())))
         .reset_index())
gm = (watched.pivot_table(index=["viewer_id", "date"], columns="primary_genre",
                          values="minutes", aggfunc="sum", fill_value=0)
      .rename(columns=lambda g: f"gm_{g}").reset_index())
daily = daily.merge(gm, on=["viewer_id", "date"], how="left").fillna(0)

snapshots = []
all_dates = pd.date_range(ev["date"].min(), ev["date"].max(), freq="D")
for vid, g in daily.groupby("viewer_id"):
    g = g.set_index("date").reindex(all_dates, fill_value=0.0)
    g.index.name = "ts"
    r30 = g.rolling(30, min_periods=1).sum()
    r7 = g.rolling(7, min_periods=1).sum()
    total_min = r30["minutes"].replace(0, np.nan)
    row = pd.DataFrame({
        "viewer_id": vid,
        "ts": g.index,
        "minutes_watched_7d": r7["minutes"].round(2),
        "plays_7d": r7["plays"].round(0).astype(int),
        "completion_rate_30d": (r30["completes"] / r30["plays"].replace(0, np.nan)).fillna(0).round(4),
        "avg_watch_minutes_30d": (r30["minutes"] / r30["plays"].replace(0, np.nan)).fillna(0).round(2),
        "skips_7d": r7["skips"].astype(int),
    })
    for gen in GENRES:
        col = f"gm_{gen}"
        share = (r30[col] / total_min).fillna(0) if col in r30.columns else 0.0
        row[f"genre_affinity_{gen}"] = np.round(share, 4)
    snapshots.append(row.reset_index(drop=True))

viewer_ts = pd.concat(snapshots, ignore_index=True)
viewer_ts["ts"] = pd.to_datetime(viewer_ts["ts"])
feature_cols = [c for c in viewer_ts.columns if c not in ("viewer_id", "ts")]
print("viewer_features_ts:", viewer_ts.shape, "| feature cols:", feature_cols)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Current snapshots — the online mirror
# COMMAND ----------
viewer_current = (viewer_ts.sort_values("ts").groupby("viewer_id").tail(1)
                  .drop(columns=["ts"]).reset_index(drop=True))
print("viewer_features_current:", viewer_current.shape)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Title features — catalog + derived popularity
# COMMAND ----------
last30 = ev[pd.to_datetime(ev["event_ts"]) > END_TS - pd.Timedelta(days=30)]
plays30 = (last30[last30["event_type"].isin(["complete", "skip"])]
           .groupby("title_id").size().rename("plays_30d"))
tf = titles.copy()
tf = tf.merge(plays30, left_on="title_id", right_index=True, how="left").fillna({"plays_30d": 0})
tf["plays_30d"] = tf["plays_30d"].astype(int)
tf["popularity_30d"] = (tf["plays_30d"] / max(tf["plays_30d"].max(), 1)).round(4)
tf["days_since_release"] = tf["release_year"].map(lambda y: (END_TS - pd.Timestamp(f"{y}-07-01")).days)
tf["maturity_rank"] = tf["maturity_rating"].map({"all": 0, "13+": 1, "16+": 2, "18+": 3}).astype(int)
tf["is_simulcast"] = tf["is_simulcast"].astype(int)
tf["episodes_log"] = np.log1p(tf["episode_count"]).round(4)
for gen in GENRES:
    tf[f"genre_{gen}"] = (tf["primary_genre"] == gen).astype(int)
title_features = tf[["title_id", "popularity_30d", "plays_30d", "avg_rating",
                     "days_since_release", "maturity_rank", "is_simulcast", "episodes_log"]
                    + [f"genre_{g}" for g in GENRES]].copy()
print("title_features:", title_features.shape)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Recent behavior — the freshness-sensitive class
# COMMAND ----------
last24 = ev[pd.to_datetime(ev["event_ts"]) > END_TS - pd.Timedelta(hours=24)]
rb_rows = []
for vid in events["viewer_id"].unique():
    g = last24[last24["viewer_id"] == vid]
    g_w = g[g["watch_seconds"].fillna(0) > 0]
    last_genre = "none"
    if len(g_w):
        last_row = g_w.sort_values("event_ts").iloc[-1]
        last_genre = title_genre.get(last_row["title_id"], "none")
    rb_rows.append({
        "viewer_id": vid,
        "minutes_watched_24h": round(float(g_w["watch_seconds"].sum() / 60.0), 2),
        "skips_24h": int((g["event_type"] == "skip").sum()),
        "active_titles_24h": int(g_w["title_id"].nunique()),
        "last_primary_genre": last_genre,
    })
recent_behavior = pd.DataFrame(rb_rows)
print("recent_behavior_current:", recent_behavior.shape)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Register feature tables in Unity Catalog
# COMMAND ----------
from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()

def to_sdf(pdf):
    return spark.createDataFrame(pdf)

def recreate(pdf, name, primary_keys, description, timeseries=None):
    full = f"{CATALOG}.{SCHEMA}.{name}"
    spark.sql(f"DROP TABLE IF EXISTS {full}")
    kwargs = dict(name=full, primary_keys=primary_keys, df=to_sdf(pdf), description=description)
    if timeseries:
        kwargs["timeseries_column"] = timeseries
    fe.create_table(**kwargs)
    spark.sql(f"ALTER TABLE {full} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    cnt = spark.table(full).count()
    print(f"{full}: {cnt} rows | PK={primary_keys} | ts={timeseries}")

recreate(viewer_ts, "viewer_features_ts", ["viewer_id", "ts"],
         "Daily snapshots of long-horizon viewer features for point-in-time-correct training",
         timeseries="ts")
recreate(viewer_current, "viewer_features_current", ["viewer_id"],
         "Latest long-horizon viewer features — mirrored to the online store")
recreate(title_features, "title_features", ["title_id"],
         "Title catalog and derived popularity features — mirrored to the online store")
recreate(recent_behavior, "recent_behavior_current", ["viewer_id"],
         "Last-24h viewer behavior — freshness-sensitive class mirrored to the online store")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Create the Lakebase-backed Online Feature Store
# COMMAND ----------
import time
try:
    os_store = fe.create_online_store(name=ONLINE_STORE, capacity="CU_2")
    print("created online store:", ONLINE_STORE)
except Exception as e:
    print("create_online_store:", str(e)[:300])
    os_store = fe.get_online_store(name=ONLINE_STORE)

store = fe.get_online_store(name=ONLINE_STORE)
print("store object:", str(store)[:400])
# COMMAND ----------
# MAGIC %md
# MAGIC ## Publish features online (TRIGGERED incremental sync)
# COMMAND ----------
published = []

def publish_with_retry(full_src, full_dst, attempts=15):
    for i in range(attempts):
        try:
            fe.publish_table(
                online_store=fe.get_online_store(name=ONLINE_STORE),
                source_table_name=full_src,
                online_table_name=full_dst,
                publish_mode="TRIGGERED",
            )
            print("published:", full_src, "->", full_dst)
            return
        except Exception as e:
            print(f"publish attempt {i+1} for {full_dst}:", str(e)[:200])
            time.sleep(20)
    raise RuntimeError(f"publish failed: {full_dst}")

for src, dst in [
    ("viewer_features_current", "online_viewer_features"),
    ("title_features", "online_title_features"),
    ("recent_behavior_current", "online_recent_behavior"),
]:
    publish_with_retry(f"{CATALOG}.{SCHEMA}.{src}", f"{CATALOG}.{SCHEMA}.{dst}")
    published.append(f"{CATALOG}.{SCHEMA}.{dst}")

time.sleep(60)
for tbl in published:
    try:
        n = spark.sql(f"SELECT COUNT(*) AS n FROM {tbl}").first()["n"]
        print(f"online {tbl}: {n} rows")
    except Exception as e:
        print("online read pending:", tbl, str(e)[:200])
# COMMAND ----------
dbutils.notebook.exit("OK: 4 feature tables registered, online store + 3 online tables published")
