# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Freshness on demand — a binge changes the next ranking
# MAGIC
# MAGIC The claim under test: an event that lands now should change the next
# MAGIC recommendation. Not tomorrow, not after a nightly job.
# MAGIC
# MAGIC 1. Reset viewer `v0001` to a calm baseline so the demo is repeatable
# MAGIC 2. Rank 25 candidates — the "before"
# MAGIC 3. Three sci-fi episodes complete **right now**
# MAGIC 4. Recompute the viewer's last-24h features and refresh **both** online copies --
# MAGIC    `online_recent_behavior` for the app's raw panel, and
# MAGIC    `online_recent_behavior_ts` because that is what the rankers' feature specs read
# MAGIC 5. Rank the same 25 again — the "after"
# MAGIC
# MAGIC This is the **TRIGGERED** path: a refresh per change, on demand. Notebook 10
# MAGIC runs the same contract with `publish_mode="CONTINUOUS"`, where a streaming
# MAGIC pipeline keeps Lakebase current with no refresh call at all. Showing both is
# MAGIC the point — freshness is a per-feature-class decision, not one global switch.
# MAGIC
# MAGIC Two things this notebook fixes from its first version: live events go to
# MAGIC `engagement_events_stream` rather than mutating the training corpus, and the
# MAGIC recompute calls the same `src/crfs/features.py` definition notebook 01 used
# MAGIC instead of re-deriving the maths — re-deriving it was training/serving skew
# MAGIC inside the demo that argues against training/serving skew.
# COMMAND ----------
# MAGIC %pip install databricks-feature-engineering databricks-sdk "psycopg[binary]" --quiet
# COMMAND ----------
dbutils.library.restartPython()
# COMMAND ----------
import os, sys
# Walk up to the repo root instead of assuming a depth. These notebooks sit in
# track folders (00_shared, 10_horizontal, ...), and the previous
# `os.getcwd()/".."` resolved to notebooks/ the moment one moved -- which fails as
# ModuleNotFoundError: src, from a line that looks like boilerplate.
_root = os.getcwd()
while _root != "/" and not os.path.isdir(os.path.join(_root, "src", "crfs")):
    _root = os.path.dirname(_root)
assert os.path.isdir(os.path.join(_root, "src", "crfs")), \
    f"src/crfs not found above {os.getcwd()} -- is the bundle's whole file tree synced?"
if _root not in sys.path:
    sys.path.insert(0, _root)

from src.crfs.config import Config
from src.crfs import features as F
from src.crfs import ops, online, candidates as C

# n_events=0 means recompute-only: do not append events, just rebuild the feature and
# refresh the online copies. That is what the crfs_event_burst job needs, because its first
# task has already appended them.
cfg = Config.from_widgets(dbutils, extra_widgets={"viewer_id": "v0001", "n_events": "3"})
VID = cfg.extras.get("viewer_id", "v0001")
N_EVENTS = int(cfg.extras.get("n_events", "3"))
spark.sql(f"USE {cfg.fq}")

import json, time
import datetime as dt
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.feature_engineering import FeatureEngineeringClient

w = WorkspaceClient()
fe = FeatureEngineeringClient()
store = online.from_config(w, cfg)
print("viewer:", VID, "| endpoint:", cfg.ranker_endpoint)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Reset to a calm baseline
# MAGIC
# MAGIC A repeatable demo needs a known starting point: a quiet slice-of-life
# MAGIC evening, so the sci-fi burst is unmistakable.
# COMMAND ----------
BASELINE = pd.DataFrame([{
    "viewer_id": VID,
    "minutes_watched_24h": 38.5,
    "skips_24h": 0,
    "active_titles_24h": 2,
    "last_primary_genre": "slice_of_life",
    "last_event_epoch_s": int(time.time()) - 3 * 3600,
}])
RECENT = cfg.t("recent_behavior_current")
ONLINE_RECENT = cfg.t("online_recent_behavior")
# The point-in-time table and its online copy -- what both rankers' feature specs resolve
# since their lookups gained a timestamp_lookup_key (verification_log V76).
RECENT_TS = cfg.t("recent_behavior_ts")
ONLINE_RECENT_TS = cfg.t("online_recent_behavior_ts")

fe.write_table(name=RECENT, df=spark.createDataFrame(BASELINE), mode="merge")
ops.refresh_and_wait(w, ONLINE_RECENT, timeout_s=300)

row, cols, ms = store.keyed_read("online_recent_behavior", "viewer_id", VID)
online_before = dict(zip(cols, row))
print(f"\nonline row BEFORE ({ms:.0f} ms):")
print("  ", online_before)
# COMMAND ----------
# MAGIC %md
# MAGIC ## Rank 25 candidates — the "before"
# MAGIC
# MAGIC The request timestamp is frozen for the whole notebook. `session_decay` is a
# MAGIC function of the request clock, so letting it drift between the two rankings
# MAGIC would mean two different questions were asked.
# COMMAND ----------
REQUEST_EPOCH = int(time.time())
cands = C.candidates(spark, cfg.fq, VID, limit=25)
records = C.request_records(VID, cands["title_id"], surface="post_play", device="tv",
                            hour_of_day=21, request_epoch_s=REQUEST_EPOCH)
before_scores, before_ms = C.query_ranker(w, cfg.ranker_endpoint, records)
before = C.rank(cands, before_scores)
print(f"ranked 25 candidates in {before_ms:.0f} ms")
display(spark.createDataFrame(before[["title_name", "primary_genre", "play_start_probability"]].head(10)))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Three sci-fi episodes complete, right now
# MAGIC
# MAGIC These land in `engagement_events_stream`, carrying `produced_epoch_ms` — the
# MAGIC producer's own clock, which is what makes the freshness number measurable
# MAGIC rather than asserted.
# COMMAND ----------
scifi = spark.sql(f"""
  SELECT t.title_id, t.title_name
  FROM {cfg.t('titles')} t
  JOIN {cfg.t('entitlements')} e ON e.title_id = t.title_id AND e.viewer_id = '{VID}' AND e.allowed
  WHERE t.primary_genre = 'sci_fi'
  ORDER BY t.intrinsic_popularity DESC
  LIMIT {N_EVENTS}
""").toPandas()

now = dt.datetime.now()
produced_ms = int(time.time() * 1000)
burst = pd.DataFrame([{
    "event_id": f"live-{produced_ms}-{i}",
    "viewer_id": VID,
    "title_id": r.title_id,
    "event_ts": now - dt.timedelta(minutes=(N_EVENTS - i) * 8),
    "event_type": "complete",
    "watch_seconds": 1420.0,
    "surface": "post_play",
    "device": "tv",
    "locale": "en-US",
    "produced_epoch_ms": produced_ms,
} for i, r in enumerate(scifi.itertuples())])

if N_EVENTS > 0:
    (spark.createDataFrame(burst).write.mode("append")
     .saveAsTable(cfg.t("engagement_events_stream")))
    print(f"appended {len(burst)} completions:", ", ".join(scifi["title_name"]))
else:
    # n_events=0 is recompute-only, and it exists because of a double-count. The
    # crfs_event_burst job runs notebook 11 (which appends the events) and then this
    # notebook to recompute and refresh -- so with this cell appending as well, every click
    # of the app's "watch 3 episodes now" button recorded SIX completions while the UI said
    # three. The job now passes n_events=0 here.
    print("n_events=0: recompute-only, appending nothing. The caller already wrote the "
          "events -- see the crfs_event_burst job.")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Recompute the feature, refresh the online table
# MAGIC
# MAGIC One definition, called from `src/crfs/features.py`, over history plus the
# MAGIC live events. Then `ops.wait_for_sync` polls the real sync API until the
# MAGIC pipeline reports it has consumed the commit we just wrote — no `sleep(90)`,
# MAGIC and the wait is visible rather than a mystery.
# COMMAND ----------
hist = spark.sql(f"""
  SELECT event_id, viewer_id, title_id, event_ts, event_type, watch_seconds
  FROM {cfg.t('engagement_events')} WHERE viewer_id = '{VID}'
  UNION ALL
  SELECT event_id, viewer_id, title_id, event_ts, event_type, watch_seconds
  FROM {cfg.t('engagement_events_stream')} WHERE viewer_id = '{VID}'
""").toPandas()
titles_pdf = spark.table(cfg.t("titles")).toPandas()

recomputed = F.build_recent_behavior(hist, titles_pdf, pd.Timestamp(now), viewer_ids=[VID])
print("recomputed:", recomputed.to_dict("records"))

# BOTH tables, because the two rankers read the time series one.
#
# This notebook used to write only recent_behavior_current. Once the rankers' lookups
# became point-in-time they resolve `recent_behavior_ts` instead, so the freshness beat was
# updating a table no endpoint reads: the app's panel would show the new value while the
# ranking stayed identical, and this task could report a zero delta as success. The
# _current table is still written because the app's raw-Lakebase panel and the horizontal
# demo path read it.
fe.write_table(name=RECENT, df=spark.createDataFrame(recomputed), mode="merge")

# The time series row is stamped NOW, not at a day boundary: this is the live-freshness
# path, and a latest-per-key online lookup takes the newest row regardless. Same
# end-of-window convention as the daily snapshots -- the window closes at `now`.
recomputed_ts = recomputed.copy()
recomputed_ts["ts"] = pd.Timestamp(now)
fe.write_table(name=RECENT_TS, df=spark.createDataFrame(recomputed_ts), mode="merge")
print(f"wrote a {pd.Timestamp(now)} snapshot to {RECENT_TS}")

t0 = time.time()
summary = ops.refresh_and_wait(w, ONLINE_RECENT, timeout_s=300)
# The one the endpoints actually read. Refreshed second so the measured wall time below
# covers both syncs rather than hiding one.
summary_ts = ops.refresh_and_wait(w, ONLINE_RECENT_TS, timeout_s=300)
print(f"{ONLINE_RECENT_TS}: {summary_ts.get('detailed_state')}")
sync_wall_s = time.time() - t0

row, cols, ms = store.keyed_read("online_recent_behavior", "viewer_id", VID)
online_after = dict(zip(cols, row))
print(f"\nonline row AFTER ({ms:.0f} ms): {online_after}")

# Assert the thing the whole beat depends on: the online value actually moved.
# Without this the notebook can pass while the endpoint re-reads stale features.
if float(online_after.get("minutes_watched_24h") or 0) == float(online_before.get("minutes_watched_24h") or 0):
    raise RuntimeError(
        "the online row did not change after the refresh: "
        f"before={online_before} after={online_after}. Re-ranking now would compare "
        "identical features and report a delta of zero.")
print(f"triggered refresh took {sync_wall_s:.1f}s wall clock "
      f"(state={summary['detailed_state']}, processed_commit={summary['last_processed_commit_version']})")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Rank the same 25 again — the "after"
# MAGIC
# MAGIC Same viewer, same candidates, same frozen request clock. The only thing that
# MAGIC changed is a value in Lakebase.
# MAGIC
# MAGIC Say:
# MAGIC > "Nothing about the model changed. Nothing about the application changed.
# MAGIC > A feature changed in the online store, and the endpoint picked it up on the
# MAGIC > next request — because the endpoint is the thing doing the lookup."
# COMMAND ----------
after_scores, after_ms = C.query_ranker(w, cfg.ranker_endpoint, records)
after = C.rank(cands, after_scores)

merged = (before[["title_id", "title_name", "primary_genre", "play_start_probability"]]
          .rename(columns={"play_start_probability": "before"})
          .merge(after[["title_id", "play_start_probability"]]
                 .rename(columns={"play_start_probability": "after"}), on="title_id"))
merged["delta"] = (merged["after"] - merged["before"]).round(4)
movers = merged.reindex(merged["delta"].abs().sort_values(ascending=False).index)

print(f"re-ranked in {after_ms:.0f} ms")
print(f"top before: {before.iloc[0]['title_name']}   top after: {after.iloc[0]['title_name']}")
display(spark.createDataFrame(movers.head(10)))
# COMMAND ----------
store.close()
dbutils.notebook.exit(json.dumps({
    "viewer": VID,
    "publish_mode": "TRIGGERED",
    "before_top": str(before.iloc[0]["title_name"]),
    "after_top": str(after.iloc[0]["title_name"]),
    "max_abs_delta": float(merged["delta"].abs().max()),
    "n_moved": int((merged["delta"].abs() > 0.001).sum()),
    "minutes_watched_24h": float(recomputed["minutes_watched_24h"].iloc[0]),
    "last_primary_genre": str(recomputed["last_primary_genre"].iloc[0]),
    "sync_wall_seconds": round(sync_wall_s, 1),
    "query_before_ms": round(before_ms),
    "query_after_ms": round(after_ms),
    "keyed_read_ms": round(ms),
    "online_before": {k: str(v) for k, v in online_before.items()},
    "online_after": {k: str(v) for k, v in online_after.items()},
}))
