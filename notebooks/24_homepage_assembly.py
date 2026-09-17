# Databricks notebook source
# MAGIC %md
# MAGIC # 24 · A whole homepage, from two models on one feature store
# MAGIC
# MAGIC This is the notebook that answers the actual ask: **do Feature Store and Model
# MAGIC Serving work together as shared infrastructure for more than one recommendation
# MAGIC model**, rather than as two capabilities evaluated separately.
# MAGIC
# MAGIC A Crunchyroll homepage is built by two rankers in sequence:
# MAGIC
# MAGIC 1. **Vertical** — `crunchyroll-rail-ranker` orders the rails. One request, one
# MAGIC    row per eligible rail, ranked rails back.
# MAGIC 2. **Horizontal** — `crunchyroll-watch-next-ranker` orders the titles inside
# MAGIC    each rail that is going to be rendered.
# MAGIC
# MAGIC Both endpoints do their own feature lookups against **the same Lakebase online
# MAGIC store**, and the overlap is not a diagram — it is checked here against Unity
# MAGIC Catalog and printed.
# MAGIC
# MAGIC The last section is the one to run twice in a demo: the same viewer at 09:00 on
# MAGIC a phone and at 21:00 on a TV. Nothing in the feature store changes between those
# MAGIC two calls. The rail order changes anyway, because four of the five request-time
# MAGIC features read a value that only exists in the request (device, hour_of_day or
# MAGIC request_epoch_s). The fifth, rail_taste_match, is a viewer x rail cross that
# MAGIC cannot be precomputed at scale.
# COMMAND ----------
import os, sys
_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _root not in sys.path:
    sys.path.insert(0, _root)

from src.crfs.config import Config
from src.crfs import config as C_CFG
from src.crfs import rails as R
from src.crfs import candidates as C

cfg = Config.from_widgets(dbutils, extra_widgets={
    "viewer_id": "",                  # blank -> the busiest viewer
    "rails_to_fill": "3",
    "titles_per_rail": "12",
})
spark.sql(f"USE {cfg.fq}")
RAILS_TO_FILL = int(cfg.extras["rails_to_fill"])
TITLES_PER_RAIL = int(cfg.extras["titles_per_rail"])
print(cfg.describe())
# COMMAND ----------
import json, time
import pandas as pd
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

viewer = cfg.extras.get("viewer_id") or spark.sql(f"""
    SELECT viewer_id FROM {cfg.t('viewer_rail_features_ts')}
    GROUP BY viewer_id ORDER BY SUM(vr_impressions_30d) DESC LIMIT 1
""").first()["viewer_id"]
profile = spark.sql(f"SELECT * FROM {cfg.t('viewers')} WHERE viewer_id = '{viewer}'").first().asDict()
print("viewer:", json.dumps(profile, indent=2, default=str))
# COMMAND ----------
# MAGIC %md
# MAGIC ## The feature layer both models read
# MAGIC
# MAGIC Resolved from Unity Catalog, not from a slide. `online_*` tables are the
# MAGIC published Lakebase copies the endpoints look up at request time.
# COMMAND ----------
online_tables = spark.sql(f"""
    SHOW TABLES IN {cfg.fq} LIKE 'online_*'
""").toPandas()["tableName"].tolist()

# Derived from the FeatureLookup declarations the two trainers use (src/crfs/config.py),
# not typed again here. The previous version kept a parallel dict in this notebook that
# could disagree with the models without anyone noticing.
READERS = C_CFG.online_readers()
rows = []
for t in sorted(online_tables):
    n = spark.table(cfg.t(t)).count()
    readers = READERS.get(t, ["-"])
    rows.append({"online_table": t, "rows": n, "read_by": ", ".join(readers),
                 "shared": "YES" if len(readers) > 1 else ""})
overlap = pd.DataFrame(rows)
print(overlap.to_string(index=False))
shared_n = int((overlap["shared"] == "YES").sum())
print(f"\n{shared_n} of {len(overlap)} published online tables are read by both rankers. "
      f"Neither model owns a private copy of a viewer feature.")
print("Table list resolved from Unity Catalog (SHOW TABLES); the reader mapping is "
      "derived from the FeatureLookup declarations in src/crfs/config.py, which is what "
      "notebooks 02 and 22 train against -- not from the deployed models' own feature "
      "specs, so it can drift if someone retrains with different lookups.")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1 · Vertical — which rails, in what order
# MAGIC
# MAGIC Eligibility runs first and is a hard filter, not a feature: a viewer with
# MAGIC nothing in progress is not offered Continue Watching, and no model score can
# MAGIC overrule that. Same discipline as the entitlement join on the horizontal side.
# COMMAND ----------
eligible = R.eligible_rails(spark, cfg.fq, viewer)
print(f"{len(eligible)} of 16 rails are eligible for {viewer}")
print(eligible[["rail_id", "rail_name", "rail_type", "editorial_rank"]].to_string(index=False))

records = R.rail_request_records(viewer, eligible["rail_id"].tolist(),
                                 device="tv", locale="en-US", hour_of_day=21)
t0 = time.perf_counter()
resp = w.serving_endpoints.query(name=cfg.rail_ranker_endpoint, dataframe_records=records)
vertical_ms = (time.perf_counter() - t0) * 1000.0
ranked_rails = pd.DataFrame(list(resp.predictions or [])).sort_values("rail_rank")
ranked_rails = ranked_rails.merge(
    eligible[["rail_id", "rail_name", "rail_type", "editorial_rank"]], on="rail_id", how="left")
# Same correction as notebooks 23 and the app: compare like with like. rail_rank is
# dense over the eligible rails; editorial_rank is catalog-wide. This value is also
# returned in dbutils.notebook.exit, so an inflated one propagates to consumers.
ranked_rails["moved"] = (
    ranked_rails["editorial_rank"].astype(int).rank(method="first").astype(int)
    - ranked_rails["rail_rank"].astype(int))
print(f"\n{len(ranked_rails)} rails ranked in one request: {vertical_ms:.0f} ms")
print(ranked_rails[["rail_rank", "rail_name", "rail_type", "engagement_probability",
                    "editorial_rank", "moved"]].to_string(index=False))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2 · Horizontal — the titles inside the top rails
# MAGIC
# MAGIC Catalog-driven rails draw from `rail_title_map`; personalized rails resolve per
# MAGIC viewer, which is exactly why they carry no static membership. Either way the
# MAGIC candidate list goes to the watch-next ranker, which does its own lookups against
# MAGIC the same online store the rail ranker just used.
# COMMAND ----------
def rail_candidates(rail_id: str, rail_type: str, limit: int):
    """Candidate titles for one rail. Entitlement-filtered in every branch.

    Branches on rail_id against R.STATE_DEPENDENT, not on rail_type: r_simulcast and
    r_new_eps are both rail_type "new_release", but only r_new_eps is personalized.
    Branching on the type would have resolved This Season's Simulcasts from the
    viewer's own watch history, which is the opposite of what that rail is.
    R.STATE_DEPENDENT is the single definition of which rails are viewer-specific.
    """
    if rail_id in R.STATE_DEPENDENT:
        # Personalized rails: from this viewer's own history, entitlement-filtered.
        sql = f"""
            SELECT t.title_id, t.title_name, t.primary_genre
            FROM {cfg.t('titles')} t
            JOIN {cfg.t('entitlements')} e
              ON e.title_id = t.title_id AND e.viewer_id = '{viewer}' AND e.allowed
            JOIN (SELECT title_id, MAX(event_ts) AS last_ts
                  FROM {cfg.t('engagement_events')}
                  WHERE viewer_id = '{viewer}' AND watch_seconds > 0
                  GROUP BY title_id) h ON h.title_id = t.title_id
            {"WHERE t.is_simulcast" if rail_type == "new_release" else ""}
            ORDER BY h.last_ts DESC
            LIMIT {limit}
        """
    else:
        sql = f"""
            SELECT t.title_id, t.title_name, t.primary_genre
            FROM {cfg.t('rail_title_map')} m
            JOIN {cfg.t('titles')} t ON t.title_id = m.title_id
            JOIN {cfg.t('entitlements')} e
              ON e.title_id = t.title_id AND e.viewer_id = '{viewer}' AND e.allowed
            WHERE m.rail_id = '{rail_id}'
            ORDER BY m.rank_in_rail
            LIMIT {limit}
        """
    return spark.sql(sql).toPandas()


homepage, horizontal_ms_total = [], 0.0
for _, rail in ranked_rails.head(RAILS_TO_FILL).iterrows():
    cands = rail_candidates(rail["rail_id"], rail["rail_type"], TITLES_PER_RAIL)
    if cands.empty:
        homepage.append({"rail": rail["rail_name"], "titles": [], "ms": 0.0,
                         "note": "no entitled candidates"})
        continue
    recs = C.request_records(viewer, cands["title_id"].tolist(), surface="home_rail",
                             device="tv", locale="en-US", hour_of_day=21)
    scores, ms = C.query_ranker(w, cfg.ranker_endpoint, recs)
    horizontal_ms_total += ms
    ordered = C.rank(cands, [s.get("played", s) if isinstance(s, dict) else s for s in scores])
    homepage.append({"rail": rail["rail_name"], "rail_rank": int(rail["rail_rank"]),
                     "titles": ordered["title_name"].tolist(), "ms": ms})

print(f"the homepage {viewer} would see at 21:00 on a TV\n")
for row in homepage:
    print(f"  {row.get('rail_rank', '-')}. {row['rail']}   ({row['ms']:.0f} ms)")
    for t in row["titles"][:6]:
        print(f"       · {t}")
    if not row["titles"]:
        print(f"       ({row.get('note', 'empty')})")
    print()
print(f"assembly: 1 vertical call ({vertical_ms:.0f} ms) + {RAILS_TO_FILL} horizontal "
      f"calls ({horizontal_ms_total:.0f} ms total) = {vertical_ms + horizontal_ms_total:.0f} ms")
print("In production the horizontal calls are independent and would be issued in "
      "parallel, so wall time is the vertical call plus the slowest horizontal one. "
      "Notebook 25 measures each endpoint under real concurrency.")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3 · The same viewer, four different contexts
# MAGIC
# MAGIC Nothing in the feature store changes between these four calls. Only the request
# MAGIC does. If the rail order were identical across all four, the request-time UDFs
# MAGIC would be dead weight and the whole on-demand feature layer would be
# MAGIC unjustifiable.
# COMMAND ----------
CONTEXTS = [
    ("21:00, TV",     dict(device="tv",     hour_of_day=21)),
    ("09:00, TV",     dict(device="tv",     hour_of_day=9)),
    ("21:00, mobile", dict(device="mobile", hour_of_day=21)),
    ("09:00, mobile", dict(device="mobile", hour_of_day=9)),
]

orders, latencies = {}, {}
for label, kw in CONTEXTS:
    recs = R.rail_request_records(viewer, eligible["rail_id"].tolist(),
                                 locale="en-US", **kw)
    t0 = time.perf_counter()
    r = w.serving_endpoints.query(name=cfg.rail_ranker_endpoint, dataframe_records=recs)
    latencies[label] = (time.perf_counter() - t0) * 1000.0
    df = pd.DataFrame(list(r.predictions or [])).sort_values("rail_rank")
    orders[label] = df.set_index("rail_id")["rail_rank"].to_dict()

names = eligible.set_index("rail_id")["rail_name"].to_dict()
compare = pd.DataFrame(orders)
compare.insert(0, "rail", [names.get(i, i) for i in compare.index])
compare = compare.sort_values(CONTEXTS[0][0])
print(compare.to_string(index=False))

first = compare[CONTEXTS[0][0]]
moved_any = 0
for label, _ in CONTEXTS[1:]:
    diff = int((compare[label] != first).sum())
    moved_any = max(moved_any, diff)
    print(f"\n{label:16s} moves {diff} of {len(compare)} rails vs {CONTEXTS[0][0]}")
print(f"\nlatency per context: " +
      " | ".join(f"{k} {v:.0f} ms" for k, v in latencies.items()))
if moved_any == 0:
    print("\nWARNING: no rail moved across any context. Either the on-demand features "
          "are not reaching the model or the request columns are not varying - "
          "investigate before showing this to anyone.")
# COMMAND ----------
# MAGIC %md
# MAGIC ## What the request actually carried
# MAGIC
# MAGIC Seven fields. Every feature value the endpoint resolved -- 45 of them, across four
# MAGIC tables and five UDFs -- was retrieved by the endpoint, not sent by the caller.
# MAGIC That is the property that makes training-serving consistency structural instead
# MAGIC of a code-review rule.
# COMMAND ----------
print(json.dumps(records[0], indent=2))
print(f"\nfields in the request: {len(records[0])}")
print("request keys the contract defines:", R.RAIL_REQUEST_KEYS)
# COMMAND ----------
dbutils.notebook.exit(json.dumps({
    "viewer": viewer,
    "eligible_rails": int(len(eligible)),
    "vertical_ms": round(vertical_ms, 1),
    "horizontal_ms_total": round(horizontal_ms_total, 1),
    "rails_filled": RAILS_TO_FILL,
    "online_tables": rows,
    "shared_online_tables": shared_n,
    "ranked_rails": ranked_rails[["rail_rank", "rail_id", "engagement_probability",
                                  "editorial_rank", "moved"]].to_dict(orient="records"),
    "context_sensitivity": {"max_rails_moved": int(moved_any),
                            "latency_ms": {k: round(v, 1) for k, v in latencies.items()}},
    "request_fields": len(records[0]),
}, default=str))
