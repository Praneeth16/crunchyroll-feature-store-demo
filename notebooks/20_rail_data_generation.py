# Databricks notebook source
# MAGIC %md
# MAGIC # 20 · The homepage rail log — raw signals for vertical ranking
# MAGIC
# MAGIC Horizontal ranking orders titles *inside* a rail. **Vertical ranking orders the
# MAGIC rails themselves**: which row goes first on the Crunchyroll homepage, for this
# MAGIC viewer, on this device, at this hour.
# MAGIC
# MAGIC That needs three things the title-level demo does not have:
# MAGIC
# MAGIC | Table | Grain | Contents |
# MAGIC |---|---|---|
# MAGIC | `rails` | rail_id | The rail catalog: 16 rails, their type, their genre, and the editorial order the current rule-based homepage renders them in |
# MAGIC | `rail_title_map` | rail_id × title_id | Which titles each catalog-driven rail can draw from. Personalized rails resolve per viewer at request time, so they are deliberately absent |
# MAGIC | `rail_impressions` | impression | The homepage render log — one row per rail shown in one homepage session, with the **position it was rendered at**, whether it was in the viewport, and whether the viewer engaged |
# MAGIC | `rail_position_propensity` | rail_position | Empirical P(viewport \| position) and the inverse-propensity weight that undoes it |
# MAGIC
# MAGIC **Why the position column matters.** Every label in a homepage log was
# MAGIC observed at a position the *old* policy chose. Rails at the top get clicked
# MAGIC because they are at the top. Train on that without correction and the model
# MAGIC learns the incumbent policy, not the viewer. `rail_position_propensity` is
# MAGIC how notebook 22 corrects for it, and it is the single most important thing to
# MAGIC get right about vertical ranking on logged data.
# MAGIC
# MAGIC Engagement is generated from a latent viewer × rail utility built out of
# MAGIC genre taste, rail type, device and hour — signal the model can only recover
# MAGIC through the feature tables, never from the row itself.
# COMMAND ----------
import os, sys
_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _root not in sys.path:
    sys.path.insert(0, _root)
from src.crfs.config import Config
from src.crfs import rails as R

cfg = Config.from_widgets(dbutils, extra_widgets={"rail_seed": "4242"})
CATALOG, SCHEMA = cfg.catalog, cfg.schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.fq}")
spark.sql(f"USE {cfg.fq}")
SEED = int(cfg.extras.get("rail_seed") or 4242)
print("target:", cfg.fq, "| seed:", SEED)
# COMMAND ----------
import pandas as pd

# Anchored on the newest event in engagement_events, exactly like every other
# notebook -- so the rail log and the title-level log describe the same 90 days
# and a viewer's rail history lines up with their watch history.
as_of = cfg.demo_now(spark)
titles = spark.table(cfg.t("titles")).toPandas()
viewers = spark.table(cfg.t("viewers")).toPandas()
events = spark.table(cfg.t("engagement_events")).toPandas()
start = pd.Timestamp(as_of).normalize() - pd.Timedelta(days=89)
end = pd.Timestamp(as_of).normalize()
print(f"titles {len(titles)} | viewers {len(viewers)} | events {len(events):,}")
print("rail history window:", start.date(), "->", end.date())
# COMMAND ----------
# MAGIC %md
# MAGIC ## The rail catalog
# MAGIC
# MAGIC `editorial_rank` is the incumbent policy: the fixed order the rule-based
# MAGIC homepage ships today. The learned ranker has to beat it, and the benchmark in
# MAGIC notebook 24 measures whether it does.
# COMMAND ----------
rails_pdf = R.rails_frame()
rail_titles_pdf = R.rail_title_map(titles, rails_pdf, as_of)
print(rails_pdf.to_string(index=False))
print(f"\nrail_title_map: {len(rail_titles_pdf)} rows across "
      f"{rail_titles_pdf['rail_id'].nunique()} catalog-driven rails")
print("personalized rails resolved at request time:",
      sorted(set(rails_pdf.loc[rails_pdf['is_personalized'], 'rail_id'])))
# COMMAND ----------
# MAGIC %md
# MAGIC ## The homepage render log
# COMMAND ----------
import time

t0 = time.perf_counter()
impressions = R.generate_rail_impressions(
    viewers, titles, events, rails_pdf, start=start, end=end, seed=SEED)
gen_s = time.perf_counter() - t0
print(f"{len(impressions):,} rail impressions generated in {gen_s:.1f}s")
print(f"sessions: {impressions['session_id'].nunique():,} | "
      f"viewers: {impressions['viewer_id'].nunique()} | "
      f"viewport rate: {impressions['was_viewport'].mean():.3f} | "
      f"CTR: {impressions['engaged'].mean():.4f}")
# COMMAND ----------
# MAGIC %md
# MAGIC ### Position bias, before any modelling
# MAGIC
# MAGIC This is the confound. CTR falls monotonically with position on rails that
# MAGIC are, by construction, no worse — because fewer viewers ever scroll to them.
# COMMAND ----------
propensity = R.position_propensity(impressions)
print(propensity.to_string(index=False))
print("\nCTR by rail, as logged (contaminated by where the incumbent policy put each rail):")
by_rail = (impressions.groupby("rail_id")
           .agg(impressions=("impression_id", "size"),
                mean_position=("rail_position", "mean"),
                ctr=("engaged", "mean"))
           .sort_values("ctr", ascending=False).round(4))
print(by_rail.to_string())
# COMMAND ----------
# MAGIC %md
# MAGIC ## Write the four tables
# MAGIC
# MAGIC Change Data Feed on all of them: `rail_impressions` is what a retraining job
# MAGIC reads incrementally, and the feature tables built in notebook 21 need CDF on
# MAGIC their own sources to publish incrementally to the online store.
# COMMAND ----------
TBL_PROPS = "TBLPROPERTIES (delta.enableChangeDataFeed = true)"


def write(pdf, name, comment, partition_by=None):
    sdf = spark.createDataFrame(pdf)
    writer = sdf.write.mode("overwrite").option("overwriteSchema", "true").format("delta")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.saveAsTable(cfg.t(name))
    spark.sql(f"ALTER TABLE {cfg.t(name)} SET {TBL_PROPS}")
    spark.sql(f"COMMENT ON TABLE {cfg.t(name)} IS '{comment}'")
    n = spark.table(cfg.t(name)).count()
    print(f"  {name:28s} {n:>9,} rows")
    return n


print("writing:")
write(rails_pdf, "rails",
      "Homepage rail catalog. editorial_rank is the incumbent rule-based order the "
      "learned vertical ranker has to beat.")
write(rail_titles_pdf, "rail_title_map",
      "Titles each catalog-driven rail can draw from. Personalized rails are absent "
      "by design - they resolve per viewer at request time.")
write(impressions, "rail_impressions",
      "Homepage render log: one row per rail shown in one homepage session, with the "
      "position it was rendered at. Labels here are position-biased; join "
      "rail_position_propensity before training on them.")
write(propensity, "rail_position_propensity",
      "Empirical P(viewport | rail_position) and the clipped inverse-propensity weight "
      "that corrects the homepage log for position bias.")
# COMMAND ----------
# MAGIC %md
# MAGIC ## Primary keys
# MAGIC
# MAGIC Declared so Unity Catalog lineage and the feature-store lookups have real
# MAGIC constraints to resolve against, not just conventions.
# COMMAND ----------
for tbl, cols in [("rails", "rail_id"),
                  ("rail_title_map", "rail_id, title_id"),
                  ("rail_impressions", "impression_id"),
                  ("rail_position_propensity", "rail_position")]:
    for c in cols.split(", "):
        spark.sql(f"ALTER TABLE {cfg.t(tbl)} ALTER COLUMN {c} SET NOT NULL")
    spark.sql(f"ALTER TABLE {cfg.t(tbl)} DROP PRIMARY KEY IF EXISTS CASCADE")
    spark.sql(f"ALTER TABLE {cfg.t(tbl)} ADD CONSTRAINT {tbl}_pk PRIMARY KEY ({cols})")
    print(f"  {tbl}: PK ({cols})")
# COMMAND ----------
import json

summary = {
    "rails": int(len(rails_pdf)),
    "rail_title_map_rows": int(len(rail_titles_pdf)),
    "impressions": int(len(impressions)),
    "sessions": int(impressions["session_id"].nunique()),
    "viewport_rate": round(float(impressions["was_viewport"].mean()), 4),
    "ctr": round(float(impressions["engaged"].mean()), 4),
    # Named for what it holds: p_view is P(viewport | position), not a click-through
    # rate. The old key was "ctr_position_1", which invites reading ~0.97 as a 97% CTR.
    "viewport_prob_position_1": float(propensity.loc[propensity.rail_position == 1, "p_view"].iloc[0]),
    "max_ips_weight": float(propensity["ips_weight"].max()),
    "window": [str(start.date()), str(end.date())],
    "generation_seconds": round(gen_s, 1),
}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
