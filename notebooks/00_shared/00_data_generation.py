# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Synthetic Crunchyroll data
# MAGIC
# MAGIC Builds the raw signals the whole demo stands on, in
# MAGIC a Unity Catalog schema `crunchyroll_demo` (set the `catalog` widget):
# MAGIC
# MAGIC | Table | Grain | Contents |
# MAGIC |---|---|---|
# MAGIC | `titles` | title_id | Catalog metadata: genre, franchise, maturity, episodes, release date |
# MAGIC | `viewers` | viewer_id | Profile: country, language, tier, age bracket |
# MAGIC | `entitlements` | viewer_id × title_id | Territory / subscription hard-filter flag |
# MAGIC | `engagement_events` | event | impressions (with play-outcome label), skips, completes |
# MAGIC | `engagement_events_stream` | event | empty on creation; the live feed notebooks 10/11 use |
# MAGIC
# MAGIC Events are generated with latent genre affinities, so the ranking model in
# MAGIC notebook 02 has real signal to learn — affinity match, popularity and
# MAGIC recency all move the play probability.
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

cfg = Config.from_widgets(dbutils)
CATALOG, SCHEMA = cfg.catalog, cfg.schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.fq}")
spark.sql(f"USE {cfg.fq}")
print("target:", cfg.fq)
print(cfg.describe())
# COMMAND ----------
import random, math, datetime as dt
import pandas as pd
import numpy as np

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

DAYS = 90
# History ends yesterday unless the end_date widget pins it. The first version of
# this demo hardcoded 2026-08-31, so by the time it was presented the "last 24h"
# online features were a week older than the wall clock the freshness demo used.
END_DATE = cfg.end_date_resolved
START_DATE = END_DATE - dt.timedelta(days=DAYS - 1)
print("history window:", START_DATE, "->", END_DATE)

GENRES = ["action", "adventure", "fantasy", "sci_fi", "sports", "drama", "romance", "slice_of_life"]
MATURITY = ["all", "13+", "16+", "18+"]
TIERS = ["fan", "mega_fan", "ultimate_fan"]
DEVICES = ["tv", "mobile", "web", "console"]
SURFACES = ["post_play", "home_rail", "search", "watchlist"]
LOCALES = ["en-US", "es-MX", "pt-BR", "fr-FR", "de-DE", "ja-JP"]
# COMMAND ----------
# MAGIC %md
# MAGIC ## Titles — a recognizable anime catalog
# COMMAND ----------
CORE_TITLES = [
    # (name, franchise, primary_genre, secondary, maturity, episodes, release_year, simulcast)
    ("Attack on Titan", "Attack on Titan", "action", "drama", "16+", 89, 2013, False),
    ("Attack on Titan: Final Season", "Attack on Titan", "action", "drama", "16+", 35, 2020, False),
    ("One Piece", "One Piece", "adventure", "action", "13+", 1100, 1999, True),
    ("Jujutsu Kaisen", "Jujutsu Kaisen", "action", "fantasy", "16+", 47, 2020, False),
    ("Jujutsu Kaisen Season 2", "Jujutsu Kaisen", "action", "fantasy", "16+", 23, 2023, True),
    ("Demon Slayer", "Demon Slayer", "action", "fantasy", "16+", 55, 2019, False),
    ("Demon Slayer: Swordsmith Village", "Demon Slayer", "action", "fantasy", "16+", 11, 2023, True),
    ("Chainsaw Man", "Chainsaw Man", "action", "fantasy", "18+", 12, 2022, True),
    ("Spy x Family", "Spy x Family", "slice_of_life", "action", "13+", 37, 2022, True),
    ("Frieren: Beyond Journey's End", "Frieren", "fantasy", "adventure", "13+", 28, 2023, True),
    ("Solo Leveling", "Solo Leveling", "action", "fantasy", "16+", 25, 2024, True),
    ("Solo Leveling Season 2", "Solo Leveling", "action", "fantasy", "16+", 13, 2025, True),
    ("Dan Da Dan", "Dan Da Dan", "sci_fi", "action", "16+", 24, 2024, True),
    ("Dan Da Dan Season 2", "Dan Da Dan", "sci_fi", "action", "16+", 12, 2025, True),
    ("Vinland Saga", "Vinland Saga", "drama", "action", "18+", 48, 2019, False),
    ("Vinland Saga Season 2", "Vinland Saga", "drama", "action", "18+", 24, 2023, True),
    ("Mob Psycho 100", "Mob Psycho 100", "action", "sci_fi", "13+", 37, 2016, False),
    ("My Hero Academia", "My Hero Academia", "action", "adventure", "13+", 159, 2016, True),
    ("Haikyuu!!", "Haikyuu!!", "sports", "drama", "all", 85, 2014, False),
    ("Blue Lock", "Blue Lock", "sports", "drama", "13+", 38, 2022, True),
    ("Blue Lock Season 2", "Blue Lock", "sports", "drama", "13+", 14, 2024, True),
    ("Death Note", "Death Note", "drama", "fantasy", "16+", 37, 2006, False),
    ("Fullmetal Alchemist: Brotherhood", "Fullmetal Alchemist", "adventure", "fantasy", "16+", 64, 2009, False),
    ("Tokyo Revengers", "Tokyo Revengers", "action", "drama", "16+", 50, 2021, True),
    ("Black Clover", "Black Clover", "fantasy", "action", "13+", 170, 2017, False),
    ("Dr. Stone", "Dr. Stone", "sci_fi", "adventure", "13+", 70, 2019, True),
    ("Hunter x Hunter (2011)", "Hunter x Hunter", "adventure", "action", "13+", 148, 2011, False),
    ("One Punch Man", "One Punch Man", "action", "sci_fi", "16+", 24, 2015, False),
    ("One Punch Man Season 3", "One Punch Man", "action", "sci_fi", "16+", 12, 2025, True),
    ("That Time I Got Reincarnated as a Slime", "Slime", "fantasy", "adventure", "13+", 72, 2018, True),
    ("Mushoku Tensei", "Mushoku Tensei", "fantasy", "drama", "18+", 47, 2021, True),
    ("Re:Zero", "Re:Zero", "fantasy", "drama", "16+", 66, 2016, True),
    ("Sword Art Online", "Sword Art Online", "sci_fi", "fantasy", "16+", 96, 2012, False),
    ("Naruto", "Naruto", "action", "adventure", "13+", 220, 2002, False),
    ("Naruto Shippuden", "Naruto", "action", "adventure", "13+", 500, 2007, False),
    ("Boruto", "Naruto", "action", "adventure", "13+", 293, 2017, True),
    ("Dragon Ball Super", "Dragon Ball", "action", "adventure", "13+", 131, 2015, False),
    ("Dragon Ball Daima", "Dragon Ball", "action", "adventure", "all", 20, 2024, True),
    ("Bleach: Thousand-Year Blood War", "Bleach", "action", "fantasy", "16+", 40, 2022, True),
    ("Frieren Season 2", "Frieren", "fantasy", "adventure", "13+", 12, 2026, True),
    ("Kaiju No. 8", "Kaiju No. 8", "action", "sci_fi", "16+", 24, 2024, True),
    ("Kaiju No. 8 Season 2", "Kaiju No. 8", "action", "sci_fi", "16+", 12, 2025, True),
    ("The Apothecary Diaries", "Apothecary Diaries", "drama", "romance", "16+", 48, 2023, True),
    ("Frieren: A New Journey", "Frieren", "fantasy", "drama", "13+", 6, 2026, True),
    ("Delicious in Dungeon", "Delicious in Dungeon", "fantasy", "adventure", "13+", 24, 2024, False),
    ("Ranking of Kings", "Ranking of Kings", "fantasy", "adventure", "13+", 23, 2021, False),
    ("Violet Evergarden", "Violet Evergarden", "drama", "romance", "13+", 13, 2018, False),
    ("Your Lie in April", "Your Lie in April", "drama", "romance", "13+", 22, 2014, False),
    ("Horimiya", "Horimiya", "romance", "slice_of_life", "13+", 26, 2021, False),
    ("Kaguya-sama: Love is War", "Kaguya-sama", "romance", "slice_of_life", "13+", 41, 2019, False),
    ("My Dress-Up Darling", "My Dress-Up Darling", "romance", "slice_of_life", "16+", 24, 2022, True),
    ("Fruits Basket (2019)", "Fruits Basket", "romance", "drama", "13+", 63, 2019, False),
    ("The Angel Next Door", "Angel Next Door", "romance", "slice_of_life", "13+", 12, 2023, False),
    ("Rent-a-Girlfriend", "Rent-a-Girlfriend", "romance", "drama", "16+", 48, 2020, True),
    ("Oshi no Ko", "Oshi no Ko", "drama", "fantasy", "16+", 24, 2023, True),
    ("Zom 100", "Zom 100", "adventure", "slice_of_life", "16+", 12, 2023, False),
    ("Heavenly Delusion", "Heavenly Delusion", "sci_fi", "adventure", "18+", 13, 2023, False),
    ("Pluto", "Pluto", "sci_fi", "drama", "18+", 8, 2023, False),
    ("Cowboy Bebop", "Cowboy Bebop", "sci_fi", "action", "16+", 26, 1998, False),
    ("Neon Genesis Evangelion", "Evangelion", "sci_fi", "drama", "16+", 26, 1995, False),
    ("Ghost in the Shell: SAC", "Ghost in the Shell", "sci_fi", "action", "18+", 52, 2002, False),
    ("Psycho-Pass", "Psycho-Pass", "sci_fi", "drama", "18+", 41, 2012, False),
    ("Steins;Gate", "Steins;Gate", "sci_fi", "drama", "16+", 24, 2011, False),
    ("Code Geass", "Code Geass", "sci_fi", "drama", "18+", 50, 2006, False),
    ("86 -Eighty Six-", "86", "sci_fi", "drama", "16+", 23, 2021, False),
    ("Cyberpunk: Edgerunners", "Edgerunners", "sci_fi", "action", "18+", 10, 2022, False),
    ("Ping Pong the Animation", "Ping Pong", "sports", "drama", "16+", 11, 2014, False),
    ("Run with the Wind", "Run with the Wind", "sports", "drama", "13+", 23, 2018, False),
    ("Yuri on Ice", "Yuri on Ice", "sports", "romance", "13+", 12, 2016, False),
    ("Kuroko's Basketball", "Kuroko's Basketball", "sports", "drama", "13+", 75, 2012, False),
    ("Slam Dunk", "Slam Dunk", "sports", "drama", "all", 101, 1993, False),
    ("March Comes in Like a Lion", "March Lion", "drama", "slice_of_life", "13+", 44, 2016, False),
    ("A Silent Voice: The Movie", "A Silent Voice", "drama", "romance", "16+", 1, 2016, False),
    ("Weathering with You", "Weathering", "drama", "fantasy", "13+", 1, 2019, False),
    ("Your Name", "Your Name", "drama", "fantasy", "13+", 1, 2016, False),
    ("Spirited Away", "Ghibli", "fantasy", "adventure", "all", 1, 2001, False),
    ("Made in Abyss", "Made in Abyss", "fantasy", "adventure", "18+", 25, 2017, True),
    ("The Promised Neverland", "Promised Neverland", "fantasy", "drama", "16+", 23, 2019, False),
    ("Fire Force", "Fire Force", "action", "fantasy", "16+", 48, 2019, True),
    ("Fire Force Season 3", "Fire Force", "action", "fantasy", "16+", 24, 2025, True),
    ("Black Butler", "Black Butler", "fantasy", "drama", "16+", 65, 2008, True),
    ("JoJo's Bizarre Adventure", "JoJo", "action", "adventure", "16+", 190, 2012, True),
    ("Gintama", "Gintama", "action", "slice_of_life", "16+", 367, 2006, False),
    ("Assassination Classroom", "Assassination Classroom", "action", "sci_fi", "13+", 47, 2015, False),
    ("The Rising of the Shield Hero", "Shield Hero", "fantasy", "adventure", "16+", 50, 2019, True),
    ("Overlord", "Overlord", "fantasy", "action", "18+", 52, 2015, True),
    ("Konosuba", "Konosuba", "fantasy", "adventure", "16+", 31, 2016, True),
    ("No Game No Life", "No Game No Life", "fantasy", "adventure", "16+", 12, 2014, False),
    ("Log Horizon", "Log Horizon", "fantasy", "adventure", "13+", 62, 2013, True),
    ("Is It Wrong to Try to Pick Up Girls in a Dungeon?", "DanMachi", "fantasy", "adventure", "16+", 77, 2015, True),
    ("Goblin Slayer", "Goblin Slayer", "fantasy", "action", "18+", 24, 2018, True),
    ("The Eminence in Shadow", "Eminence in Shadow", "fantasy", "action", "16+", 32, 2022, True),
    ("Frieren Specials", "Frieren", "fantasy", "slice_of_life", "all", 4, 2024, False),
    ("Mashle", "Mashle", "action", "fantasy", "13+", 24, 2023, True),
    ("Undead Unluck", "Undead Unluck", "action", "fantasy", "16+", 24, 2023, False),
    ("Hell's Paradise", "Hell's Paradise", "action", "fantasy", "18+", 13, 2023, True),
    ("Hell's Paradise Season 2", "Hell's Paradise", "action", "fantasy", "18+", 12, 2026, True),
    ("Wind Breaker", "Wind Breaker", "action", "sports", "16+", 25, 2024, True),
    ("Blue Box", "Blue Box", "sports", "romance", "13+", 25, 2024, True),
    ("Sakamoto Days", "Sakamoto Days", "action", "slice_of_life", "16+", 22, 2025, True),
    ("Witch Watch", "Witch Watch", "fantasy", "slice_of_life", "13+", 25, 2025, True),
    ("To Be Hero X", "To Be Hero X", "action", "sci_fi", "16+", 24, 2025, True),
    ("Gachiakuta", "Gachiakuta", "action", "fantasy", "16+", 24, 2025, True),
    ("The Fragrant Flower Blooms with Dignity", "Fragrant Flower", "romance", "drama", "13+", 13, 2025, True),
    ("Call of the Night Season 2", "Call of the Night", "romance", "fantasy", "16+", 12, 2025, True),
    ("Rent-a-Girlfriend Season 4", "Rent-a-Girlfriend", "romance", "drama", "16+", 12, 2025, True),
    ("Kaiju No. 8: Mission Recon", "Kaiju No. 8", "action", "sci_fi", "16+", 1, 2025, False),
    ("The Beginning After the End", "TBATE", "fantasy", "action", "13+", 12, 2025, True),
    ("Lazarus", "Lazarus", "sci_fi", "action", "18+", 13, 2025, True),
    ("Moonrise", "Moonrise", "sci_fi", "drama", "16+", 18, 2025, True),
    ("Apocalypse Hotel", "Apocalypse Hotel", "sci_fi", "slice_of_life", "13+", 12, 2025, True),
    ("Grand Blue Season 2", "Grand Blue", "slice_of_life", "drama", "16+", 12, 2025, True),
    ("Rascal Does Not Dream: Bunny Girl Senpai", "Rascal", "romance", "drama", "16+", 13, 2018, False),
    ("Toradora!", "Toradora!", "romance", "drama", "13+", 25, 2008, False),
    ("Clannad: After Story", "Clannad", "romance", "drama", "13+", 24, 2008, False),
    ("Anohana", "Anohana", "drama", "romance", "13+", 11, 2011, False),
    ("Erased", "Erased", "drama", "sci_fi", "16+", 12, 2016, False),
    ("Monster", "Monster", "drama", "drama", "18+", 74, 2004, False),
    ("Parasyte", "Parasyte", "sci_fi", "action", "18+", 24, 2014, False),
    ("Tokyo Ghoul", "Tokyo Ghoul", "action", "drama", "18+", 48, 2014, False),
    ("Seven Deadly Sins", "Seven Deadly Sins", "fantasy", "action", "16+", 100, 2014, False),
    ("Fairy Tail", "Fairy Tail", "fantasy", "adventure", "13+", 328, 2009, False),
    ("Hajime no Ippo", "Hajime no Ippo", "sports", "drama", "13+", 126, 2000, False),
    ("Ace of Diamond", "Ace of Diamond", "sports", "drama", "13+", 178, 2013, True),
    ("Days", "Days", "sports", "drama", "13+", 24, 2016, False),
    ("Chihayafuru", "Chihayafuru", "drama", "sports", "13+", 74, 2011, False),
    ("Welcome to Demon School! Iruma-kun", "Iruma-kun", "fantasy", "slice_of_life", "13+", 65, 2019, True),
    ("The Ancient Magus' Bride", "Magus Bride", "fantasy", "romance", "16+", 48, 2017, True),
    ("Natsume's Book of Friends", "Natsume", "fantasy", "slice_of_life", "13+", 86, 2008, True),
    ("Barakamon", "Barakamon", "slice_of_life", "drama", "all", 12, 2014, False),
    ("Sweetness & Lightning", "Sweetness", "slice_of_life", "drama", "all", 12, 2016, False),
    ("Laid-Back Camp", "Laid-Back Camp", "slice_of_life", "slice_of_life", "all", 37, 2018, True),
]

titles = []
for i, (name, fran, g1, g2, mat, eps, year, simul) in enumerate(CORE_TITLES):
    base_pop = 1.0
    if simul and year >= 2024:
        base_pop = np.random.uniform(0.7, 1.0)
    elif year >= 2020:
        base_pop = np.random.uniform(0.4, 0.9)
    else:
        base_pop = np.random.uniform(0.1, 0.8)
    titles.append({
        "title_id": f"t{i+1:04d}",
        "title_name": name,
        "franchise": fran,
        "primary_genre": g1,
        "secondary_genre": g2,
        "maturity_rating": mat,
        "episode_count": int(eps),
        "release_year": int(year),
        "is_simulcast": bool(simul),
        "intrinsic_popularity": round(float(base_pop), 4),
        "avg_rating": round(float(np.clip(np.random.normal(4.0, 0.5), 2.5, 5.0)), 2),
    })
titles_df = pd.DataFrame(titles)
print("titles:", len(titles_df))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Viewers — profiles with latent genre affinities
# COMMAND ----------
AGE_BRACKETS = ["13-17", "18-24", "25-34", "35-44", "45+"]
MATURITY_RANK = {"all": 0, "13+": 1, "16+": 2, "18+": 3}

def allowed_maturity(age_bracket):
    return {"13-17": 1, "18-24": 3, "25-34": 3, "35-44": 3, "45+": 3}[age_bracket]

viewers = []
for v in range(300):
    vid = f"v{v+1:04d}"
    age = np.random.choice(AGE_BRACKETS, p=[0.08, 0.4, 0.3, 0.15, 0.07])
    # latent affinity vector over genres: each viewer has 1-2 strong genres
    affinity = {g: float(np.random.exponential(0.15)) for g in GENRES}
    strong = np.random.choice(GENRES, size=np.random.choice([1, 2], p=[0.6, 0.4]), replace=False)
    for g in strong:
        affinity[g] += float(np.random.uniform(0.6, 1.0))
    viewers.append({
        "viewer_id": vid,
        "country": str(np.random.choice(["US", "BR", "MX", "FR", "DE", "IN", "JP"], p=[0.35, 0.1, 0.1, 0.08, 0.07, 0.2, 0.1])),
        "language": str(np.random.choice(["en", "es", "pt", "fr", "de", "ja"], p=[0.5, 0.12, 0.1, 0.08, 0.1, 0.1])),
        "tier": str(np.random.choice(TIERS, p=[0.5, 0.35, 0.15])),
        "age_bracket": age,
        "signup_date": str(END_DATE - dt.timedelta(days=int(np.random.uniform(30, 1200)))),
        "activity_level": round(float(np.random.uniform(0.15, 0.95)), 3),
        "_max_maturity": allowed_maturity(age),
        "_affinity": affinity,
    })
# COMMAND ----------
# MAGIC %md
# MAGIC ## Engagement events — 90 days of impressions, plays, skips, completes
# COMMAND ----------
title_by_id = {t["title_id"]: t for t in titles}
title_ids = list(title_by_id.keys())

events = []
eid = 0
day_list = [START_DATE + dt.timedelta(days=d) for d in range(DAYS)]

for viewer in viewers:
    aff = viewer["_affinity"]
    act = viewer["activity_level"]
    locale = {"US": "en-US", "BR": "pt-BR", "MX": "es-MX", "FR": "fr-FR", "DE": "de-DE", "IN": "en-US", "JP": "ja-JP"}[viewer["country"]]
    for day in day_list:
        if random.random() > act * 0.5:
            continue
        n_sessions = 1 + (1 if random.random() < act * 0.5 else 0)
        for s in range(n_sessions):
            session_id = f"{viewer['viewer_id']}-{day.isoformat()}-{s}"
            surface = str(np.random.choice(SURFACES, p=[0.4, 0.3, 0.2, 0.1]))
            device = str(np.random.choice(DEVICES, p=[0.45, 0.3, 0.2, 0.05]))
            hour = int(np.random.choice(range(24), p=[0.01,0.005,0.003,0.002,0.002,0.005,0.01,0.02,0.03,0.04,0.05,0.05,0.06,0.06,0.06,0.05,0.05,0.06,0.07,0.08,0.09,0.09,0.07,0.033]))
            ts_base = dt.datetime.combine(day, dt.time(hour=hour, minute=int(random.random()*60)))
            # candidate set: 5-9 titles, eligibility-filtered by maturity
            eligible = [t for t in titles if MATURITY_RANK[t["maturity_rating"]] <= viewer["_max_maturity"]]
            # bias candidate generation toward affinity (like a real retrieval stage)
            weighted = sorted(eligible, key=lambda t: -(aff.get(t["primary_genre"], 0) + aff.get(t["secondary_genre"], 0) * 0.5 + t["intrinsic_popularity"] * 0.4 + random.random() * 0.8))
            candidates = weighted[: int(np.random.randint(5, 10))]
            for pos, cand in enumerate(candidates):
                g_match = max(aff.get(cand["primary_genre"], 0), aff.get(cand["secondary_genre"], 0) * 0.6)
                recency_boost = 0.5 if cand["release_year"] >= 2024 else 0.0
                simul_boost = 0.3 if cand["is_simulcast"] else 0.0
                logit = (2.2 * g_match
                         + 1.1 * cand["intrinsic_popularity"]
                         + recency_boost + simul_boost
                         - 0.15 * pos
                         + np.random.normal(0, 0.8) - 2.2)
                played = random.random() < 1 / (1 + math.exp(-logit))
                ts = ts_base + dt.timedelta(minutes=pos * 4)
                eid += 1
                events.append({
                    "event_id": f"e{eid:08d}",
                    "viewer_id": viewer["viewer_id"],
                    "title_id": cand["title_id"],
                    "event_ts": ts,
                    "event_type": "impression",
                    "session_id": session_id,
                    "surface": surface,
                    "device": device,
                    "locale": locale,
                    "hour_of_day": hour,
                    "position": pos,
                    "played": int(played),
                    "watch_seconds": 0,
                })
                if played:
                    full_watch = random.random() < 0.55
                    secs = int(np.random.uniform(1100, 1450)) if full_watch else int(np.random.uniform(120, 900))
                    eid += 1
                    events.append({
                        "event_id": f"e{eid:08d}",
                        "viewer_id": viewer["viewer_id"],
                        "title_id": cand["title_id"],
                        "event_ts": ts + dt.timedelta(seconds=secs),
                        "event_type": "complete" if full_watch else "skip",
                        "session_id": session_id,
                        "surface": surface,
                        "device": device,
                        "locale": locale,
                        "hour_of_day": hour,
                        "position": None,
                        "played": None,
                        "watch_seconds": secs,
                    })

events_df = pd.DataFrame(events)
print("events:", len(events_df))
print("impressions:", int((events_df.event_type == "impression").sum()),
      "| plays:", int(events_df.played.fillna(0).sum()))
print("overall play rate:", round(float(events_df[events_df.event_type=="impression"].played.mean()), 4))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Entitlements — the hard-filter layer (territory × subscription)
# COMMAND ----------
np.random.seed(SEED)
ent = []
for t in titles:
    for viewer in viewers:
        # ~92% of catalog available to a given viewer; premium tiers see everything
        allowed = viewer["tier"] != "fan" or random.random() < 0.85
        if MATURITY_RANK[t["maturity_rating"]] > viewer["_max_maturity"]:
            allowed = False
        ent.append({"viewer_id": viewer["viewer_id"], "title_id": t["title_id"], "allowed": bool(allowed)})
ent_df = pd.DataFrame(ent)
print("entitlements:", len(ent_df), "| allowed share:", round(float(ent_df.allowed.mean()), 3))
# COMMAND ----------
# MAGIC %md
# MAGIC ## Persist raw tables to Unity Catalog
# COMMAND ----------
viewers_out = pd.DataFrame(viewers).drop(columns=["_max_maturity", "_affinity"])
viewers_out["signup_date"] = pd.to_datetime(viewers_out["signup_date"]).dt.date

tables = {
    "titles": spark.createDataFrame(titles_df),
    "viewers": spark.createDataFrame(viewers_out),
    "entitlements": spark.createDataFrame(ent_df),
    "engagement_events": spark.createDataFrame(events_df),
}
for name, sdf in tables.items():
    (sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.{name}"))
    spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.{name} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    print(f"table {name}: {spark.table(f'{CATALOG}.{SCHEMA}.{name}').count()} rows")
# COMMAND ----------
# MAGIC %md
# MAGIC ## The live-events table — where the streaming demo lands
# MAGIC
# MAGIC `engagement_events_stream` starts empty and stays separate from the 90-day
# MAGIC history. Notebook 11 produces into it, notebook 10 aggregates it into
# MAGIC session features and syncs those to Lakebase continuously.
# MAGIC
# MAGIC Keeping it separate is deliberate: appending live demo events into
# MAGIC `engagement_events` would mutate the training corpus every time anyone ran
# MAGIC the freshness beat, so training would stop being reproducible.
# MAGIC
# MAGIC `produced_epoch_ms` is the producer's own clock, carried all the way to the
# MAGIC online store. Subtracting two readings of that one clock is how freshness
# MAGIC gets measured without a clock-skew argument.
# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.t('engagement_events_stream')} (
  event_id STRING,
  viewer_id STRING,
  title_id STRING,
  event_ts TIMESTAMP,
  event_type STRING,
  watch_seconds DOUBLE,
  surface STRING,
  device STRING,
  locale STRING,
  produced_epoch_ms BIGINT
) TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
print("engagement_events_stream rows:", spark.table(cfg.t("engagement_events_stream")).count())
# COMMAND ----------
display(spark.sql(f"SELECT * FROM {cfg.t('engagement_events')} ORDER BY event_ts DESC LIMIT 10"))
# COMMAND ----------
import json

max_ts = spark.sql(f"SELECT MAX(event_ts) AS m FROM {cfg.t('engagement_events')}").first()["m"]
dbutils.notebook.exit(json.dumps({
    "schema": cfg.fq,
    "end_date": str(END_DATE),
    "max_event_ts": str(max_ts),
    "titles": spark.table(cfg.t("titles")).count(),
    "viewers": spark.table(cfg.t("viewers")).count(),
    "events": spark.table(cfg.t("engagement_events")).count(),
}))
