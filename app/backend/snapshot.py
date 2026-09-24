"""Reference data held in memory, so the request path makes no warehouse call.

The Streamlit app ran two SQL statements per page view -- rail eligibility state and
entitled candidates -- each a serverless-warehouse round trip of one to three seconds,
against endpoints that answer in ~50 ms. Every input those statements read is small at
demo scale (16 rails, 132 titles, 300 viewers), so it is loaded once, refreshed in the
background, and refreshed per viewer after the burst job writes new events.

This is a demo-scale shortcut and is labelled as one. At Crunchyroll's cardinality the
per-viewer state (in-progress, history, simulcast, completed titles) belongs in the
online store as a published feature table, read with the same keyed lookup as every
other row -- the request path keeps its shape, only the source of these sets changes.
"""
import asyncio
import hashlib
import time

from . import settings as S
from .clients import sql_rows

# Mirrors src/crfs/rails.ELIGIBLE_RAILS_SQL and INPROGRESS_DAYS: anchored on the newest
# event, not current_timestamp(), with the 7-day in-progress window the homepage log
# was generated with. A 30-day wall-clock window made every rail eligible for every
# viewer (measured 2026-09-16), which removed the property the contract exists for.
_STATE_SQL = """
WITH clock AS (SELECT MAX(event_ts) AS as_of FROM {fq}.engagement_events),
watched AS (
  SELECT viewer_id, title_id, MAX(event_ts) AS last_ts
  FROM {fq}.engagement_events WHERE watch_seconds > 0 {where}
  GROUP BY viewer_id, title_id
)
SELECT w.viewer_id,
       SUM(CASE WHEN w.last_ts > c.as_of - INTERVAL 7 DAY THEN 1 ELSE 0 END) AS inprogress,
       COUNT(*) AS history,
       SUM(CASE WHEN t.is_simulcast THEN 1 ELSE 0 END) AS simulcast
FROM watched w CROSS JOIN clock c JOIN {fq}.titles t ON t.title_id = w.title_id
GROUP BY w.viewer_id
"""
_DONE_SQL = """
SELECT viewer_id, array_join(collect_set(title_id), ',') AS done
FROM {fq}.engagement_events WHERE event_type = 'complete' {where}
GROUP BY viewer_id
"""
_ENT_SQL = """
SELECT viewer_id, array_join(collect_list(title_id), ',') AS titles
FROM {fq}.entitlements WHERE allowed GROUP BY viewer_id
"""
_RAILS_SQL = ("SELECT rail_id, rail_name, rail_type, rail_genre, editorial_rank, "
              "is_personalized FROM {fq}.rails ORDER BY editorial_rank")
_TITLES_SQL = ("SELECT title_id, title_name, primary_genre, maturity_rating, is_simulcast, "
               "release_year, avg_rating, intrinsic_popularity FROM {fq}.titles")

# The four rails whose eligibility depends on viewer state (src/crfs/rails.STATE_DEPENDENT).
STATE_GATED = ("r_continue", "r_because", "r_new_eps", "r_watchlist")


class Snapshot:
    def __init__(self):
        self.rails: list[dict] = []
        self.titles: dict[str, dict] = {}
        self.popularity: list[str] = []
        self.entitled: dict[str, set] = {}
        self.done: dict[str, set] = {}
        self.state: dict[str, dict] = {}
        self.loaded_at = 0.0
        self.error: str | None = None
        self.ready = asyncio.Event()

    async def load(self):
        fq = S.FQ
        q = lambda s, **k: asyncio.to_thread(sql_rows, s.format(fq=fq, **k))
        rails, titles, ent, state, done = await asyncio.gather(
            q(_RAILS_SQL), q(_TITLES_SQL), q(_ENT_SQL),
            q(_STATE_SQL, where=""), q(_DONE_SQL, where=""))
        self.rails = [{**r, "editorial_rank": int(r["editorial_rank"]),
                       "is_personalized": r["is_personalized"] == "true"} for r in rails]
        self.titles = {t["title_id"]: {**t, "is_simulcast": t["is_simulcast"] == "true",
                                       "intrinsic_popularity": float(t["intrinsic_popularity"])}
                       for t in titles}
        self.popularity = sorted(self.titles, key=lambda t: -self.titles[t]["intrinsic_popularity"])
        self.entitled = {r["viewer_id"]: set(r["titles"].split(",")) for r in ent}
        self.state = {r["viewer_id"]: {k: int(r[k]) for k in ("inprogress", "history", "simulcast")}
                      for r in state}
        self.done = {r["viewer_id"]: set(r["done"].split(",")) for r in done}
        self.loaded_at = time.time()
        self.error = None
        self.ready.set()

    async def refresh_viewer(self, viewer: str):
        """Re-read one viewer's state after the burst job appended their events."""
        where = f"AND viewer_id = '{_safe(viewer)}'"
        state, done = await asyncio.gather(
            asyncio.to_thread(sql_rows, _STATE_SQL.format(fq=S.FQ, where=where)),
            asyncio.to_thread(sql_rows, _DONE_SQL.format(fq=S.FQ, where=where)))
        if state:
            self.state[viewer] = {k: int(state[0][k]) for k in ("inprogress", "history", "simulcast")}
        if done:
            self.done[viewer] = set(done[0]["done"].split(","))

    async def run(self):
        while True:
            try:
                await self.load()
            except Exception as e:  # keep serving the last good snapshot
                self.error = f"{type(e).__name__}: {str(e)[:300]}"
            await asyncio.sleep(S.SNAPSHOT_REFRESH_S if self.loaded_at else 15)

    # ---------------------------------------------------------------- queries
    def known(self, viewer: str) -> bool:
        return viewer in self.entitled

    def eligible_rails(self, viewer: str) -> list[dict]:
        """Eligibility is a hard filter applied before scoring, never a model feature."""
        st = self.state.get(viewer, {"inprogress": 0, "history": 0, "simulcast": 0})
        # The same stable watchlist trait the generator used (src/crfs/rails.py).
        h = hashlib.sha256(f"watchlist|{viewer}".encode()).hexdigest()
        gate = {"r_continue": st["inprogress"] > 0, "r_because": st["history"] > 0,
                "r_new_eps": st["simulcast"] > 0,
                "r_watchlist": int(h[:12], 16) / float(16 ** 12) < 0.62}
        return [r for r in self.rails if gate.get(r["rail_id"], True)]

    def entitled_unseen(self, viewer: str, title_ids) -> list[str]:
        ent, done = self.entitled.get(viewer, set()), self.done.get(viewer, set())
        return [t for t in title_ids if t in ent and t not in done]


def _safe(viewer: str) -> str:
    if not viewer.replace("_", "").isalnum():
        raise ValueError(f"not a viewer id: {viewer!r}")
    return viewer


snapshot = Snapshot()
