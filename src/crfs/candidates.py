"""Candidate selection and request payloads.

The request contract lives here so notebook 04, notebook 05, the app and the
agent all send the endpoint the same shape. Whenever the model's signature
changes, this is the one file that changes with it.
"""
import time

# What the application sends. Everything else is a governed lookup.
REQUEST_KEYS = ["viewer_id", "title_id", "surface", "device", "locale",
                "hour_of_day", "request_epoch_s"]

CANDIDATE_SQL = """
SELECT t.title_id, t.title_name, t.primary_genre, t.maturity_rating
FROM {fq}.titles t
JOIN {fq}.entitlements e
  ON e.title_id = t.title_id AND e.viewer_id = '{viewer_id}' AND e.allowed
LEFT JOIN (
  SELECT title_id, COUNT(*) AS plays
  FROM {fq}.engagement_events
  WHERE viewer_id = '{viewer_id}' AND event_type = 'complete'
  GROUP BY title_id
) seen ON seen.title_id = t.title_id
WHERE seen.plays IS NULL
ORDER BY t.intrinsic_popularity DESC
LIMIT {limit}
"""

ACTIVE_VIEWER_SQL = """
SELECT e.viewer_id, COUNT(*) AS events
FROM {fq}.engagement_events e
JOIN {fq}.viewers v ON v.viewer_id = e.viewer_id
WHERE v.age_bracket IN ('25-34', '35-44', '45+')
GROUP BY e.viewer_id
ORDER BY events DESC
LIMIT {limit}
"""


def candidates(spark, fq: str, viewer_id: str, limit: int = 25):
    """Entitlement-eligible, not-yet-completed titles for one viewer.

    Entitlement is a hard filter applied before scoring, never a model feature --
    a policy decision must not be something a model can trade off.
    """
    return spark.sql(CANDIDATE_SQL.format(fq=fq, viewer_id=viewer_id, limit=limit)).toPandas()


def most_active_viewer(spark, fq: str) -> str:
    return spark.sql(ACTIVE_VIEWER_SQL.format(fq=fq, limit=1)).first()["viewer_id"]


def request_records(viewer_id: str, title_ids, surface: str = "post_play",
                    device: str = "tv", locale: str = "en-US",
                    hour_of_day: int = None, request_epoch_s: int = None):
    """Build the dataframe_records payload.

    request_epoch_s is the model's clock for session decay. Default it explicitly
    rather than letting the endpoint guess, so a frozen-clock demo is
    reproducible and a live-clock demo is deliberate.
    """
    now = int(time.time())
    epoch = now if request_epoch_s is None else int(request_epoch_s)
    hour = hour_of_day if hour_of_day is not None else time.localtime(epoch).tm_hour
    return [
        {"viewer_id": viewer_id, "title_id": str(t), "surface": surface,
         "device": device, "locale": locale, "hour_of_day": int(hour),
         "request_epoch_s": epoch}
        for t in title_ids
    ]


def query_ranker(w, endpoint: str, records):
    """Score records, returning (scores, elapsed_ms)."""
    t0 = time.perf_counter()
    resp = w.serving_endpoints.query(name=endpoint, dataframe_records=records)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return list(resp.predictions), elapsed_ms


def query_retriever(w, endpoint: str, viewer_id: str, top_k: int = 60):
    """Stage one of the funnel: ids only, no titles, no features."""
    t0 = time.perf_counter()
    resp = w.serving_endpoints.query(
        name=endpoint,
        dataframe_records=[{"viewer_id": viewer_id, "top_k": int(top_k)}])
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    # The retriever returns one JSON string per requested row (see notebook 08 --
    # a typed Series is what lets Unity Catalog accept the model's signature).
    import json as _json

    preds = list(resp.predictions or [])
    out = preds[0] if preds else []
    if isinstance(out, dict):
        # The model returns a single-column DataFrame, so a row arrives as
        # {"candidates": "<json string>"} -- unwrap the column before decoding.
        out = out.get("candidates", [])
    if isinstance(out, str):
        out = _json.loads(out)
    return out, elapsed_ms


def query_feature_endpoint(w, endpoint: str, records):
    """Feature Serving endpoint: keys in, feature values out, no model."""
    t0 = time.perf_counter()
    resp = w.serving_endpoints.query(name=endpoint, dataframe_records=records)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return resp.predictions, elapsed_ms


def rank(candidates_pdf, scores, score_col: str = "play_start_probability"):
    out = candidates_pdf.copy()
    out[score_col] = [float(s) for s in scores]
    return out.sort_values(score_col, ascending=False).reset_index(drop=True)
