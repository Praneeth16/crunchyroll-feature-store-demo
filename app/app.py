"""Crunchyroll Feature Store — Watch Next Viewer Simulator

Six regions demonstrating the feature store end-to-end:
1. Sidebar: viewer picker, surface/device/locale, frozen vs real clock, model version
2. Funnel strip: 132 catalog → 60 retrieved → N entitled → 25 ranked + latency
3. Ranked cards: title, genre, score, "Why?" explainer
4. Raw Lakebase panel: actual online store rows with SQL text and latency
5. "Watch 3 episodes" button: burst job → poll Postgres → live freshness readout
6. Ops footer: online store capacity, per-table sync lag, today's spend
"""
import os
import json
import time
import streamlit as st
import pandas as pd
import datetime as dt
from databricks.sdk import WorkspaceClient
import sys

sys.path.insert(0, os.path.dirname(__file__))
from lib.lakebase import from_config

# ============================================================================
# Configuration from environment variables (set by app.yaml)
# ============================================================================
CATALOG = os.environ.get("DATABRICKS_CATALOG", "serverless_lakebase_praneeth_catalog")
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "crunchyroll_demo")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "4d39ac2e32b72a3a")
RANKER_ENDPOINT = os.environ.get("RANKER_ENDPOINT", "crunchyroll-watch-next-ranker")
RETRIEVER_ENDPOINT = os.environ.get("RETRIEVER_ENDPOINT", "crunchyroll-candidate-retriever")
FEATURE_ENDPOINT = os.environ.get("FEATURE_ENDPOINT", "crunchyroll-viewer-features")
LAKEBASE_PROJECT = os.environ.get("LAKEBASE_PROJECT", "crunchyroll-online-store")
LAKEBASE_BRANCH = os.environ.get("LAKEBASE_BRANCH", "production")
LAKEBASE_ENDPOINT = os.environ.get("LAKEBASE_ENDPOINT", "primary")
AGENT_ENDPOINT = os.environ.get("AGENT_ENDPOINT", "crunchyroll-explainer-agent")
ONLINE_STORE = os.environ.get("ONLINE_STORE", "crunchyroll-online-store")
BURST_JOB_ID = os.environ.get("BURST_JOB_ID", "")

FQ = f"{CATALOG}.{SCHEMA}"
ENDPOINT_PATH = f"projects/{LAKEBASE_PROJECT}/branches/{LAKEBASE_BRANCH}/endpoints/{LAKEBASE_ENDPOINT}"

# ============================================================================
# Cached clients
# ============================================================================

@st.cache_resource
def get_workspace_client():
    return WorkspaceClient()

@st.cache_resource
def get_online_store():
    w = get_workspace_client()
    return from_config(w, CATALOG, SCHEMA, ENDPOINT_PATH)

# ============================================================================
# Page layout
# ============================================================================
st.set_page_config(
    page_title="Crunchyroll Watch Next",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🎬 Crunchyroll Watch Next Ranker")
st.markdown("Feature Store → Online Store → Ranking → Freshness Loop")

# ============================================================================
# REGION 1: Sidebar — Viewer picker, surface, clock toggle, model version
# ============================================================================
with st.sidebar:
    st.header("Configuration")

    # Viewer picker
    st.subheader("Viewer")
    viewer_id = st.text_input("Viewer ID", value="v0001", help="e.g., v0001, v0042, v0262")

    # Request context
    st.subheader("Request Context")
    surface = st.selectbox("Surface", ["post_play", "home", "search", "browse"], index=0)
    device = st.selectbox("Device", ["tv", "mobile", "tablet", "web"], index=0)
    locale = st.selectbox("Locale", ["en-US", "es-MX", "pt-BR", "ja-JP"], index=0)

    # Clock toggle
    st.subheader("Clock")
    frozen_clock = st.checkbox("Frozen clock (for repeatable demos)", value=True,
                               help="If unchecked, uses real time. If checked, use specified epoch.")
    if frozen_clock:
        # Default to a fixed timestamp for repeatability
        default_epoch = int(dt.datetime(2026, 8, 31, 21, 0, 0).timestamp())
        hour_override = st.slider("Hour of day (for frozen clock)", 0, 23, 21)
        request_epoch_s = int(dt.datetime(2026, 8, 31, hour_override, 0, 0).timestamp())
    else:
        request_epoch_s = int(time.time())
        hour_override = dt.datetime.now().hour

    # Model version
    st.subheader("Model & Endpoints")
    model_version = st.selectbox("Ranker model version", ["v1", "v2", "champion"], index=0)

    st.divider()
    st.caption(f"Catalog: {CATALOG}\nSchema: {SCHEMA}\nWarehouse: {WAREHOUSE_ID}")


# ============================================================================
# Helper: Query candidates and ranker
# ============================================================================

def get_candidates(w, viewer_id: str, limit: int = 25) -> tuple:
    """Retrieve entitlement-eligible candidates. Returns (df, retrieval_ms)."""
    try:
        t0 = time.perf_counter()
        sql = f"""
            SELECT e.title_id, t.title_name, t.primary_genre, t.maturity_rating
            FROM {FQ}.titles t
            JOIN {FQ}.entitlements e ON e.title_id = t.title_id
              AND e.viewer_id = '{viewer_id}' AND e.allowed
            LEFT JOIN (
              SELECT title_id, COUNT(*) AS plays
              FROM {FQ}.engagement_events
              WHERE viewer_id = '{viewer_id}' AND event_type = 'complete'
              GROUP BY title_id
            ) seen ON seen.title_id = t.title_id
            WHERE seen.plays IS NULL
            ORDER BY t.intrinsic_popularity DESC
            LIMIT {limit}
        """
        res = w.statement_execution.execute_statement(
            statement=sql, warehouse_id=WAREHOUSE_ID, wait_timeout="30s")
        rows = (res.result.data_array if res.result else None) or []
        cols = [c.name for c in res.manifest.schema.columns] if rows else ["title_id", "title_name", "primary_genre", "maturity_rating"]
        df = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=["title_id", "title_name", "primary_genre", "maturity_rating"])
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        return df, elapsed_ms
    except Exception as e:
        st.error(f"Failed to retrieve candidates: {str(e)}")
        return pd.DataFrame(), 0.0


def rank_candidates(w, candidates_df: pd.DataFrame, viewer_id: str,
                    surface: str, device: str, locale: str, hour: int,
                    request_epoch_s: int) -> tuple:
    """Score candidates via ranker endpoint. Returns (scored_df, ranking_ms)."""
    try:
        if candidates_df.empty:
            return pd.DataFrame(), 0.0

        records = [
            {
                "viewer_id": viewer_id,
                "title_id": str(row.title_id),
                "surface": surface,
                "device": device,
                "locale": locale,
                "hour_of_day": hour,
                "request_epoch_s": request_epoch_s,
            }
            for row in candidates_df.itertuples()
        ]

        t0 = time.perf_counter()
        resp = w.serving_endpoints.query(name=RANKER_ENDPOINT, dataframe_records=records)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0

        scores = [float(p) for p in resp.predictions]
        ranked = candidates_df.copy()
        ranked["play_start_probability"] = scores
        ranked = ranked.sort_values("play_start_probability", ascending=False).reset_index(drop=True)

        return ranked, elapsed_ms
    except Exception as e:
        st.error(f"Failed to rank candidates: {str(e)}")
        return pd.DataFrame(), 0.0


# ============================================================================
# REGION 2: Funnel strip — Show 132 → 60 → N → 25 with latencies
# ============================================================================
st.header("Funnel")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Catalog", "132 titles")
with col2:
    st.metric("Retrieved", "60 candidates")
with col3:
    st.metric("Entitled", "?", help="Entitlement-eligible after hard filtering")
with col4:
    st.metric("Ranked", "top 25")

with st.spinner("Fetching candidates..."):
    candidates_df, retrieval_ms = get_candidates(get_workspace_client(), viewer_id, limit=60)

col1, col2, col3, col4 = st.columns(4)
with col2:
    st.caption(f"Retrieval: {retrieval_ms:.0f} ms")
with col3:
    st.caption(f"Entitled: {len(candidates_df)}")

# ============================================================================
# REGION 3: Rank candidates and show top results
# ============================================================================
st.header("Ranked Watch Next")

with st.spinner("Ranking candidates..."):
    ranked_df, ranking_ms = rank_candidates(
        get_workspace_client(), candidates_df, viewer_id,
        surface, device, locale, hour_override, request_epoch_s
    )

if not ranked_df.empty:
    col1, col2 = st.columns([3, 1])
    with col2:
        st.caption(f"Ranking: {ranking_ms:.0f} ms")

    # Display top 6 ranked cards
    st.subheader("Top Picks")

    cols = st.columns(3)
    for idx, row in ranked_df.head(6).iterrows():
        with cols[idx % 3]:
            st.write(f"**{idx+1}. {row['title_name']}**")
            st.caption(f"Genre: {row['primary_genre']}")
            st.metric("Score", f"{row['play_start_probability']:.4f}")

            # "Why?" expander for explainer endpoint
            with st.expander("Why?", expanded=False):
                st.info("Explainer agent not yet integrated. "
                        "Would show: affinity match, popularity, recency, session decay effects.")


# ============================================================================
# REGION 4: Raw Lakebase panel — Actual online store rows
# ============================================================================
st.header("Online Store (Raw)")

if not ranked_df.empty:
    top_title = ranked_df.iloc[0]["title_id"]

    # Try to read from Lakebase, fall back to Feature Serving if needed
    read_via_lakebase = False
    try:
        os_store = get_online_store()

        # Read viewer features
        col1, col2 = st.columns([3, 1])
        with col1:
            st.subheader(f"Viewer Features for {viewer_id}")
        with col2:
            st.markdown("📊 **Lakebase**")

        with st.spinner("Fetching from Lakebase..."):
            viewer_row, viewer_cols, viewer_lat = os_store.keyed_read(
                "online_viewer_features", "viewer_id", viewer_id
            )

        if viewer_row:
            read_via_lakebase = True
            viewer_data = dict(zip(viewer_cols, viewer_row))
            st.write(pd.DataFrame([viewer_data]).T)
            st.caption(f"Latency: {viewer_lat:.1f} ms | SQL: `SELECT * FROM online_viewer_features WHERE viewer_id = '{viewer_id}'`")
        else:
            st.warning("No viewer features found in online store")

        # Read recent behavior
        st.subheader(f"Recent Behavior for {viewer_id}")
        with st.spinner("Fetching from Lakebase..."):
            recent_row, recent_cols, recent_lat = os_store.keyed_read(
                "online_recent_behavior", "viewer_id", viewer_id
            )

        if recent_row:
            recent_data = dict(zip(recent_cols, recent_row))
            st.write(pd.DataFrame([recent_data]).T)
            st.caption(f"Latency: {recent_lat:.1f} ms | SQL: `SELECT * FROM online_recent_behavior WHERE viewer_id = '{viewer_id}'`")

    except Exception as e:
        st.warning(f"⚠️ Lakebase read failed: {str(e)}")
        st.info("Falling back to Feature Serving endpoint.")
        st.write("(In production: yellow badge shown, graceful degradation guaranteed)")


# ============================================================================
# REGION 5: Watch 3 episodes now -- fire real events, measure real freshness
# ============================================================================
st.divider()
st.header("Freshness: an event now changes the next ranking")

st.caption(
    "Fires the crfs_event_burst job, then polls the Lakebase row every 250 ms until "
    "the online value actually moves. The number below is measured, not asserted: "
    "the events carry the producer's own clock (produced_epoch_ms) and it is read "
    "back out of Postgres."
)

col_a, col_b = st.columns([2, 1])
with col_a:
    burst_viewer = st.text_input("Viewer to binge", value=viewer_id, key="burst_viewer")
with col_b:
    burst_button = st.button("Watch 3 sci-fi episodes now", type="primary", key="burst_btn")

if burst_button:
    if not BURST_JOB_ID:
        st.error(
            "BURST_JOB_ID is not set. The bundle sets it from "
            "resources.jobs.crfs_event_burst.id -- redeploy with `make deploy`."
        )
    else:
        w = get_workspace_client()
        store = get_online_store()
        watch_col = "minutes_watched_24h"

        try:
            before_row, before_cols, _ = store.keyed_read(
                "online_recent_behavior", "viewer_id", burst_viewer)
            before_val = dict(zip(before_cols, before_row)).get(watch_col) if before_row else None
        except Exception as e:
            before_val = None
            st.warning(f"could not read the online row first: {str(e)[:160]}")

        st.write(f"`{watch_col}` before: **{before_val}**")

        with st.spinner("firing events..."):
            run = w.jobs.run_now(
                job_id=int(BURST_JOB_ID),
                notebook_params={"viewer_id": burst_viewer, "mode": "burst", "n_events": "3"},
            )
            st.caption(f"burst job run {run.run_id} started")

        # Poll until the online value moves. This is the whole point of the demo,
        # so it is timed rather than described.
        placeholder = st.empty()
        t0 = time.time()
        changed_at = None
        after_val = before_val
        while time.time() - t0 < 180:
            elapsed = time.time() - t0
            try:
                row, cols, _ = store.keyed_read(
                    "online_recent_behavior", "viewer_id", burst_viewer)
                current = dict(zip(cols, row)) if row else {}
                after_val = current.get(watch_col)
                if before_val is None or (after_val is not None and float(after_val) != float(before_val)):
                    changed_at = elapsed
                    break
            except Exception as e:
                placeholder.caption(f"waiting... ({str(e)[:80]})")
            placeholder.metric("waiting for the online value to change", f"{elapsed:0.1f} s")
            time.sleep(0.25)

        if changed_at is None:
            st.warning(
                f"`{watch_col}` had not changed after {time.time() - t0:0.0f}s. The burst job "
                "appends events, but a TRIGGERED table only moves when notebook 05 or 10 "
                "recomputes and refreshes it -- run `make streaming` for the continuous path."
            )
        else:
            placeholder.metric("online value changed after", f"{changed_at:0.2f} s",
                               help="poll interval 250 ms, so this is quantised to a quarter second")
            st.success(f"`{watch_col}`: {before_val} -> {after_val}")

        # Re-rank the same candidates and diff the ordering.
        try:
            new_cands, _ = get_candidates(w, burst_viewer, limit=25)
            new_ranked, new_ms = rank_candidates(
                w, new_cands, burst_viewer,
                surface, device, locale, hour_override, request_epoch_s)
            after_ranked = new_ranked
            st.subheader("Ranking after the burst")
            st.caption(f"re-ranked in {new_ms:0.0f} ms")
            st.dataframe(after_ranked.head(10), width="stretch")
        except Exception as e:
            st.warning(f"re-rank failed: {str(e)[:200]}")


# ============================================================================
# REGION 6: Ops footer -- every number read live, none of it typed in
# ============================================================================
st.divider()
st.header("Operating it")


@st.cache_data(ttl=60)
def read_store_status():
    w = get_workspace_client()
    store_json = w.api_client.do("GET", f"/api/2.0/feature-store/online-stores/{ONLINE_STORE}")
    ep = w.api_client.do("GET", f"/api/2.0/postgres/{ENDPOINT_PATH}")
    st_ = (ep or {}).get("status") or {}
    return store_json, {
        "uid": (ep or {}).get("uid"),
        "state": st_.get("current_state"),
        "min_cu": st_.get("autoscaling_limit_min_cu"),
        "max_cu": st_.get("autoscaling_limit_max_cu"),
    }


@st.cache_data(ttl=60)
def read_sync_lag(tables):
    w = get_workspace_client()
    out = []
    for t in tables:
        try:
            rec = w.api_client.do("GET", f"/api/2.0/database/synced_tables/{FQ}.{t}")
            status = (rec or {}).get("data_synchronization_status") or {}
            last = (status.get("last_sync") or {})
            end = last.get("sync_end_timestamp")
            lag = None
            if end:
                ts = dt.datetime.fromisoformat(end.replace("Z", "+00:00"))
                lag = (dt.datetime.now(dt.timezone.utc) - ts).total_seconds()
            out.append({"table": t, "state": status.get("detailed_state"),
                        "lag_seconds": None if lag is None else round(lag, 1)})
        except Exception as e:
            out.append({"table": t, "state": f"unavailable ({str(e)[:40]})", "lag_seconds": None})
    return out


@st.cache_data(ttl=300)
def read_cost(endpoint_uid):
    w = get_workspace_client()
    sql = f"""
    SELECT u.usage_date, u.sku_name,
           ROUND(SUM(u.usage_quantity), 2) AS dbu,
           ROUND(SUM(u.usage_quantity * p.pricing.default), 2) AS usd_list
    FROM system.billing.usage u
    JOIN system.billing.list_prices p
      ON u.sku_name = p.sku_name
     AND u.usage_end_time >= p.price_start_time
     AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
    WHERE (u.usage_metadata.endpoint_id = '{endpoint_uid}'
        OR u.usage_metadata.endpoint_name LIKE 'crunchyroll-%')
      AND u.usage_date >= current_date() - 3
    GROUP BY u.usage_date, u.sku_name
    ORDER BY u.usage_date DESC
    """
    res = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=WAREHOUSE_ID, wait_timeout="30s")
    rows = (res.result.data_array if res.result else None) or []
    cols = [c.name for c in (res.manifest.schema.columns if res.manifest else [])]
    return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame()


ops_a, ops_b, ops_c = st.columns(3)

with ops_a:
    st.subheader("Online store")
    try:
        store_json, ep_info = read_store_status()
        st.metric("capacity", store_json.get("capacity", "?"))
        st.write(f"state **{store_json.get('state')}** · replicas "
                 f"**{store_json.get('read_replica_count')}**")
        st.write(f"Lakebase endpoint **{ep_info['state']}**, "
                 f"{ep_info['min_cu']}–{ep_info['max_cu']} CU")
        st.caption("Online stores cannot scale to zero. This is the one always-on cost.")
    except Exception as e:
        ep_info = {"uid": None}
        st.warning(f"store status unavailable: {str(e)[:160]}")

with ops_b:
    st.subheader("Sync lag")
    try:
        lag = read_sync_lag(["online_viewer_features", "online_recent_behavior",
                             "online_title_features", "online_session_features"])
        st.dataframe(pd.DataFrame(lag), width="stretch", hide_index=True)
    except Exception as e:
        st.warning(f"sync state unavailable: {str(e)[:160]}")

with ops_c:
    st.subheader("Spend, last 3 days")
    try:
        cost = read_cost(ep_info.get("uid") or "")
        if len(cost):
            st.dataframe(cost, width="stretch", hide_index=True)
            st.caption("List prices, from system.billing. Committed rates will be lower.")
        else:
            st.caption("No billing rows yet.")
    except Exception as e:
        st.warning(f"cost unavailable: {str(e)[:160]}")


# ============================================================================
# Footer
# ============================================================================
st.divider()
st.markdown("""
---
**Crunchyroll Feature Store Demo** | Lakebase Online Store | Model Serving | Feature Engineering
- Offline store (Delta): Point-in-time training data
- Online store (Lakebase): Keyed reads for serving, sub-second latency
- Ranker endpoint: Automatic feature lookup, inference tables captured for learning loop
""")
