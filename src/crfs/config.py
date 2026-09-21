"""Single source of truth for every name and clock the demo uses.

Notebooks build a Config from their widgets; jobs pass the same values as task
base_parameters. Nothing else in the repo may hardcode a catalog, schema, store
or endpoint name.
"""
from dataclasses import dataclass, field
import datetime as dt

GENRES = ["action", "adventure", "fantasy", "sci_fi", "sports", "drama", "romance", "slice_of_life"]

MATURITY_RANK = {"all": 0, "13+": 1, "16+": 2, "18+": 3}

# Widget name -> default. Defaults match the workspace the demo was built on so
# every notebook still runs standalone from the workspace UI.
DEFAULTS = {
    "catalog": "serverless_lakebase_praneeth_catalog",
    "schema": "crunchyroll_demo",
    "online_store": "crunchyroll-online-store",
    "lakebase_project": "crunchyroll-online-store",
    "lakebase_branch": "production",
    "lakebase_endpoint": "primary",
    "ranker_endpoint": "crunchyroll-watch-next-ranker",
    # Vertical ranking (rails). Separate endpoint from the horizontal ranker
    # because the two are sized differently: the rail ranker sits in the homepage
    # request path and never scales to zero, the watch-next ranker does.
    "rail_ranker_endpoint": "crunchyroll-rail-ranker",
    "retriever_endpoint": "crunchyroll-candidate-retriever",
    "feature_endpoint": "crunchyroll-viewer-features",
    "agent_endpoint": "crunchyroll-explainer-agent",
    "llm_endpoint": "databricks-claude-sonnet-4-5",
    "warehouse_id": "4d39ac2e32b72a3a",
    "end_date": "",          # "" -> yesterday
    "volume": "crfs_ops",
}


@dataclass(frozen=True)
class Config:
    catalog: str
    schema: str
    online_store: str
    lakebase_project: str
    lakebase_branch: str
    lakebase_endpoint: str
    ranker_endpoint: str
    rail_ranker_endpoint: str
    retriever_endpoint: str
    feature_endpoint: str
    agent_endpoint: str
    llm_endpoint: str
    warehouse_id: str
    end_date: str
    volume: str
    extras: dict = field(default_factory=dict)

    @classmethod
    def from_widgets(cls, dbutils, extra_widgets=None):
        """Declare every widget with its default, then read them all back."""
        wanted = dict(DEFAULTS)
        for name, default in (extra_widgets or {}).items():
            wanted[name] = default
        for name, default in wanted.items():
            dbutils.widgets.text(name, default)
        vals = {name: dbutils.widgets.get(name) or DEFAULTS.get(name, "") for name in wanted}
        extras = {k: v for k, v in vals.items() if k not in DEFAULTS}
        core = {k: v for k, v in vals.items() if k in DEFAULTS}
        return cls(extras=extras, **core)

    # ---- naming helpers -------------------------------------------------
    @property
    def fq(self) -> str:
        return f"{self.catalog}.{self.schema}"

    def t(self, name: str) -> str:
        """Three-level name for a table, function, model or feature spec."""
        return f"{self.catalog}.{self.schema}.{name}"

    @property
    def branch_path(self) -> str:
        return f"projects/{self.lakebase_project}/branches/{self.lakebase_branch}"

    @property
    def endpoint_path(self) -> str:
        return f"{self.branch_path}/endpoints/{self.lakebase_endpoint}"

    def checkpoint(self, name: str) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/checkpoints/{name}"

    # ---- the demo clock -------------------------------------------------
    @property
    def end_date_resolved(self) -> dt.date:
        """Last day of generated history. Defaults to yesterday so 'last 24h'
        features are never stale relative to wall clock -- the bug the first
        version of this demo shipped with."""
        if self.end_date:
            return dt.date.fromisoformat(self.end_date)
        return dt.date.today() - dt.timedelta(days=1)

    def demo_now(self, spark=None):
        """The instant every 'as of now' window is measured from.

        Anchored on the newest event in the data when a table exists, so
        recompute in notebook 01 and recompute in notebook 05 agree. Falls back
        to end_date's end-of-day before any data is generated.
        """
        import pandas as pd
        if spark is not None:
            try:
                row = spark.sql(
                    f"SELECT MAX(event_ts) AS m FROM {self.t('engagement_events')}"
                ).first()
                if row and row["m"] is not None:
                    return pd.Timestamp(row["m"])
            except Exception:
                pass
        return pd.Timestamp(self.end_date_resolved) + pd.Timedelta(hours=23, minutes=59, seconds=59)

    def describe(self) -> str:
        return "\n".join(
            f"  {k:20s} {getattr(self, k)}"
            for k in ("catalog", "schema", "online_store", "ranker_endpoint",
                      "rail_ranker_endpoint",
                      "retriever_endpoint", "feature_endpoint", "agent_endpoint",
                      "llm_endpoint", "warehouse_id")
        ) + f"\n  {'end_date_resolved':20s} {self.end_date_resolved}"


# ---------------------------------------------------------------- feature-table registry
# Which feature tables each model looks up, as ONE declaration the trainers and the
# reporting notebook both read.
#
# Notebook 24's sharing table used to carry its own hand-typed dict of who-reads-what,
# while the document claimed the overlap was "resolved from Unity Catalog ... so it
# cannot drift from reality". Only the table LIST was resolved (`SHOW TABLES LIKE
# 'online_*'`); the mapping that actually constitutes the sharing claim was typed by
# hand in a reporting notebook and could disagree with the models silently.
#
# These lists mirror the FeatureLookup declarations in notebooks 02 and 22. That is a
# code declaration, not the deployed model's own feature spec, so it can still drift if
# someone retrains with different lookups and does not update it -- notebook 24 says
# which source it used rather than implying more authority than it has.
# Both rankers read the point-in-time tables, and they read the SAME two viewer tables --
# which is the sharing claim this repo makes, now checkable rather than asserted. See
# rails.title_lookups / rails.rail_lookups, which are what the trainers use.
HORIZONTAL_FEATURE_TABLES = [
    "viewer_features_ts",
    "recent_behavior_ts",
    "title_features_ts",
]
# The rail ranker reads the POINT-IN-TIME tables, not the _current ones. That changed when
# three of its lookups gained a timestamp_lookup_key (verification_log V76): the model's
# feature spec now names these, so these are what the endpoint resolves online, where each
# deduplicates to the latest row per key. Keeping the old names here would have made
# notebook 24 report a shared-table overlap that no longer exists -- the exact drift the
# notebook's own comment says this list exists to prevent.
#
# Must stay in step with rails.rail_lookups(), which is what the trainers actually use.
VERTICAL_FEATURE_TABLES = [
    "viewer_features_ts",
    "recent_behavior_ts",
    "rail_features_ts",
    "viewer_rail_features_ts",
]
# Published online but read by neither ranker: the retriever's embedding and the
# streaming freshness path.
OTHER_ONLINE_READERS = {
    "viewer_embedding_current": "retriever",
    "session_features_current": "streaming freshness path",
}


def online_readers() -> dict:
    """{online_table_name: [reader, ...]} derived from the lookup declarations above.

    Keyed by the PUBLISHED table name, which is the offline name prefixed with
    `online_` and, for the time series table, shortened -- `viewer_rail_features_ts`
    publishes to `online_viewer_rail`.
    """
    # The published name is the offline name prefixed with online_, with two exceptions:
    # viewer_rail_features_ts was published under a shortened name before the others
    # existed, and the _current suffix is dropped.
    published = {"viewer_rail_features_ts": "online_viewer_rail"}
    def pub(t):
        return published.get(t, f"online_{t.replace('_current', '')}")

    out = {}
    for t in HORIZONTAL_FEATURE_TABLES:
        out.setdefault(pub(t), []).append("watch-next (horizontal)")
    for t in VERTICAL_FEATURE_TABLES:
        out.setdefault(pub(t), []).append("rail ranker (vertical)")
    for t, reader in OTHER_ONLINE_READERS.items():
        out.setdefault(pub(t), []).append(reader)
    return out
