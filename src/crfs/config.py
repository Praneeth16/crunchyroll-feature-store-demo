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
