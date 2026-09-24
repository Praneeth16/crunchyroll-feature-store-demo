import { useState } from "react";
import { post, type Homepage } from "../api";
import { Card, ms } from "./ui";

export function RequestPanel({ hp }: { hp: Homepage }) {
  return (
    <Card title="What one rail row in the request carries">
      <pre className="overflow-x-auto rounded-lg bg-ink-950 p-3 font-mono text-xs leading-relaxed text-ink-300">
        {JSON.stringify(hp.rails.request_example, null, 2)}
      </pre>
      <p className="mt-2 text-xs text-ink-500">
        Seven fields. The 45 feature values the model scores on are resolved <i>inside</i> the endpoint — four feature tables and five UC
        Python UDFs — from the feature spec logged with the model. No second code path to drift.
      </p>
    </Card>
  );
}

const TABS = [
  ["viewer_rail", "viewer × rail"],
  ["viewer_features", "viewer"],
  ["recent_behavior", "recent behavior"],
] as const;

export function OnlineRows({ hp }: { hp: Homepage }) {
  const [tab, setTab] = useState<(typeof TABS)[number][0]>("viewer_rail");
  const r = hp.online[tab];
  return (
    <Card
      title="Lakebase online store · raw row"
      right={<span className="num text-xs text-ink-400">{r?.ms != null && ms(r.ms)}</span>}
    >
      <div className="mb-2 flex gap-1">
        {TABS.map(([k, label]) => (
          <button
            key={k}
            onClick={() => setTab(k)}
            className={`rounded-md px-2 py-1 text-xs ${tab === k ? "bg-ink-700 text-ink-100" : "text-ink-400 hover:text-ink-100"}`}
          >
            {label}
          </button>
        ))}
      </div>
      {r?.sql && <code className="mb-2 block truncate rounded bg-ink-950 px-2 py-1 font-mono text-[11px] text-sky-300" title={r.sql}>{r.sql}</code>}
      {r?.error && <p className="text-xs text-rose-300">direct Postgres read unavailable ({r.error}); the endpoints' own lookups are unaffected</p>}
      {r && !r.error && !r.row && <p className="text-xs text-ink-400">no online row for this key — the endpoint would score it on defaults</p>}
      {r?.row && (
        <div className="max-h-72 overflow-y-auto">
          <table className="w-full text-xs">
            <tbody>
              {Object.entries(r.row).map(([k, v]) => (
                <tr key={k} className="border-b border-ink-800/60">
                  <td className="py-1 pr-2 font-mono text-ink-400">{k}</td>
                  <td className="num py-1 text-right font-mono text-ink-100">{fmt(v)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  );
}

const fmt = (v: unknown) => (typeof v === "number" ? (Number.isInteger(v) ? String(v) : v.toFixed(4)) : v == null ? "null" : String(v));

const MODES = [
  ["off", "Healthy"],
  ["timeout", "Endpoints slow"],
  ["open", "Breakers open"],
] as const;

/** Open item #5: the homepage has to render without a fresh ranking. */
export function FallbackPanel({ hp, onChange }: { hp: Homepage; onChange: () => void }) {
  const [busy, setBusy] = useState(false);
  const set = async (mode: string) => {
    setBusy(true);
    try {
      await post("/api/fallback/simulate", { mode });
      onChange();
    } finally {
      setBusy(false);
    }
  };
  return (
    <Card title="Fallback · timeout budget + circuit breaker">
      <div className="mb-3 inline-flex rounded-lg border border-ink-700 bg-ink-850 p-0.5">
        {MODES.map(([m, label]) => (
          <button
            key={m}
            disabled={busy}
            onClick={() => set(m)}
            className={`rounded-md px-2.5 py-1 text-xs font-medium ${hp.simulate === m ? (m === "off" ? "bg-emerald-600 text-white" : "bg-rose-600 text-white") : "text-ink-400 hover:text-ink-100"}`}
          >
            {label}
          </button>
        ))}
      </div>
      <table className="w-full text-xs">
        <thead className="text-ink-500">
          <tr>
            <th className="text-left font-normal">breaker</th>
            <th className="text-left font-normal">state</th>
            <th className="text-right font-normal">fails</th>
            <th className="text-right font-normal">trips</th>
          </tr>
        </thead>
        <tbody>
          {Object.entries(hp.breakers).map(([k, b]) => (
            <tr key={k}>
              <td className="py-0.5 text-ink-300">{k}</td>
              <td className={b.state === "closed" ? "text-emerald-400" : b.state === "open" ? "text-rose-400" : "text-amber-400"}>{b.state}</td>
              <td className="num text-right text-ink-300">{b.consecutive_failures}</td>
              <td className="num text-right text-ink-300">{b.trips}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="mt-2 text-xs text-ink-500">
        Rails: model → this viewer's last good order (filtered to today's eligible set) → editorial. Titles: model → retrieval order →
        popularity. Past capacity the endpoint returns 429 instead of queueing, so this is what keeps the page up.
      </p>
    </Card>
  );
}
