import type { Homepage } from "../api";
import { Card, ms } from "./ui";

const LABEL: Record<string, string> = {
  rail_ranker: "Rail ranker (vertical)",
  retriever: "Candidate retriever",
  watch_next_ranker: "Watch-next ranker (horizontal)",
  lakebase_viewer: "Lakebase · online_viewer_features",
  lakebase_recent: "Lakebase · online_recent_behavior",
  lakebase_viewer_rail: "Lakebase · online_viewer_rail",
};

const COLOR: Record<string, string> = {
  rail_ranker: "bg-brand-500",
  retriever: "bg-sky-500",
  watch_next_ranker: "bg-sky-400",
  lakebase_viewer: "bg-violet-500",
  lakebase_recent: "bg-violet-500",
  lakebase_viewer_rail: "bg-violet-400",
};

/** Where one homepage request spends its time. Stages that overlap ran in parallel. */
export function LatencyWaterfall({ hp, roundTrip, history }: { hp: Homepage; roundTrip: number | null; history: number[] }) {
  const total = Math.max(hp.total_ms, 1);
  const sorted = [...history].sort((a, b) => a - b);
  const p = (q: number) => (sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] : null);
  const serial = hp.timings.reduce((s, t) => s + t.ms, 0);
  return (
    <Card
      title="Request waterfall"
      right={
        <div className="flex flex-wrap items-center justify-end gap-x-4 gap-y-1 text-xs text-ink-400">
          <span>
            server <b className="num text-ink-100">{ms(hp.total_ms)}</b>
          </span>
          <span>
            browser round trip <b className="num text-ink-100">{ms(roundTrip)}</b>
          </span>
          {sorted.length > 1 && (
            <span title={`last ${sorted.length} requests this session`}>
              p50 <b className="num text-ink-100">{ms(p(0.5))}</b> · p95 <b className="num text-ink-100">{ms(p(0.95))}</b>
            </span>
          )}
        </div>
      }
    >
      <div className="space-y-1.5">
        {hp.timings.map((t) => (
          <div key={t.stage} className="grid grid-cols-[7.5rem_1fr_3.75rem] items-center gap-2 text-xs sm:grid-cols-[14rem_1fr_4.5rem] sm:gap-3">
            <span className="truncate text-ink-300">{LABEL[t.stage] ?? t.stage}</span>
            <div className="relative h-3 rounded bg-ink-850">
              <div
                className={`absolute h-3 rounded ${COLOR[t.stage] ?? "bg-ink-500"}`}
                style={{ left: `${(t.start_ms / total) * 100}%`, width: `${Math.max((t.ms / total) * 100, 0.8)}%` }}
              />
            </div>
            <span className="num text-right text-ink-100">{ms(t.ms)}</span>
          </div>
        ))}
      </div>
      <p className="mt-3 text-xs text-ink-500">
        {hp.timings.length} calls, {ms(serial)} if run one after another — {ms(hp.total_ms)} fanned out. Every endpoint does its own
        feature lookups against the same Lakebase store; the direct reads are only here to show the rows.
      </p>
    </Card>
  );
}
