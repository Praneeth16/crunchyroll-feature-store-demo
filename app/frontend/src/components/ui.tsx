import type { ReactNode } from "react";
import type { Source } from "../api";

const SOURCE: Record<Source, { label: string; cls: string; hint: string }> = {
  model: { label: "model", cls: "bg-emerald-500/15 text-emerald-300 ring-emerald-500/30", hint: "Fresh ranking from the endpoint" },
  cached: { label: "cached", cls: "bg-amber-500/15 text-amber-300 ring-amber-500/30", hint: "Endpoint unavailable: this viewer's last good ranking, filtered to today's eligible rails" },
  editorial: { label: "editorial", cls: "bg-rose-500/15 text-rose-300 ring-rose-500/30", hint: "Endpoint unavailable and no cached ranking: the incumbent editorial order" },
  retrieval: { label: "retrieval order", cls: "bg-amber-500/15 text-amber-300 ring-amber-500/30", hint: "Ranker unavailable: retriever's order" },
  popularity: { label: "popularity", cls: "bg-rose-500/15 text-rose-300 ring-rose-500/30", hint: "Retriever unavailable: entitled titles by popularity" },
};

export function SourceBadge({ source }: { source: Source }) {
  const s = SOURCE[source];
  return (
    <span title={s.hint} className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[11px] font-medium ring-1 ${s.cls}`}>
      <span className="h-1.5 w-1.5 rounded-full bg-current" />
      {s.label}
    </span>
  );
}

export function Card({ title, right, children, className = "" }: { title: ReactNode; right?: ReactNode; children: ReactNode; className?: string }) {
  return (
    <section className={`card ${className}`}>
      <header className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <h2 className="card-title">{title}</h2>
        {right}
      </header>
      {children}
    </section>
  );
}

export function Stat({ label, value, sub }: { label: string; value: ReactNode; sub?: ReactNode }) {
  return (
    <div className="min-w-0">
      <div className="text-[11px] uppercase tracking-wider text-ink-500">{label}</div>
      <div className="num text-xl font-semibold text-ink-100">{value}</div>
      {sub && <div className="truncate text-xs text-ink-400">{sub}</div>}
    </div>
  );
}

export const ms = (v: number | null | undefined) => (v == null ? "–" : v < 10 ? `${v.toFixed(1)} ms` : `${Math.round(v)} ms`);

export function Skeleton({ className = "" }: { className?: string }) {
  return <div className={`animate-pulse rounded-md bg-ink-800 ${className}`} />;
}

export function ErrorNote({ children }: { children: ReactNode }) {
  return <div className="rounded-lg border border-rose-500/30 bg-rose-500/10 px-3 py-2 text-sm text-rose-200">{children}</div>;
}
