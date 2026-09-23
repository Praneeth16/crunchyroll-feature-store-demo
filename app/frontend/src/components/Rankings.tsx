import { useEffect, useState } from "react";
import { stream, type Ctx, type Homepage, type Title } from "../api";
import { Card, SourceBadge, ms } from "./ui";

function Moved({ n }: { n: number }) {
  if (n === 0) return <span className="num w-10 text-right text-xs text-ink-500">—</span>;
  const up = n > 0;
  return (
    <span className={`num w-10 text-right text-xs font-semibold ${up ? "text-emerald-400" : "text-rose-400"}`}>
      {up ? "▲" : "▼"}
      {Math.abs(n)}
    </span>
  );
}

export function RailList({ hp }: { hp: Homepage }) {
  const r = hp.rails;
  const max = Math.max(...r.items.map((i) => i.score ?? 0), 0.0001);
  const gained = r.items.filter((i) => i.moved > 0).length;
  return (
    <Card
      title="Vertical · homepage rail order"
      right={
        <div className="flex items-center gap-2 text-xs text-ink-400">
          <SourceBadge source={r.source} />
          {r.cached_age_s != null && <span>cached {Math.round(r.cached_age_s)}s ago</span>}
          <span className="num">{ms(r.call.ms)}</span>
        </div>
      }
    >
      <div className="mb-3 grid grid-cols-3 gap-3 text-xs text-ink-400">
        <div>
          <b className="num text-lg text-ink-100">{r.catalog}</b> rails in catalog
        </div>
        <div>
          <b className="num text-lg text-ink-100">{r.eligible}</b> eligible
        </div>
        <div>
          <b className="num text-lg text-ink-100">{r.items.length}</b> scored in 1 request
        </div>
      </div>
      {r.call.status !== "ok" && (
        <div className="mb-3 rounded-lg bg-amber-500/10 px-3 py-2 text-xs text-amber-200">
          Rail ranker {r.call.status.replace("_", " ")} after {ms(r.call.ms)} (budget {ms(r.call.budget_ms)}) — rendered the{" "}
          {r.source} order instead. The homepage never waits past its budget.
          {r.call.error && <span className="block font-mono text-[11px] text-amber-300/70">{r.call.error}</span>}
        </div>
      )}
      <ol className="space-y-1">
        {r.items.map((i) => (
          <li key={i.rail_id} className="grid grid-cols-[1.75rem_1fr_7rem_2.5rem] items-center gap-3 rounded-lg px-2 py-1.5 hover:bg-ink-850">
            <span className="num text-sm font-semibold text-ink-500">{i.rank}</span>
            <div className="min-w-0">
              <div className="truncate text-sm text-ink-100">{i.rail_name}</div>
              <div className="text-[11px] text-ink-500">
                {i.rail_type} · was #{i.incumbent_rank}
              </div>
            </div>
            <div className="flex items-center gap-2">
              <div className="h-1.5 flex-1 rounded bg-ink-800">
                <div className="h-1.5 rounded bg-brand-500" style={{ width: `${((i.score ?? 0) / max) * 100}%` }} />
              </div>
              <span className="num w-10 text-right text-[11px] text-ink-400">{i.score == null ? "–" : i.score.toFixed(3)}</span>
            </div>
            <Moved n={i.moved} />
          </li>
        ))}
      </ol>
      <p className="mt-3 text-xs text-ink-500">
        ▲▼ = positions moved against the incumbent editorial order, dense-ranked over the same eligible set. {gained} of{" "}
        {r.items.length} rails moved up; all dashes would mean the model agrees with the old homepage.
      </p>
    </Card>
  );
}

const HUES = [18, 200, 280, 150, 340, 45, 230, 110];
const hue = (s: string) => HUES[[...s].reduce((a, c) => a + c.charCodeAt(0), 0) % HUES.length];

export function TitleRow({ hp, ctx }: { hp: Homepage; ctx: Ctx }) {
  const t = hp.titles;
  const [open, setOpen] = useState<Title | null>(null);
  const f = t.funnel;
  return (
    <Card
      title="Horizontal · watch-next row"
      right={
        <div className="flex items-center gap-2 text-xs text-ink-400">
          <SourceBadge source={t.source} />
          <span className="num">
            retrieve {t.calls.retriever.cached ? "cached" : ms(t.calls.retriever.ms)} · rank {ms(t.calls.ranker.ms)}
          </span>
        </div>
      }
    >
      <div className="mb-4 flex items-center gap-1 text-xs">
        {[
          ["catalog", f.catalog],
          [t.retrieval_source === "model" ? "retrieved" : "popular (retriever down)", f.retrieved],
          ["entitled & unseen", f.entitled_unseen],
          ["ranked", f.ranked],
        ].map(([label, n], idx) => (
          <div key={String(label)} className="flex items-center gap-1">
            {idx > 0 && <span className="text-ink-700">→</span>}
            <span className="rounded-md bg-ink-850 px-2 py-1">
              <b className="num text-ink-100">{n}</b> <span className="text-ink-400">{label}</span>
            </span>
          </div>
        ))}
      </div>
      <div className="-mx-1 flex snap-x gap-3 overflow-x-auto px-1 pb-2">
        {t.items.map((x, i) => (
          <button
            key={x.title_id}
            onClick={() => setOpen(open?.title_id === x.title_id ? null : x)}
            className={`group w-36 shrink-0 snap-start text-left transition ${open?.title_id === x.title_id ? "scale-[1.02]" : ""}`}
          >
            <div
              className={`relative aspect-[2/3] overflow-hidden rounded-lg ring-2 ${open?.title_id === x.title_id ? "ring-brand-500" : "ring-transparent group-hover:ring-ink-500"}`}
              style={{ background: `linear-gradient(160deg, hsl(${hue(x.genre)} 55% 32%), hsl(${hue(x.genre) + 30} 40% 12%))` }}
            >
              <span className="absolute left-2 top-2 rounded bg-black/50 px-1.5 text-xs font-bold text-white">{i + 1}</span>
              {x.is_simulcast && <span className="absolute right-2 top-2 rounded bg-brand-500 px-1.5 text-[10px] font-semibold text-white">SIMULCAST</span>}
              <div className="absolute inset-x-0 bottom-0 bg-gradient-to-t from-black/80 to-transparent p-2">
                <div className="line-clamp-2 text-sm font-semibold leading-tight text-white">{x.title_name}</div>
              </div>
            </div>
            <div className="mt-1.5 flex items-center justify-between text-[11px] text-ink-400">
              <span>
                {x.genre} · {x.maturity}
              </span>
              <span className="num">{x.score == null ? "–" : x.score.toFixed(3)}</span>
            </div>
            <div className="text-[11px] text-brand-400 opacity-0 transition group-hover:opacity-100">Why this? →</div>
          </button>
        ))}
      </div>
      {open && <Explain key={open.title_id} title={open} rank={t.items.indexOf(open) + 1} ctx={ctx} />}
    </Card>
  );
}

function Explain({ title, rank, ctx }: { title: Title; rank: number; ctx: Ctx }) {
  const [text, setText] = useState("");
  const [meta, setMeta] = useState<{ first?: number; total?: number; endpoint?: string; error?: string }>({});
  useEffect(() => {
    const ac = new AbortController();
    setText("");
    setMeta({});
    stream(
      "/api/explain",
      { ...ctx, title_id: title.title_id, score: title.score, rank },
      (ev, d) => {
        if (ev === "token") setText((s) => s + d.text);
        if (ev === "meta") setMeta((m) => ({ ...m, first: d.first_token_ms }));
        if (ev === "error") setMeta((m) => ({ ...m, error: d.message }));
        if (ev === "done") setMeta((m) => ({ ...m, total: d.total_ms, endpoint: d.endpoint }));
      },
      ac.signal,
    ).catch((e) => !ac.signal.aborted && setMeta((m) => ({ ...m, error: String(e) })));
    return () => ac.abort();
  }, [title.title_id]); // eslint-disable-line react-hooks/exhaustive-deps
  return (
    <div className="mt-2 rounded-lg border border-ink-700 bg-ink-850 p-3">
      <div className="mb-1 flex items-center justify-between text-xs">
        <span className="font-semibold text-ink-100">
          Why #{rank}: {title.title_name}
        </span>
        <span className="num text-ink-500">
          {meta.first != null && `first token ${ms(meta.first)}`}
          {meta.total != null && ` · ${ms(meta.total)} · ${meta.endpoint}`}
        </span>
      </div>
      {meta.error ? (
        <p className="text-sm text-rose-300">{meta.error}</p>
      ) : (
        <p className="text-sm leading-relaxed text-ink-300">{text || <span className="animate-pulse text-ink-500">reading the feature rows…</span>}</p>
      )}
      <p className="mt-2 text-[11px] text-ink-500">
        Grounded only on the online rows this service read for the request (viewer, recent behaviour, title) and the model's score. Off the
        hot path: nothing is generated until you ask.
      </p>
    </div>
  );
}
