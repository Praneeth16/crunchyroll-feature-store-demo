import { useEffect, useRef, useState } from "react";
import { get, post, stream, type Contexts, type Ctx, type Ops, type Title } from "../api";
import { Card, ErrorNote, SourceBadge, ms } from "./ui";

/** Same viewer, same feature store, four requests. Only the request changes. */
export function ContextMatrix({ ctx }: { ctx: Ctx }) {
  const [data, setData] = useState<Contexts | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  useEffect(() => setData(null), [ctx.viewer_id, ctx.locale]);
  const run = async () => {
    setBusy(true);
    setErr(null);
    try {
      setData(await post<Contexts>("/api/contexts", ctx));
    } catch (e) {
      setErr(String(e));
    } finally {
      setBusy(false);
    }
  };
  return (
    <Card
      title="Context sensitivity · 4 requests in parallel"
      right={
        <button className="btn-ghost" disabled={busy} onClick={run}>
          {busy ? "Scoring…" : data ? "Re-score" : "Score four contexts"}
        </button>
      }
    >
      {err && <ErrorNote>{err}</ErrorNote>}
      {!data && !err && (
        <p className="text-sm text-ink-400">
          Scores {ctx.viewer_id} at 21:00 / 09:00 on TV / mobile. Nothing in the feature store changes between the calls — if rails move,
          the request-time features are doing work a precomputed batch table cannot.
        </p>
      )}
      {data && (
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="text-ink-500">
                <th className="py-1 text-left font-normal">rail</th>
                {data.columns.map((c) => (
                  <th key={c.label} className="px-2 py-1 text-right font-normal">
                    <div className="text-ink-300">{c.label}</div>
                    <div className="flex items-center justify-end gap-1">
                      {c.source !== "model" && <SourceBadge source={c.source} />}
                      <span className="num">{ms(c.ms)}</span>
                    </div>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.rails.map((r) => (
                <tr key={r.rail_id} className="border-t border-ink-800/60">
                  <td className="py-1 text-ink-300">{r.rail_name}</td>
                  {data.columns.map((c) => {
                    const ref = data.columns.find((x) => x.label === data.reference);
                    const changed = ref && c.label !== ref.label && ref.ranks[r.rail_id] !== c.ranks[r.rail_id];
                    return (
                      <td key={c.label} className={`num px-2 py-1 text-right ${changed ? "font-semibold text-brand-400" : "text-ink-400"}`}>
                        {c.ranks[r.rail_id] ?? "–"}
                      </td>
                    );
                  })}
                </tr>
              ))}
              <tr className="border-t border-ink-700">
                <td className="py-1.5 text-ink-400">rails moved vs {data.reference}</td>
                {data.columns.map((c) => (
                  <td key={c.label} className="num px-2 py-1.5 text-right font-semibold text-ink-100">
                    {c.label === data.reference ? "ref" : c.moved_vs_ref ?? "–"}
                  </td>
                ))}
              </tr>
            </tbody>
          </table>
        </div>
      )}
    </Card>
  );
}

type Log = { t: string; text: string; tone?: "ok" | "warn" };

/** Fire real events, measure real freshness: the value is read back out of Postgres. */
export function Freshness({ ctx, enabled }: { ctx: Ctx; enabled: boolean }) {
  const [log, setLog] = useState<Log[]>([]);
  const [busy, setBusy] = useState(false);
  const [after, setAfter] = useState<Title[] | null>(null);
  const [elapsed, setElapsed] = useState<number | null>(null);
  const ac = useRef<AbortController | null>(null);
  useEffect(() => () => ac.current?.abort(), []);
  const add = (text: string, tone?: Log["tone"]) => setLog((l) => [...l, { t: new Date().toLocaleTimeString(), text, tone }]);

  const run = async () => {
    setBusy(true);
    setLog([]);
    setAfter(null);
    setElapsed(null);
    ac.current = new AbortController();
    try {
      await stream(
        "/api/burst",
        { ...ctx, n_events: 3 },
        (ev, d) => {
          if (ev === "before") add(`${d.column} before: ${d.value}`);
          if (ev === "run") add(`burst job run ${d.run_id} started`);
          if (ev === "tick") {
            setElapsed(d.elapsed_s);
            add(`${d.elapsed_s.toFixed(0)}s · job ${d.job_state}`);
          }
          if (ev === "changed") {
            setElapsed(d.after_s);
            add(`online value changed after ${d.after_s.toFixed(2)} s: ${d.before} → ${d.after}`, "ok");
          }
          if (ev === "timeout") add(`value had not moved after ${d.elapsed_s}s — check the job's recompute_and_refresh task`, "warn");
          if (ev === "reranked") setAfter(d.items.slice(0, 10));
        },
        ac.current.signal,
      );
    } catch (e) {
      if (!ac.current?.signal.aborted) add(String(e), "warn");
    } finally {
      setBusy(false);
    }
  };

  return (
    <Card
      title="Freshness · an event now changes the next ranking"
      right={
        <button className="btn-primary" disabled={busy || !enabled} onClick={run} title={enabled ? "" : "BURST_JOB_ID not set"}>
          {busy ? `Waiting… ${elapsed != null ? `${elapsed.toFixed(0)}s` : ""}` : `Watch 3 sci-fi episodes as ${ctx.viewer_id}`}
        </button>
      }
    >
      <p className="mb-3 text-xs text-ink-500">
        Fires <code>crfs_event_burst</code> (append events → recompute <code>recent_behavior_current</code> → refresh its sync), then polls
        the Lakebase row every 250 ms. TRIGGERED path, so minutes; <code>make streaming</code> is the seconds-scale CONTINUOUS one.
      </p>
      {log.length > 0 && (
        <div className="max-h-40 overflow-y-auto rounded-lg bg-ink-950 p-2 font-mono text-[11px]">
          {log.map((l, i) => (
            <div key={i} className={l.tone === "ok" ? "text-emerald-300" : l.tone === "warn" ? "text-amber-300" : "text-ink-400"}>
              <span className="text-ink-600">{l.t}</span> {l.text}
            </div>
          ))}
        </div>
      )}
      {after && (
        <div className="mt-3">
          <div className="mb-1 text-xs text-ink-400">Watch-next after the burst</div>
          <ol className="grid grid-cols-2 gap-x-4 text-xs sm:grid-cols-5">
            {after.map((t, i) => (
              <li key={t.title_id} className="truncate text-ink-300">
                <span className="num text-ink-500">{i + 1}.</span> {t.title_name}
              </li>
            ))}
          </ol>
        </div>
      )}
    </Card>
  );
}

export function OpsFooter() {
  const [ops, setOps] = useState<Ops | null>(null);
  useEffect(() => {
    get<Ops>("/api/ops").then(setOps).catch(() => setOps({}));
  }, []);
  const s = ops?.store as Record<string, string | number> | undefined;
  return (
    <div className="grid gap-4 md:grid-cols-3">
      <Card title="Online store">
        {!ops ? (
          <p className="text-xs text-ink-500">loading…</p>
        ) : s && !("error" in s) ? (
          <div className="space-y-1 text-sm">
            <div>
              capacity <b className="text-ink-100">{s.capacity}</b> · state <b className="text-ink-100">{s.state}</b>
            </div>
            <div>
              Lakebase endpoint <b className="text-ink-100">{s.endpoint_state}</b>, {s.min_cu}–{s.max_cu} CU
            </div>
            <p className="text-xs text-ink-500">Online stores cannot scale to zero — the one always-on cost.</p>
          </div>
        ) : (
          <p className="text-xs text-rose-300">{String(s?.error ?? "unavailable")}</p>
        )}
      </Card>
      <Card title="Sync lag">
        <table className="w-full text-xs">
          <tbody>
            {(ops?.sync ?? []).map((r) => (
              <tr key={r.table}>
                <td className="truncate py-0.5 font-mono text-ink-400">{r.table.replace("online_", "")}</td>
                <td className="truncate text-ink-500">{(r.state ?? "").replace("SYNCED_", "").toLowerCase()}</td>
                <td className="num text-right text-ink-100">{r.lag_s == null ? "–" : r.lag_s > 3600 ? `${(r.lag_s / 3600).toFixed(1)} h` : `${Math.round(r.lag_s)} s`}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>
      <Card title="Spend · last 3 days, list price">
        {ops?.cost_error && <p className="text-xs text-rose-300">{ops.cost_error}</p>}
        <table className="w-full text-xs">
          <tbody>
            {(ops?.cost ?? []).slice(0, 8).map((r, i) => (
              <tr key={i}>
                <td className="py-0.5 text-ink-500">{r.usage_date.slice(5)}</td>
                <td className="truncate text-ink-400" title={r.sku_name}>
                  {r.sku_name.replace(/^(ENTERPRISE|PREMIUM)_/, "").slice(0, 28)}
                </td>
                <td className="num text-right text-ink-100">${r.usd_list}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>
    </div>
  );
}
