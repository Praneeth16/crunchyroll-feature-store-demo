import { useCallback, useEffect, useRef, useState } from "react";
import { get, post, type Config, type Ctx, type Homepage } from "./api";
import { ControlBar } from "./components/ControlBar";
import { LatencyWaterfall } from "./components/LatencyWaterfall";
import { ContextMatrix, Freshness, OpsFooter } from "./components/Panels";
import { RailList, TitleRow } from "./components/Rankings";
import { FallbackPanel, OnlineRows, RequestPanel } from "./components/SidePanels";
import { ErrorNote, Skeleton } from "./components/ui";

const DEFAULT: Ctx = { viewer_id: "v0001", surface: "post_play", device: "tv", locale: "en-US", hour: 21, frozen: true };

function fromUrl(): Ctx {
  const q = new URLSearchParams(location.search);
  return {
    viewer_id: q.get("viewer") ?? DEFAULT.viewer_id,
    surface: q.get("surface") ?? DEFAULT.surface,
    device: q.get("device") ?? DEFAULT.device,
    locale: q.get("locale") ?? DEFAULT.locale,
    hour: Number(q.get("hour") ?? DEFAULT.hour),
    frozen: q.get("frozen") !== "0",
  };
}

export default function App() {
  const [ctx, setCtx] = useState<Ctx>(fromUrl);
  const [cfg, setCfg] = useState<Config | null>(null);
  const [hp, setHp] = useState<Homepage | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [roundTrip, setRoundTrip] = useState<number | null>(null);
  const [history, setHistory] = useState<number[]>([]);
  const inflight = useRef<AbortController | null>(null);

  useEffect(() => {
    get<Config>("/api/config").then(setCfg).catch(() => {});
  }, []);

  const load = useCallback(async (c: Ctx) => {
    if (!/^[A-Za-z0-9_]{1,32}$/.test(c.viewer_id)) return;
    // A newer control change supersedes the request in flight instead of queueing behind it.
    inflight.current?.abort();
    const ac = new AbortController();
    inflight.current = ac;
    setLoading(true);
    const t0 = performance.now();
    try {
      const body = await post<Homepage>("/api/homepage", c, ac.signal);
      setRoundTrip(performance.now() - t0);
      setHp(body);
      setHistory((h) => [...h.slice(-49), body.total_ms]);
      setErr(null);
    } catch (e) {
      if (!ac.signal.aborted) setErr(String(e));
    } finally {
      if (inflight.current === ac) setLoading(false);
    }
  }, []);

  useEffect(() => {
    const q = new URLSearchParams({ viewer: ctx.viewer_id, device: ctx.device, locale: ctx.locale, surface: ctx.surface, hour: String(ctx.hour), frozen: ctx.frozen ? "1" : "0" });
    history_replace(`?${q}`);
    const t = setTimeout(() => load(ctx), 120);
    return () => clearTimeout(t);
  }, [ctx, load]);

  const set = (p: Partial<Ctx>) => setCtx((c) => ({ ...c, ...p }));

  return (
    <div className="mx-auto max-w-[1400px] space-y-4 px-4 py-6 sm:px-6">
      <header className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <div className="flex items-center gap-2 text-xs font-semibold uppercase tracking-[0.2em] text-brand-500">
            <span className="h-2 w-2 rounded-full bg-brand-500" /> Crunchyroll × Databricks
          </div>
          <h1 className="mt-1 text-2xl font-semibold text-ink-100 sm:text-3xl">Two rankers, one feature store</h1>
          <p className="mt-1 max-w-2xl text-sm text-ink-400">
            <b className="text-ink-300">Vertical</b> ranks the rails. <b className="text-ink-300">Horizontal</b> ranks the titles. Both look
            up their own features from the same Lakebase online store, and the homepage renders even when they don't answer.
          </p>
        </div>
        {cfg && (
          <div className="text-right text-xs text-ink-500">
            <div className="font-mono">
              {cfg.catalog}.{cfg.schema}
            </div>
            <div>
              reference snapshot {cfg.snapshot.age_s != null ? `${Math.round(cfg.snapshot.age_s)}s old` : "loading"} · budgets rails{" "}
              {cfg.budgets_ms.rails} / titles {cfg.budgets_ms.titles} ms
            </div>
          </div>
        )}
      </header>

      <ControlBar ctx={ctx} set={set} viewers={cfg?.snapshot.viewers ?? []} />

      {err && <ErrorNote>{err}</ErrorNote>}
      {hp && !hp.known_viewer && <ErrorNote>{hp.viewer_id} has no entitlements in this dataset — every rail and title below is scored on defaults.</ErrorNote>}

      {!hp ? (
        <div className="grid gap-4 lg:grid-cols-3">
          <Skeleton className="h-40 lg:col-span-3" />
          <Skeleton className="h-[32rem] lg:col-span-2" />
          <Skeleton className="h-[32rem]" />
        </div>
      ) : (
        <div className={`space-y-4 transition-opacity ${loading ? "opacity-70" : ""}`}>
          <LatencyWaterfall hp={hp} roundTrip={roundTrip} history={history} />
          <div className="grid gap-4 lg:grid-cols-3">
            <div className="space-y-4 lg:col-span-2">
              <RailList hp={hp} />
              <TitleRow hp={hp} ctx={ctx} />
            </div>
            <div className="space-y-4">
              <FallbackPanel hp={hp} onChange={() => load(ctx)} />
              <OnlineRows hp={hp} />
              <RequestPanel hp={hp} />
            </div>
          </div>
          <div className="grid gap-4 lg:grid-cols-2">
            <ContextMatrix ctx={ctx} />
            <Freshness ctx={ctx} enabled={cfg?.burst_enabled ?? false} />
          </div>
          <OpsFooter />
        </div>
      )}
    </div>
  );
}

function history_replace(search: string) {
  if (location.search !== search) window.history.replaceState(null, "", search);
}
