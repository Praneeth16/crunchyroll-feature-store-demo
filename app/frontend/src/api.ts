export type Source = "model" | "cached" | "editorial" | "retrieval" | "popularity";

export interface Call {
  status: string;
  ms: number;
  budget_ms?: number;
  error?: string;
  cached?: boolean;
}

export interface Rail {
  rail_id: string;
  rail_name: string;
  rail_type: string;
  rank: number;
  score: number | null;
  editorial_rank: number;
  incumbent_rank: number;
  moved: number;
}

export interface Title {
  title_id: string;
  title_name: string;
  genre: string;
  maturity: string;
  is_simulcast: boolean;
  release_year: string;
  score: number | null;
  retrieval_score: number | null;
}

export interface OnlineRead {
  row: Record<string, unknown> | null;
  sql?: string;
  ms?: number;
  error?: string;
}

export interface Titles {
  items: Title[];
  source: Source;
  retrieval_source: Source;
  calls: { retriever: Call; ranker: Call };
  funnel: { catalog: number; retrieved: number; entitled_unseen: number; ranked: number };
}

export interface Homepage {
  viewer_id: string;
  known_viewer: boolean;
  context: Record<string, string | number>;
  rails: {
    items: Rail[];
    source: Source;
    cached_age_s: number | null;
    call: Call;
    catalog: number;
    eligible: number;
    request_example: Record<string, unknown> | null;
  };
  titles: Titles;
  online: Record<"viewer_features" | "recent_behavior" | "viewer_rail", OnlineRead | null>;
  timings: { stage: string; start_ms: number; ms: number }[];
  total_ms: number;
  breakers: Record<string, { state: string; consecutive_failures: number; trips: number }>;
  simulate: string;
}

export interface Config {
  catalog: string;
  schema: string;
  endpoints: Record<string, string>;
  budgets_ms: Record<string, number>;
  snapshot: { ready: boolean; error: string | null; age_s: number | null; viewers: string[] };
  burst_enabled: boolean;
}

export interface Contexts {
  reference: string | null;
  columns: { label: string; source: Source; ms: number; ranks: Record<string, number>; moved_vs_ref: number | null }[];
  rails: { rail_id: string; rail_name: string }[];
}

export interface Ops {
  store?: Record<string, unknown>;
  sync?: { table: string; state: string; lag_s: number | null }[];
  cost?: { usage_date: string; sku_name: string; dbu: string; usd_list: string }[];
  cost_error?: string;
}

export interface Ctx {
  viewer_id: string;
  surface: string;
  device: string;
  locale: string;
  hour: number;
  frozen: boolean;
}

export async function post<T>(path: string, body: unknown, signal?: AbortSignal): Promise<T> {
  const r = await fetch(path, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
    signal,
  });
  if (!r.ok) throw new Error(`${r.status}: ${(await r.text()).slice(0, 300)}`);
  return r.json();
}

export async function get<T>(path: string): Promise<T> {
  const r = await fetch(path);
  if (!r.ok) throw new Error(`${r.status}: ${(await r.text()).slice(0, 300)}`);
  return r.json();
}

/** POST, then read a text/event-stream body. EventSource cannot POST. */
export async function stream(
  path: string,
  body: unknown,
  onEvent: (event: string, data: any) => void,
  signal?: AbortSignal,
) {
  const r = await fetch(path, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
    signal,
  });
  if (!r.ok || !r.body) throw new Error(`${r.status}: ${(await r.text()).slice(0, 300)}`);
  const reader = r.body.getReader();
  const dec = new TextDecoder();
  let buf = "";
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });
    let i;
    while ((i = buf.indexOf("\n\n")) >= 0) {
      const chunk = buf.slice(0, i);
      buf = buf.slice(i + 2);
      const ev = /^event: (.*)$/m.exec(chunk)?.[1] ?? "message";
      const data = /^data: (.*)$/m.exec(chunk)?.[1];
      if (data) onEvent(ev, JSON.parse(data));
    }
  }
}
