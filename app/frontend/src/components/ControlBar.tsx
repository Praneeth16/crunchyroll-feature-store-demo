import type { Ctx } from "../api";

const DEVICES = ["tv", "mobile", "tablet", "web"];
const LOCALES = ["en-US", "es-MX", "pt-BR", "ja-JP"];
const SURFACES = ["post_play", "home", "search", "browse"];

function Segmented({ value, options, onChange }: { value: string; options: string[]; onChange: (v: string) => void }) {
  return (
    <div className="inline-flex rounded-lg border border-ink-700 bg-ink-850 p-0.5">
      {options.map((o) => (
        <button
          key={o}
          onClick={() => onChange(o)}
          className={`rounded-md px-2.5 py-1 text-xs font-medium transition ${
            value === o ? "bg-brand-500 text-white" : "text-ink-400 hover:text-ink-100"
          }`}
        >
          {o}
        </button>
      ))}
    </div>
  );
}

const Field = ({ label, children }: { label: string; children: React.ReactNode }) => (
  <label className="flex flex-col gap-1">
    <span className="text-[11px] uppercase tracking-wider text-ink-500">{label}</span>
    {children}
  </label>
);

const select = "rounded-lg border border-ink-700 bg-ink-850 px-2.5 py-1.5 text-sm text-ink-100 focus:border-brand-500 focus:outline-none";

export function ControlBar({ ctx, set, viewers }: { ctx: Ctx; set: (p: Partial<Ctx>) => void; viewers: string[] }) {
  return (
    <div className="card flex flex-wrap items-end gap-x-6 gap-y-3">
      <Field label="Viewer">
        <input
          list="viewers"
          value={ctx.viewer_id}
          onChange={(e) => set({ viewer_id: e.target.value.trim() })}
          className={`${select} w-28 font-mono`}
          spellCheck={false}
        />
        <datalist id="viewers">
          {viewers.map((v) => (
            <option key={v} value={v} />
          ))}
        </datalist>
      </Field>
      <Field label="Device">
        <Segmented value={ctx.device} options={DEVICES} onChange={(device) => set({ device })} />
      </Field>
      <Field label="Locale">
        <select value={ctx.locale} onChange={(e) => set({ locale: e.target.value })} className={select}>
          {LOCALES.map((l) => (
            <option key={l}>{l}</option>
          ))}
        </select>
      </Field>
      <Field label="Surface">
        <select value={ctx.surface} onChange={(e) => set({ surface: e.target.value })} className={select}>
          {SURFACES.map((l) => (
            <option key={l}>{l}</option>
          ))}
        </select>
      </Field>
      <Field label={ctx.frozen ? `Frozen clock · ${String(ctx.hour).padStart(2, "0")}:00 UTC` : "Real clock"}>
        <div className="flex items-center gap-3">
          <button
            onClick={() => set({ frozen: !ctx.frozen })}
            className={`relative h-5 w-9 rounded-full transition ${ctx.frozen ? "bg-brand-500" : "bg-ink-700"}`}
            aria-label="toggle frozen clock"
          >
            <span className={`absolute top-0.5 h-4 w-4 rounded-full bg-white transition ${ctx.frozen ? "left-4.5" : "left-0.5"}`} />
          </button>
          <input
            type="range"
            min={0}
            max={23}
            value={ctx.hour}
            disabled={!ctx.frozen}
            onChange={(e) => set({ hour: Number(e.target.value) })}
            className="w-40 accent-brand-500 disabled:opacity-40"
          />
        </div>
      </Field>
    </div>
  );
}
