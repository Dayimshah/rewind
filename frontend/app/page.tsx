"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";
import { api, type Plan, type CreateBody } from "@/lib/api";
import { StatusBadge } from "@/components/Badges";

const REASON_HINT: Record<string, string> = {
  never_published: "no prior publish at this transform",
  late_data: "raw changed after last publish",
  in_hot_window: "owned by live stream — never touched",
  no_raw_data: "nothing to reprocess",
  already_published: "already correct (idempotent skip)",
  out_of_range: "outside requested range",
};

export default function PlannerPage() {
  const router = useRouter();
  const [form, setForm] = useState<CreateBody>({
    table: "fct_revenue",
    range_start: "2026-09-20",
    range_end: "2026-11-02",
    transform_version: "v7",
  });
  const [plan, setPlan] = useState<Plan | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const set = (k: keyof CreateBody) => (e: React.ChangeEvent<HTMLInputElement>) =>
    setForm({ ...form, [k]: e.target.value });

  async function preview() {
    setError(null);
    setBusy(true);
    try {
      setPlan(await api.plan(form));
    } catch (e) {
      setError(String(e));
      setPlan(null);
    } finally {
      setBusy(false);
    }
  }

  async function createAndRun() {
    setError(null);
    setBusy(true);
    try {
      const { backfill_id } = await api.createBackfill(form);
      await api.run(backfill_id);
      router.push(`/runs/${backfill_id}`);
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  }

  const included = plan?.included.length ?? 0;
  const skipped = plan ? plan.decisions.length - included : 0;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold">Scope Planner</h1>
        <p className="mt-1 text-sm text-neutral-400">
          Preview exactly which partitions a backfill would reprocess before touching any data.
        </p>
      </div>

      <div className="grid grid-cols-1 gap-4 rounded-lg border border-neutral-800 bg-neutral-900/40 p-5 sm:grid-cols-4">
        <Field label="Table">
          <input className="inp" value={form.table} onChange={set("table")} />
        </Field>
        <Field label="Start (inclusive)">
          <input className="inp" type="date" value={form.range_start} onChange={set("range_start")} />
        </Field>
        <Field label="End (inclusive)">
          <input className="inp" type="date" value={form.range_end} onChange={set("range_end")} />
        </Field>
        <Field label="Transform version">
          <input className="inp" value={form.transform_version} onChange={set("transform_version")} />
        </Field>
      </div>

      <div className="flex gap-3">
        <button className="btn-secondary" onClick={preview} disabled={busy}>
          {busy ? "…" : "Preview scope"}
        </button>
        <button className="btn-primary" onClick={createAndRun} disabled={busy}>
          Create &amp; run backfill
        </button>
      </div>

      {error && (
        <div className="rounded border border-red-800 bg-red-950/50 px-4 py-3 text-sm text-red-300">
          {error}
        </div>
      )}

      {plan && (
        <div className="space-y-4">
          <div className="flex flex-wrap items-center gap-4 text-sm">
            <span className="rounded bg-emerald-600/20 px-3 py-1 text-emerald-300">
              {included} to reprocess
            </span>
            <span className="rounded bg-neutral-700/40 px-3 py-1 text-neutral-300">
              {skipped} skipped
            </span>
            <span className="text-neutral-500">
              hot-window cutoff {plan.hot_window_cutoff}
            </span>
          </div>

          <div className="overflow-hidden rounded-lg border border-neutral-800">
            <table className="w-full text-sm">
              <thead className="bg-neutral-900 text-left text-neutral-400">
                <tr>
                  <th className="px-4 py-2">Partition</th>
                  <th className="px-4 py-2">Action</th>
                  <th className="px-4 py-2">Reason</th>
                </tr>
              </thead>
              <tbody>
                {plan.decisions.map((d) => (
                  <tr key={d.partition} className="border-t border-neutral-800">
                    <td className="px-4 py-2 font-mono text-neutral-300">{d.partition}</td>
                    <td className="px-4 py-2">
                      <StatusBadge status={d.action === "include" ? "staged" : "pending"} />
                    </td>
                    <td className="px-4 py-2 text-neutral-400">
                      {d.reason}
                      <span className="ml-2 text-xs text-neutral-600">
                        {REASON_HINT[d.reason]}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      <style>{`
        .inp { width:100%; border-radius:0.375rem; border:1px solid #333; background:#111; padding:0.5rem 0.75rem; font-size:0.875rem; }
        .btn-primary { border-radius:0.375rem; background:#059669; padding:0.5rem 1rem; font-size:0.875rem; font-weight:500; }
        .btn-primary:disabled { opacity:0.5; }
        .btn-secondary { border-radius:0.375rem; border:1px solid #333; padding:0.5rem 1rem; font-size:0.875rem; }
      `}</style>
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="block">
      <span className="mb-1 block text-xs uppercase tracking-wide text-neutral-500">{label}</span>
      {children}
    </label>
  );
}
