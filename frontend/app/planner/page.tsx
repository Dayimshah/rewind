"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";
import { Play, Eye, ArrowRight } from "lucide-react";
import { api, type Plan, type CreateBody } from "@/lib/api";
import { Card, Button, StatusBadge, cn } from "@/components/ui";

const REASON_HINT: Record<string, string> = {
  never_published: "no prior publish at this transform version",
  late_data: "raw data changed after the last publish",
  in_hot_window: "owned by the live stream — never touched",
  no_raw_data: "nothing on disk to reprocess",
  already_published: "already correct, idempotent skip",
  out_of_range: "outside the requested range",
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
    <div className="space-y-8">
      <div>
        <h1 className="text-2xl font-semibold text-neutral-100">Scope Planner</h1>
        <p className="mt-1.5 text-sm text-neutral-400">
          Preview exactly which partitions a backfill would reprocess — before touching any data.
        </p>
      </div>

      <Card className="p-5">
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-4">
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
        <div className="mt-5 flex flex-wrap gap-3">
          <Button variant="outline" onClick={preview} disabled={busy}>
            <Eye className="h-4 w-4" /> {busy ? "working…" : "Preview scope"}
          </Button>
          <Button onClick={createAndRun} disabled={busy}>
            <Play className="h-4 w-4" /> Create &amp; run backfill
          </Button>
        </div>
      </Card>

      {error && (
        <div className="rounded-lg border border-red-900/60 bg-red-950/40 px-4 py-3 text-sm text-red-300">
          {error}
        </div>
      )}

      {plan && (
        <div className="space-y-4">
          <div className="flex flex-wrap items-center gap-3 text-sm">
            <span className="rounded-md bg-emerald-500/15 px-3 py-1 font-medium text-emerald-300 ring-1 ring-inset ring-emerald-500/25">
              {included} to reprocess
            </span>
            <span className="rounded-md bg-neutral-800 px-3 py-1 text-neutral-400 ring-1 ring-inset ring-neutral-700/50">
              {skipped} skipped
            </span>
            <span className="text-neutral-500">
              hot-window cutoff <span className="tabular text-neutral-300">{plan.hot_window_cutoff}</span>
            </span>
          </div>

          <Card className="overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-neutral-900/70 text-left text-xs uppercase tracking-wide text-neutral-500">
                <tr>
                  <th className="px-4 py-2.5 font-medium">Partition</th>
                  <th className="px-4 py-2.5 font-medium">Action</th>
                  <th className="px-4 py-2.5 font-medium">Reason</th>
                </tr>
              </thead>
              <tbody>
                {plan.decisions.map((d, i) => (
                  <tr
                    key={d.partition}
                    className={cn(
                      "border-t border-neutral-800/60",
                      i % 2 ? "bg-neutral-900/20" : "",
                    )}
                  >
                    <td className="px-4 py-2 font-mono text-neutral-300 tabular">{d.partition}</td>
                    <td className="px-4 py-2">
                      <StatusBadge status={d.action} />
                    </td>
                    <td className="px-4 py-2">
                      <span className="font-mono text-xs text-neutral-300">{d.reason}</span>
                      <span className="ml-2 text-xs text-neutral-500">
                        {REASON_HINT[d.reason]}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Card>
        </div>
      )}

      <style>{`
        .inp { width:100%; border-radius:0.5rem; border:1px solid #27272a; background:#0e0f12; padding:0.5rem 0.75rem; font-size:0.875rem; color:#e5e5e5; }
        .inp:focus { outline:none; border-color:#10b981; box-shadow:0 0 0 1px #10b98155; }
      `}</style>
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="block">
      <span className="mb-1.5 block text-xs uppercase tracking-wide text-neutral-500">{label}</span>
      {children}
    </label>
  );
}
