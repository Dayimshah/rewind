"use client";

import { useParams } from "next/navigation";
import { useCallback, useEffect, useState } from "react";
import { Play, UploadCloud, ArrowUpRight, ArrowDownRight, Minus } from "lucide-react";
import { api, type BackfillDetail } from "@/lib/api";
import { Card, Button, StatusBadge, VerdictBadge, cn } from "@/components/ui";

export default function RunDetail() {
  const params = useParams<{ id: string }>();
  const id = Number(params.id);
  const [detail, setDetail] = useState<BackfillDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const load = useCallback(async () => {
    try {
      setDetail(await api.detail(id));
      setError(null);
    } catch (e) {
      setError(String(e));
    }
  }, [id]);

  useEffect(() => {
    load();
  }, [load]);

  useEffect(() => {
    if (!detail) return;
    if (!["running", "reconciling"].includes(detail.status)) return;
    const t = setInterval(load, 1000);
    return () => clearInterval(t);
  }, [detail, load]);

  async function run() {
    setBusy(true);
    try {
      await api.run(id);
      await load();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  }

  async function publish() {
    setBusy(true);
    try {
      await api.publish(id);
      await load();
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy(false);
    }
  }

  if (error && !detail) return <div className="text-red-400">Error: {error}</div>;
  if (!detail) return <div className="text-neutral-500">Loading…</div>;

  const canRun = ["planned", "failed"].includes(detail.status);
  const canPublish = detail.status === "publishable";
  const passed = detail.reconciliation.filter((r) => r.verdict === "pass").length;
  const totalRecon = detail.reconciliation.length;

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-center gap-3">
        <h1 className="text-2xl font-semibold text-neutral-100">Backfill #{detail.id}</h1>
        <StatusBadge status={detail.status} />
        <span className="text-sm text-neutral-500">
          {detail.table} · <span className="tabular">{detail.range_start}</span> →{" "}
          <span className="tabular">{detail.range_end}</span> · {detail.transform_version}
        </span>
        <div className="ml-auto flex gap-2">
          <Button variant="outline" onClick={run} disabled={busy || !canRun}>
            <Play className="h-4 w-4" /> Run
          </Button>
          <Button onClick={publish} disabled={busy || !canPublish}>
            <UploadCloud className="h-4 w-4" /> Publish
          </Button>
        </div>
      </div>

      {error && <div className="text-sm text-red-400">{error}</div>}

      {/* summary cards */}
      <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
        <SummaryCard label="Partitions" value={String(detail.partition_tasks.length)} />
        <SummaryCard label="Reconciled" value={`${totalRecon}`} />
        <SummaryCard label="Passing" value={`${passed}/${totalRecon || 0}`} accent={passed === totalRecon && totalRecon > 0} />
        <SummaryCard label="Transform" value={detail.transform_version} />
      </div>

      <section className="space-y-3">
        <h2 className="text-sm font-semibold uppercase tracking-wider text-neutral-400">
          Partition tasks
        </h2>
        <Card className="overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-neutral-900/70 text-left text-xs uppercase tracking-wide text-neutral-500">
              <tr>
                <th className="px-4 py-2.5 font-medium">Partition</th>
                <th className="px-4 py-2.5 font-medium">Status</th>
                <th className="px-4 py-2.5 font-medium">Attempt</th>
                <th className="px-4 py-2.5 font-medium">Error</th>
              </tr>
            </thead>
            <tbody>
              {detail.partition_tasks.map((t, i) => (
                <tr key={t.partition} className={cn("border-t border-neutral-800/60", i % 2 ? "bg-neutral-900/20" : "")}>
                  <td className="px-4 py-2 font-mono tabular text-neutral-300">{t.partition}</td>
                  <td className="px-4 py-2"><StatusBadge status={t.status} /></td>
                  <td className="px-4 py-2 tabular text-neutral-400">{t.attempt}</td>
                  <td className="px-4 py-2 text-red-400">{t.error ?? ""}</td>
                </tr>
              ))}
              {detail.partition_tasks.length === 0 && (
                <tr><td colSpan={4} className="px-4 py-3 text-neutral-500">No partitions in scope.</td></tr>
              )}
            </tbody>
          </table>
        </Card>
      </section>

      <section className="space-y-3">
        <h2 className="text-sm font-semibold uppercase tracking-wider text-neutral-400">
          Reconciliation diff
        </h2>
        <Card className="overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-neutral-900/70 text-left text-xs uppercase tracking-wide text-neutral-500">
              <tr>
                <th className="px-4 py-2.5 font-medium">Partition</th>
                <th className="px-4 py-2.5 font-medium">Rows (pub → staged)</th>
                <th className="px-4 py-2.5 font-medium">Keys +/−</th>
                <th className="px-4 py-2.5 font-medium">Control total</th>
                <th className="px-4 py-2.5 font-medium">Verdict</th>
              </tr>
            </thead>
            <tbody>
              {detail.reconciliation.map((r, i) => {
                const delta = r.control_total_after - r.control_total_before;
                return (
                  <tr key={r.partition} className={cn("border-t border-neutral-800/60", i % 2 ? "bg-neutral-900/20" : "")}>
                    <td className="px-4 py-2 font-mono tabular text-neutral-300">{r.partition}</td>
                    <td className="px-4 py-2 tabular text-neutral-400">
                      {r.published_row_count} → {r.staged_row_count}
                    </td>
                    <td className="px-4 py-2 tabular">
                      <span className="text-emerald-400">+{r.keyset_added}</span>{" "}
                      <span className="text-red-400">−{r.keyset_removed}</span>
                    </td>
                    <td className="px-4 py-2 tabular text-neutral-400">
                      <span>{r.control_total_before.toLocaleString()}</span>
                      <span className="mx-1.5 text-neutral-600">→</span>
                      <span className="text-neutral-200">{r.control_total_after.toLocaleString()}</span>
                      <DeltaTag delta={delta} />
                    </td>
                    <td className="px-4 py-2"><VerdictBadge verdict={r.verdict} /></td>
                  </tr>
                );
              })}
              {detail.reconciliation.length === 0 && (
                <tr><td colSpan={5} className="px-4 py-3 text-neutral-500">Not reconciled yet — run the backfill.</td></tr>
              )}
            </tbody>
          </table>
        </Card>
      </section>
    </div>
  );
}

function SummaryCard({ label, value, accent }: { label: string; value: string; accent?: boolean }) {
  return (
    <Card className="px-4 py-3">
      <div className="text-xs uppercase tracking-wide text-neutral-500">{label}</div>
      <div className={cn("mt-1 text-xl font-semibold tabular", accent ? "text-emerald-400" : "text-neutral-100")}>
        {value}
      </div>
    </Card>
  );
}

function DeltaTag({ delta }: { delta: number }) {
  if (delta === 0)
    return (
      <span className="ml-2 inline-flex items-center text-xs text-neutral-500">
        <Minus className="h-3 w-3" />
      </span>
    );
  const up = delta > 0;
  return (
    <span className={cn("ml-2 inline-flex items-center gap-0.5 text-xs", up ? "text-emerald-400" : "text-amber-400")}>
      {up ? <ArrowUpRight className="h-3 w-3" /> : <ArrowDownRight className="h-3 w-3" />}
      {Math.abs(delta).toLocaleString()}
    </span>
  );
}
