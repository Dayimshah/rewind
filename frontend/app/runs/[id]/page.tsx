"use client";

import { useParams } from "next/navigation";
import { useCallback, useEffect, useState } from "react";
import { api, type BackfillDetail } from "@/lib/api";
import { StatusBadge, VerdictBadge } from "@/components/Badges";

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

  // Auto-refresh while the backfill is in a transient state.
  useEffect(() => {
    if (!detail) return;
    const transient = ["running", "reconciling"].includes(detail.status);
    if (!transient) return;
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

  if (error && !detail)
    return <div className="text-red-400">Error: {error}</div>;
  if (!detail) return <div className="text-neutral-500">Loading…</div>;

  const canRun = ["planned", "failed"].includes(detail.status);
  const canPublish = detail.status === "publishable";

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-center gap-4">
        <h1 className="text-2xl font-semibold">Backfill #{detail.id}</h1>
        <StatusBadge status={detail.status} />
        <span className="text-sm text-neutral-500">
          {detail.table} · {detail.range_start} → {detail.range_end} · {detail.transform_version}
        </span>
        <div className="ml-auto flex gap-2">
          <button
            className="rounded-md border border-neutral-700 px-3 py-1.5 text-sm disabled:opacity-40"
            onClick={run}
            disabled={busy || !canRun}
          >
            Run
          </button>
          <button
            className="rounded-md bg-emerald-600 px-3 py-1.5 text-sm font-medium disabled:opacity-40"
            onClick={publish}
            disabled={busy || !canPublish}
          >
            Publish
          </button>
        </div>
      </div>

      {error && <div className="text-sm text-red-400">{error}</div>}

      <section className="space-y-3">
        <h2 className="text-sm font-semibold uppercase tracking-wide text-neutral-400">
          Partition tasks
        </h2>
        <div className="overflow-hidden rounded-lg border border-neutral-800">
          <table className="w-full text-sm">
            <thead className="bg-neutral-900 text-left text-neutral-400">
              <tr>
                <th className="px-4 py-2">Partition</th>
                <th className="px-4 py-2">Status</th>
                <th className="px-4 py-2">Attempt</th>
                <th className="px-4 py-2">Error</th>
              </tr>
            </thead>
            <tbody>
              {detail.partition_tasks.map((t) => (
                <tr key={t.partition} className="border-t border-neutral-800">
                  <td className="px-4 py-2 font-mono text-neutral-300">{t.partition}</td>
                  <td className="px-4 py-2"><StatusBadge status={t.status} /></td>
                  <td className="px-4 py-2 text-neutral-400">{t.attempt}</td>
                  <td className="px-4 py-2 text-red-400">{t.error ?? ""}</td>
                </tr>
              ))}
              {detail.partition_tasks.length === 0 && (
                <tr><td colSpan={4} className="px-4 py-3 text-neutral-500">No partitions in scope.</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section className="space-y-3">
        <h2 className="text-sm font-semibold uppercase tracking-wide text-neutral-400">
          Reconciliation diff
        </h2>
        <div className="overflow-hidden rounded-lg border border-neutral-800">
          <table className="w-full text-sm">
            <thead className="bg-neutral-900 text-left text-neutral-400">
              <tr>
                <th className="px-4 py-2">Partition</th>
                <th className="px-4 py-2">Rows (pub → staged)</th>
                <th className="px-4 py-2">Keys +/−</th>
                <th className="px-4 py-2">Control total (before → after)</th>
                <th className="px-4 py-2">Verdict</th>
              </tr>
            </thead>
            <tbody>
              {detail.reconciliation.map((r) => (
                <tr key={r.partition} className="border-t border-neutral-800">
                  <td className="px-4 py-2 font-mono text-neutral-300">{r.partition}</td>
                  <td className="px-4 py-2 text-neutral-400">
                    {r.published_row_count} → {r.staged_row_count}
                  </td>
                  <td className="px-4 py-2">
                    <span className="text-emerald-400">+{r.keyset_added}</span>{" "}
                    <span className="text-red-400">−{r.keyset_removed}</span>
                  </td>
                  <td className="px-4 py-2 font-mono text-neutral-400">
                    {r.control_total_before.toLocaleString()} →{" "}
                    {r.control_total_after.toLocaleString()}
                  </td>
                  <td className="px-4 py-2"><VerdictBadge verdict={r.verdict} /></td>
                </tr>
              ))}
              {detail.reconciliation.length === 0 && (
                <tr><td colSpan={5} className="px-4 py-3 text-neutral-500">Not reconciled yet — run the backfill.</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
