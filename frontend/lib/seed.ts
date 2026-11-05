// Seeded sample data for "demo mode" — shown when the live API is unreachable
// or asleep (free-tier cold start). It mirrors a real, completed backfill so the
// UI always looks alive on a static deploy (e.g. Vercel with no backend).
//
// This is a canned fct_revenue backfill: 2026-09-20..2026-11-02 at transform v7,
// with the last two days excluded by the hot window, and a passing reconciliation.

import type { BackfillDetail, Plan } from "./api";

function dateRange(start: string, endInclusive: string): string[] {
  const out: string[] = [];
  const d = new Date(start + "T00:00:00Z");
  const end = new Date(endInclusive + "T00:00:00Z");
  while (d <= end) {
    out.push(d.toISOString().slice(0, 10));
    d.setUTCDate(d.getUTCDate() + 1);
  }
  return out;
}

const RANGE_START = "2026-09-20";
const RANGE_END = "2026-11-04";
const HOT_CUTOFF = "2026-11-03"; // last 2 days (11-03, 11-04) owned by the stream
const COLD = dateRange(RANGE_START, "2026-11-02");
const HOT = ["2026-11-03", "2026-11-04"];

export const SEED_PLAN: Plan = {
  table: "fct_revenue",
  range_start: RANGE_START,
  range_end: RANGE_END,
  transform_version: "v7",
  hot_window_cutoff: HOT_CUTOFF,
  included: COLD,
  decisions: [
    ...COLD.map((partition) => ({
      partition,
      action: "include" as const,
      reason: "never_published",
    })),
    ...HOT.map((partition) => ({
      partition,
      action: "skip" as const,
      reason: "in_hot_window",
    })),
  ],
};

// Deterministic-ish sample metrics per partition for the demo backfill.
function recon(partition: string, i: number) {
  const before = 120000 + ((i * 37) % 40) * 1000;
  const after = before + ((i * 13) % 25) * 100; // corrected totals, keys stable
  return {
    partition,
    published_row_count: 30 + (i % 20),
    staged_row_count: 30 + (i % 20),
    keyset_added: 0,
    keyset_removed: 0,
    control_total_before: before,
    control_total_after: after,
    verdict: "pass" as const,
  };
}

export const SEED_BACKFILL: BackfillDetail = {
  id: 1,
  table: "fct_revenue",
  range_start: RANGE_START,
  range_end: RANGE_END,
  transform_version: "v7",
  status: "published",
  partition_tasks: COLD.map((partition) => ({
    partition,
    status: "published",
    staging_path: `staging/fct_revenue/dt=${partition}/part.parquet`,
    attempt: 1,
    error: null,
  })),
  reconciliation: COLD.map((p, i) => recon(p, i)),
};

export const SEED_CREATE = {
  backfill_id: SEED_BACKFILL.id,
  status: "planned",
  plan: SEED_PLAN,
};
