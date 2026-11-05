// Typed client for the Rewind control-plane API.
// Base URL is configurable; defaults to the dev API port (8010).

const BASE =
  process.env.NEXT_PUBLIC_REWIND_API ?? "http://localhost:8010";

export type PartitionDecision = {
  partition: string;
  action: "include" | "skip";
  reason: string;
};

export type Plan = {
  table: string;
  range_start: string;
  range_end: string;
  transform_version: string;
  hot_window_cutoff: string;
  included: string[];
  decisions: PartitionDecision[];
};

export type PartitionTask = {
  partition: string;
  status: string;
  staging_path: string | null;
  attempt: number;
  error: string | null;
};

export type ReconRow = {
  partition: string;
  published_row_count: number;
  staged_row_count: number;
  keyset_added: number;
  keyset_removed: number;
  control_total_before: number;
  control_total_after: number;
  verdict: "pass" | "fail";
};

export type BackfillDetail = {
  id: number;
  table: string;
  range_start: string;
  range_end: string;
  transform_version: string;
  status: string;
  partition_tasks: PartitionTask[];
  reconciliation: ReconRow[];
};

export type CreateBody = {
  table: string;
  range_start: string;
  range_end: string;
  transform_version: string;
};

// --- demo-mode state -------------------------------------------------------
// When the live API is unreachable or asleep (free-tier cold start), API calls
// fall back to seeded data and we flip demoMode on so the UI can show a banner.
// This keeps a static deploy (no backend) always looking alive.

import {
  SEED_BACKFILL,
  SEED_CREATE,
  SEED_PLAN,
} from "./seed";

let _demoMode = false;
const _listeners = new Set<(on: boolean) => void>();

export function isDemoMode(): boolean {
  return _demoMode;
}

export function onDemoModeChange(fn: (on: boolean) => void): () => void {
  _listeners.add(fn);
  return () => _listeners.delete(fn);
}

function enterDemoMode() {
  if (!_demoMode) {
    _demoMode = true;
    _listeners.forEach((fn) => fn(true));
  }
}

// How long to wait for the API before falling back to the seeded demo.
const API_TIMEOUT_MS = 4000;

// Public demo key for the write endpoints (create / run / publish). The backend
// only checks it when REWIND_API_KEY is set there; reads ignore it. Sending it
// on every request is harmless and keeps the client simple.
const API_KEY = process.env.NEXT_PUBLIC_REWIND_API_KEY ?? "";

class ApiError extends Error {}

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), API_TIMEOUT_MS);
  try {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (API_KEY) headers["X-API-Key"] = API_KEY;
    const res = await fetch(`${BASE}${path}`, {
      headers,
      cache: "no-store",
      signal: controller.signal,
      ...init,
    });
    if (!res.ok) {
      let detail = res.statusText;
      try {
        detail = (await res.json()).detail ?? detail;
      } catch {
        /* ignore */
      }
      // A real HTTP error (e.g. 400/409) is a genuine API response — surface it.
      throw new ApiError(`${res.status}: ${detail}`);
    }
    return (await res.json()) as T;
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Call the API, but fall back to seeded demo data on a *connectivity* failure
 * (network error / timeout / backend asleep). Genuine API errors (ApiError,
 * e.g. a 400 for an unknown table) are re-thrown so the UI still shows them.
 */
async function withFallback<T>(fn: () => Promise<T>, seed: T): Promise<T> {
  try {
    return await fn();
  } catch (e) {
    if (e instanceof ApiError) throw e; // real API response, not a connectivity issue
    enterDemoMode();
    return seed;
  }
}

export const api = {
  plan: (body: CreateBody) =>
    withFallback(
      () => req<Plan>("/plan", { method: "POST", body: JSON.stringify(body) }),
      SEED_PLAN,
    ),

  createBackfill: (body: CreateBody) =>
    withFallback(
      () =>
        req<{ backfill_id: number; status: string; plan: Plan }>("/backfills", {
          method: "POST",
          body: JSON.stringify(body),
        }),
      SEED_CREATE,
    ),

  run: (id: number) =>
    withFallback(
      () =>
        req<{ status: string }>(`/backfills/${id}/run`, {
          method: "POST",
          body: "{}",
        }),
      { status: "publishable" },
    ),

  publish: (id: number) =>
    withFallback(
      () => req<{ status: string }>(`/backfills/${id}/publish`, { method: "POST" }),
      { status: "published" },
    ),

  detail: (id: number) =>
    withFallback(() => req<BackfillDetail>(`/backfills/${id}`), SEED_BACKFILL),
};

export { BASE as API_BASE };
