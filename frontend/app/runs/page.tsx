"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";

// The control DB owns the backfill list, but Phase-1 API exposes lookups by id.
// This page is a simple "jump to a backfill" entry point plus guidance.
export default function RunsIndex() {
  const router = useRouter();
  const [id, setId] = useState("");

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold">Run Monitor</h1>
        <p className="mt-1 text-sm text-neutral-400">
          Open a backfill by id to watch its partition tasks and reconciliation verdicts.
        </p>
      </div>

      <form
        className="flex gap-3"
        onSubmit={(e) => {
          e.preventDefault();
          if (id.trim()) router.push(`/runs/${id.trim()}`);
        }}
      >
        <input
          className="w-40 rounded-md border border-neutral-700 bg-neutral-900 px-3 py-2 text-sm"
          placeholder="backfill id"
          value={id}
          onChange={(e) => setId(e.target.value)}
        />
        <button className="rounded-md bg-emerald-600 px-4 py-2 text-sm font-medium">
          Open
        </button>
      </form>

      <p className="text-sm text-neutral-500">
        Create a backfill from the{" "}
        <a href="/" className="text-emerald-400 hover:underline">
          Planner
        </a>{" "}
        — it will redirect here with its id.
      </p>
    </div>
  );
}
