"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";
import Link from "next/link";
import { ArrowRight, Search } from "lucide-react";
import { Card, Button } from "@/components/ui";

// The control DB owns the backfill list, but the API exposes lookups by id.
// This is a "jump to a backfill" entry point. Backfill #1 is the seeded demo.
export default function RunsIndex() {
  const router = useRouter();
  const [id, setId] = useState("");

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-2xl font-semibold text-neutral-100">Run Monitor</h1>
        <p className="mt-1.5 text-sm text-neutral-400">
          Open a backfill by id to watch its partition tasks and reconciliation verdicts.
        </p>
      </div>

      <Card className="p-5">
        <form
          className="flex flex-wrap items-end gap-3"
          onSubmit={(e) => {
            e.preventDefault();
            if (id.trim()) router.push(`/runs/${id.trim()}`);
          }}
        >
          <label className="block">
            <span className="mb-1.5 block text-xs uppercase tracking-wide text-neutral-500">
              Backfill id
            </span>
            <input
              className="w-44 rounded-lg border border-neutral-800 bg-[#0e0f12] px-3 py-2 text-sm text-neutral-200 outline-none focus:border-emerald-500"
              placeholder="e.g. 1"
              value={id}
              onChange={(e) => setId(e.target.value)}
            />
          </label>
          <Button type="submit">
            <Search className="h-4 w-4" /> Open
          </Button>
        </form>
      </Card>

      <Card className="flex flex-wrap items-center justify-between gap-4 p-5">
        <div>
          <h2 className="font-medium text-neutral-100">Try the seeded demo</h2>
          <p className="mt-1 text-sm text-neutral-400">
            Backfill <span className="font-mono text-neutral-300">#1</span> is a completed
            <span className="text-emerald-400"> published</span> run with a full reconciliation diff.
          </p>
        </div>
        <Link href="/runs/1">
          <Button variant="outline">
            Open #1 <ArrowRight className="h-4 w-4" />
          </Button>
        </Link>
      </Card>

      <p className="text-sm text-neutral-500">
        Or create a new one from the{" "}
        <Link href="/planner" className="text-emerald-400 hover:underline">
          Planner
        </Link>{" "}
        — it redirects here with its id.
      </p>
    </div>
  );
}
