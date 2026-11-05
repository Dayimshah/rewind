import Link from "next/link";
import {
  Crosshair,
  ShieldCheck,
  RefreshCw,
  CheckCircle2,
  ArrowRight,
} from "lucide-react";
import { Card, Button, Stat, GithubIcon } from "@/components/ui";

const GUARANTEES = [
  {
    icon: Crosshair,
    title: "Precise scope",
    body: "Computes the minimal set of partitions to reprocess from source watermarks and the requested change — and skips anything the live stream currently owns.",
  },
  {
    icon: ShieldCheck,
    title: "Stream-safe isolation",
    body: "Reprocessing happens in staging. The recent 'hot window' the live stream is still writing never gets touched, so a backfill can't collide with ingestion.",
  },
  {
    icon: RefreshCw,
    title: "Idempotent publication",
    body: "Reprocessing a partition twice yields the same result. Retries are no-ops, and a crash mid-publish converges on re-run via the publish ledger.",
  },
  {
    icon: CheckCircle2,
    title: "Proof before publish",
    body: "A DuckDB reconciliation (row counts, key-set diff, control totals) gates the atomic swap. If the numbers don't check out, nothing goes live.",
  },
];

export default function Overview() {
  return (
    <div className="space-y-16">
      {/* hero */}
      <section className="pt-6">
        <p className="mb-3 inline-flex items-center rounded-full border border-emerald-500/30 bg-emerald-500/10 px-3 py-1 text-xs font-medium text-emerald-300">
          Backfill orchestration · correctness-first
        </p>
        <h1 className="max-w-3xl text-4xl font-semibold leading-tight tracking-tight text-neutral-50 sm:text-5xl">
          Reprocess historical data without
          <span className="text-emerald-400"> double-counting</span>,
          with a check before anything goes live.
        </h1>
        <p className="mt-5 max-w-2xl text-lg text-neutral-400">
          Most teams backfill by hand-rerunning a month of ingestion and hoping for the best.
          Rewind makes it a boring, safe operation: it figures out exactly which partitions need
          rework, reruns them in isolation, proves the result against what&apos;s already published,
          and only then swaps the corrected data in.
        </p>
        <div className="mt-8 flex flex-wrap gap-3">
          <Link href="/planner">
            <Button>
              Open the Planner <ArrowRight className="h-4 w-4" />
            </Button>
          </Link>
          <a href="https://github.com/Dayimshah/rewind" target="_blank" rel="noreferrer">
            <Button variant="outline">
              <GithubIcon className="h-4 w-4" /> View the code
            </Button>
          </a>
        </div>
      </section>

      {/* stats */}
      <Card className="px-6 py-8">
        <div className="grid grid-cols-2 gap-6 sm:grid-cols-4">
          <Stat value="4" label="correctness guarantees" />
          <Stat value="40+" label="tests (idempotency, faults)" />
          <Stat value="2" label="publish backends" />
          <Stat value="0" label="invented metrics" />
        </div>
      </Card>

      {/* guarantees */}
      <section className="space-y-6">
        <div>
          <h2 className="text-xl font-semibold text-neutral-100">What it actually enforces</h2>
          <p className="mt-1 text-sm text-neutral-500">
            Four properties the system guarantees — not hopes for.
          </p>
        </div>
        <div className="grid gap-4 sm:grid-cols-2">
          {GUARANTEES.map((g) => (
            <Card key={g.title} className="p-5">
              <div className="flex items-start gap-4">
                <div className="rounded-lg bg-emerald-500/10 p-2.5 text-emerald-400 ring-1 ring-inset ring-emerald-500/20">
                  <g.icon className="h-5 w-5" />
                </div>
                <div>
                  <h3 className="font-medium text-neutral-100">{g.title}</h3>
                  <p className="mt-1.5 text-sm leading-relaxed text-neutral-400">{g.body}</p>
                </div>
              </div>
            </Card>
          ))}
        </div>
      </section>

      {/* how it flows */}
      <section className="space-y-6">
        <div>
          <h2 className="text-xl font-semibold text-neutral-100">How a backfill flows</h2>
          <p className="mt-1 text-sm text-neutral-500">
            Each step is gated by the one before it — you can&apos;t publish what didn&apos;t reconcile.
          </p>
        </div>
        <Card className="p-6">
          <div className="flex flex-wrap items-center gap-3 text-sm">
            {["Plan scope", "Reprocess → staging", "Reconcile", "Gate", "Atomic publish"].map(
              (step, i, arr) => (
                <div key={step} className="flex items-center gap-3">
                  <span className="rounded-lg border border-neutral-800 bg-neutral-900/60 px-3 py-1.5 text-neutral-200">
                    {step}
                  </span>
                  {i < arr.length - 1 && <ArrowRight className="h-4 w-4 text-neutral-600" />}
                </div>
              ),
            )}
          </div>
        </Card>
      </section>

      {/* honest note — matches the portfolio's voice */}
      <section>
        <Card className="border-neutral-800/60 p-6">
          <h2 className="text-sm font-semibold uppercase tracking-wider text-neutral-400">
            Honest about the edges
          </h2>
          <p className="mt-3 max-w-3xl text-sm leading-relaxed text-neutral-400">
            This is a live demo on a free tier, so the API may take ~30s to wake on first load,
            and the data plane is seeded fresh on each boot. The default &quot;atomic swap&quot; is a
            pointer file (there&apos;s also a real Apache Iceberg backend behind a flag), and the scope
            planner assumes a single source dependency per table. Knowing where the instrument
            stops being exact is part of the point.
          </p>
        </Card>
      </section>
    </div>
  );
}
