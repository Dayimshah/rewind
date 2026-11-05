// Small status/verdict badges shared across screens.

const STATUS_COLORS: Record<string, string> = {
  planned: "bg-neutral-700 text-neutral-200",
  running: "bg-blue-600/80 text-white",
  reconciling: "bg-amber-600/80 text-white",
  publishable: "bg-emerald-600/80 text-white",
  published: "bg-emerald-700 text-white",
  failed: "bg-red-700 text-white",
  pending: "bg-neutral-700 text-neutral-300",
  staged: "bg-indigo-600/80 text-white",
};

export function StatusBadge({ status }: { status: string }) {
  const cls = STATUS_COLORS[status] ?? "bg-neutral-700 text-neutral-200";
  return (
    <span className={`inline-block rounded px-2 py-0.5 text-xs font-medium ${cls}`}>
      {status}
    </span>
  );
}

export function VerdictBadge({ verdict }: { verdict: string }) {
  const pass = verdict === "pass";
  return (
    <span
      className={`inline-block rounded px-2 py-0.5 text-xs font-semibold ${
        pass ? "bg-emerald-600 text-white" : "bg-red-600 text-white"
      }`}
    >
      {verdict.toUpperCase()}
    </span>
  );
}
