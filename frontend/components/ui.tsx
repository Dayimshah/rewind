// Small shared UI primitives — a lightweight shadcn-style kit without the full
// dependency. Dark theme, emerald accent, matching the portfolio's look.
import * as React from "react";

export function cn(...parts: Array<string | false | null | undefined>): string {
  return parts.filter(Boolean).join(" ");
}

export function Card({
  className,
  children,
}: {
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <div
      className={cn(
        "rounded-xl border border-neutral-800/80 bg-neutral-900/40 backdrop-blur-sm",
        className,
      )}
    >
      {children}
    </div>
  );
}

export function Button({
  variant = "primary",
  className,
  ...props
}: React.ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: "primary" | "ghost" | "outline";
}) {
  const base =
    "inline-flex items-center justify-center gap-2 rounded-lg px-4 py-2 text-sm font-medium transition disabled:opacity-40 disabled:pointer-events-none";
  const variants: Record<string, string> = {
    primary: "bg-emerald-500 text-neutral-950 hover:bg-emerald-400",
    outline: "border border-neutral-700 text-neutral-200 hover:bg-neutral-800/60",
    ghost: "text-neutral-300 hover:bg-neutral-800/60",
  };
  return <button className={cn(base, variants[variant], className)} {...props} />;
}

const STATUS_COLORS: Record<string, string> = {
  planned: "bg-neutral-700/60 text-neutral-200 ring-neutral-600/50",
  running: "bg-blue-500/15 text-blue-300 ring-blue-500/30",
  reconciling: "bg-amber-500/15 text-amber-300 ring-amber-500/30",
  publishable: "bg-emerald-500/15 text-emerald-300 ring-emerald-500/30",
  published: "bg-emerald-500/20 text-emerald-300 ring-emerald-500/40",
  failed: "bg-red-500/15 text-red-300 ring-red-500/30",
  pending: "bg-neutral-800 text-neutral-400 ring-neutral-700/50",
  staged: "bg-indigo-500/15 text-indigo-300 ring-indigo-500/30",
  include: "bg-emerald-500/15 text-emerald-300 ring-emerald-500/30",
  skip: "bg-neutral-800 text-neutral-400 ring-neutral-700/50",
};

export function StatusBadge({ status }: { status: string }) {
  const cls = STATUS_COLORS[status] ?? "bg-neutral-800 text-neutral-300 ring-neutral-700/50";
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-md px-2 py-0.5 text-xs font-medium ring-1 ring-inset",
        cls,
      )}
    >
      {status}
    </span>
  );
}

export function VerdictBadge({ verdict }: { verdict: string }) {
  const pass = verdict === "pass";
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-md px-2 py-0.5 text-xs font-semibold ring-1 ring-inset",
        pass
          ? "bg-emerald-500/20 text-emerald-300 ring-emerald-500/40"
          : "bg-red-500/20 text-red-300 ring-red-500/40",
      )}
    >
      {verdict.toUpperCase()}
    </span>
  );
}

export function Section({
  title,
  subtitle,
  children,
  className,
}: {
  title?: string;
  subtitle?: string;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <section className={cn("space-y-4", className)}>
      {(title || subtitle) && (
        <div>
          {title && <h2 className="text-sm font-semibold uppercase tracking-wider text-neutral-400">{title}</h2>}
          {subtitle && <p className="mt-1 text-sm text-neutral-500">{subtitle}</p>}
        </div>
      )}
      {children}
    </section>
  );
}

// lucide 1.x dropped the GitHub brand mark (trademark cleanup), so we inline it.
export function GithubIcon({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 24 24" fill="currentColor" aria-hidden className={className}>
      <path d="M12 .5C5.73.5.5 5.74.5 12.02c0 5.1 3.29 9.42 7.86 10.96.57.1.78-.25.78-.55 0-.27-.01-1.17-.02-2.12-3.2.7-3.88-1.37-3.88-1.37-.52-1.33-1.28-1.68-1.28-1.68-1.05-.72.08-.7.08-.7 1.16.08 1.77 1.2 1.77 1.2 1.03 1.77 2.7 1.26 3.36.96.1-.75.4-1.26.73-1.55-2.55-.29-5.24-1.28-5.24-5.68 0-1.25.45-2.28 1.18-3.08-.12-.29-.51-1.46.11-3.05 0 0 .96-.31 3.15 1.18a10.9 10.9 0 0 1 2.87-.39c.97 0 1.95.13 2.87.39 2.19-1.49 3.15-1.18 3.15-1.18.62 1.59.23 2.76.11 3.05.73.8 1.18 1.83 1.18 3.08 0 4.41-2.69 5.39-5.25 5.67.41.36.78 1.06.78 2.14 0 1.55-.01 2.8-.01 3.18 0 .3.21.66.79.55A11.53 11.53 0 0 0 23.5 12.02C23.5 5.74 18.27.5 12 .5Z" />
    </svg>
  );
}

export function Stat({ value, label }: { value: string; label: string }) {
  return (
    <div className="text-center">
      <div className="tabular text-3xl font-semibold text-neutral-100 sm:text-4xl">{value}</div>
      <div className="mt-1 text-xs uppercase tracking-wide text-neutral-500">{label}</div>
    </div>
  );
}
