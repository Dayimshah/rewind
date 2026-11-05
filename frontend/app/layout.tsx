import type { Metadata } from "next";
import Link from "next/link";
import "./globals.css";
import { DemoBanner } from "@/components/DemoBanner";

export const metadata: Metadata = {
  title: "Rewind — Control Room",
  description: "Backfill orchestration for time-partitioned data",
};

const NAV = [
  { href: "/", label: "Planner" },
  { href: "/runs", label: "Run Monitor" },
];

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>
        <div className="min-h-screen">
          <header className="border-b border-neutral-800 bg-neutral-900/60">
            <div className="mx-auto flex max-w-6xl items-center gap-6 px-6 py-4">
              <Link href="/" className="text-lg font-semibold tracking-tight">
                Rewind<span className="text-emerald-500">.</span>
              </Link>
              <nav className="flex gap-4 text-sm text-neutral-400">
                {NAV.map((n) => (
                  <Link key={n.href} href={n.href} className="hover:text-neutral-100">
                    {n.label}
                  </Link>
                ))}
              </nav>
              <span className="ml-auto text-xs text-neutral-500">
                control plane
              </span>
            </div>
          </header>
          <DemoBanner />
          <main className="mx-auto max-w-6xl px-6 py-8">{children}</main>
        </div>
      </body>
    </html>
  );
}
