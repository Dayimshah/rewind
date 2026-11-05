import type { Metadata } from "next";
import Link from "next/link";
import { Inter } from "next/font/google";
import "./globals.css";
import { DemoBanner } from "@/components/DemoBanner";
import { GithubIcon } from "@/components/ui";

const inter = Inter({ subsets: ["latin"], variable: "--font-inter" });

export const metadata: Metadata = {
  title: "Rewind — Backfill Control Room",
  description:
    "Safe backfill orchestration for time-partitioned data: precise scope, isolated reprocessing, a reconciliation gate, and atomic publish.",
};

const NAV = [
  { href: "/", label: "Overview" },
  { href: "/planner", label: "Planner" },
  { href: "/runs", label: "Run Monitor" },
];

const GITHUB = "https://github.com/Dayimshah/rewind";

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en" className={inter.variable}>
      <body className="font-sans">
        <div className="flex min-h-screen flex-col">
          <header className="sticky top-0 z-20 border-b border-neutral-800/70 bg-[#0a0b0d]/80 backdrop-blur">
            <div className="mx-auto flex max-w-6xl items-center gap-8 px-6 py-3.5">
              <Link href="/" className="text-base font-semibold tracking-tight text-neutral-100">
                Rewind<span className="text-emerald-500">.</span>
              </Link>
              <nav className="flex gap-5 text-sm text-neutral-400">
                {NAV.map((n) => (
                  <Link key={n.href} href={n.href} className="transition hover:text-neutral-100">
                    {n.label}
                  </Link>
                ))}
              </nav>
              <a
                href={GITHUB}
                target="_blank"
                rel="noreferrer"
                className="ml-auto inline-flex items-center gap-1.5 text-sm text-neutral-400 transition hover:text-neutral-100"
              >
                <GithubIcon className="h-4 w-4" />
                <span className="hidden sm:inline">source</span>
              </a>
            </div>
          </header>
          <DemoBanner />
          <main className="mx-auto w-full max-w-6xl flex-1 px-6 py-10">{children}</main>
          <footer className="border-t border-neutral-800/70">
            <div className="mx-auto flex max-w-6xl flex-col gap-1 px-6 py-6 text-xs text-neutral-500 sm:flex-row sm:items-center sm:justify-between">
              <span>
                Rewind — backfill orchestration for time-partitioned data. Built by{" "}
                <a href="https://dayimshah.vercel.app" target="_blank" rel="noreferrer" className="text-neutral-400 hover:text-neutral-200">
                  Dayim Shah
                </a>
                .
              </span>
              <span>© {new Date().getFullYear()} · demo environment</span>
            </div>
          </footer>
        </div>
      </body>
    </html>
  );
}
