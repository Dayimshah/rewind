"use client";

import { useEffect, useState } from "react";
import { isDemoMode, onDemoModeChange } from "@/lib/api";

// Shows a subtle banner once the client has fallen back to seeded data because
// the live API was unreachable or asleep. Hidden while the real backend answers.
export function DemoBanner() {
  const [demo, setDemo] = useState(false);

  useEffect(() => {
    setDemo(isDemoMode());
    return onDemoModeChange(setDemo);
  }, []);

  if (!demo) return null;

  return (
    <div className="border-b border-amber-700/40 bg-amber-500/10 px-6 py-2 text-center text-xs text-amber-300">
      Demo mode — the live API is asleep or unreachable, so this page is showing
      seeded sample data. The real backend behaves identically; it may take ~30s
      to wake on first load.
    </div>
  );
}
