// Per-school filesystem locks so concurrent runs (or restarts of a crashed
// run) never redo a school that's already done or currently in-flight.
//
// Two-tier check:
//   1. Output CSV exists       → done, skip.
//   2. Lock file (.lock) exists → another worker has it; skip unless stale.
//   3. Otherwise                → atomically create the lock and proceed.
//
// Locks are reclaimed if their mtime is older than STALE_MS — covers SIGKILL
// / OOM / network-died-mid-scrape cases. The owning process tracks its own
// locks in `ownedLocks` so the SIGINT handler can release them on Ctrl+C.

import { existsSync, mkdirSync, writeFileSync, readFileSync, statSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import os from "node:os";

const STALE_MS = 30 * 60 * 1000; // 30 min — generous; longest seen finalsite school is ~10 min

const ownedLocks = new Set<string>();

export type ClaimResult =
  | { kind: "acquired" }
  | { kind: "done" }            // output CSV already exists
  | { kind: "in_progress" }     // another worker holds a fresh lock
  | { kind: "stale_reclaimed" };

function lockPath(outputPath: string): string {
  return `${outputPath}.lock`;
}

/**
 * Try to claim a school for processing.
 *
 * - If the output CSV exists and `force` is false → "done".
 * - If a non-stale lock file exists → "in_progress".
 * - Otherwise atomically create the lock file (O_CREAT | O_EXCL) and return
 *   "acquired" or "stale_reclaimed".
 */
export function claim(outputPath: string, force = false): ClaimResult {
  if (!force && existsSync(outputPath)) {
    return { kind: "done" };
  }

  const lp = lockPath(outputPath);

  // If a lock exists, decide stale vs in-progress before attempting wx.
  if (existsSync(lp)) {
    let stale = false;
    try {
      const st = statSync(lp);
      stale = Date.now() - st.mtimeMs > STALE_MS;
    } catch {
      // race: lock vanished between existsSync and statSync — treat as stale-ish
      stale = true;
    }
    if (!stale) return { kind: "in_progress" };
    try { unlinkSync(lp); } catch {}
    return writeLock(lp) ? { kind: "stale_reclaimed" } : { kind: "in_progress" };
  }

  return writeLock(lp) ? { kind: "acquired" } : { kind: "in_progress" };
}

function writeLock(lp: string): boolean {
  try {
    mkdirSync(dirname(lp), { recursive: true });
    writeFileSync(
      lp,
      JSON.stringify({ pid: process.pid, host: os.hostname(), startedAt: new Date().toISOString() }),
      { flag: "wx" }, // O_CREAT | O_EXCL — fails if file exists, atomic across processes
    );
    ownedLocks.add(lp);
    return true;
  } catch {
    return false;
  }
}

export function release(outputPath: string): void {
  const lp = lockPath(outputPath);
  ownedLocks.delete(lp);
  try { unlinkSync(lp); } catch {}
}

export function describeOwner(outputPath: string): string | null {
  const lp = lockPath(outputPath);
  try {
    const raw = readFileSync(lp, "utf8");
    const meta = JSON.parse(raw) as { pid?: number; host?: string; startedAt?: string };
    const ageSec = Math.round((Date.now() - new Date(meta.startedAt ?? 0).getTime()) / 1000);
    return `pid=${meta.pid} host=${meta.host} age=${ageSec}s`;
  } catch {
    return null;
  }
}

// Best-effort cleanup of locks held by THIS process. Safe to call multiple
// times. Runs synchronously so it completes inside signal handlers.
export function releaseAllOwned(): void {
  for (const lp of ownedLocks) {
    try { unlinkSync(lp); } catch {}
  }
  ownedLocks.clear();
}

let handlersInstalled = false;

/**
 * Install one-time SIGINT/SIGTERM/exit handlers that release any locks this
 * process holds. Called from index.ts before `runBatch`.
 */
export function installCleanupHandlers(): void {
  if (handlersInstalled) return;
  handlersInstalled = true;

  const onSignal = (sig: NodeJS.Signals) => {
    releaseAllOwned();
    // re-raise so the default exit code is correct
    process.kill(process.pid, sig);
  };

  process.once("SIGINT", onSignal);
  process.once("SIGTERM", onSignal);
  process.on("exit", releaseAllOwned);
  process.on("uncaughtException", (err) => {
    releaseAllOwned();
    console.error(err);
    process.exit(1);
  });
}
