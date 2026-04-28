import { appendFileSync, mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { format } from "node:util";

let installed = false;

export function installFileLogger(): void {
  if (installed) return;

  const configured = process.env.LOG_FILE?.trim();
  const shouldLog = !!configured || process.env.NODE_ENV === "production";
  if (!shouldLog) return;

  installed = true;
  const logPath = resolve(configured || "logs/schoolyank.log");
  mkdirSync(dirname(logPath), { recursive: true });

  const patch = (target: NodeJS.WriteStream) => {
    const original = target.write.bind(target) as any;
    (target as any).write = (chunk: unknown, encoding?: unknown, cb?: unknown) => {
      try {
        appendFileSync(logPath, chunk as any);
      } catch {
        // Logging must never interrupt scraping.
      }
      return original(chunk, encoding, cb);
    };
  };

  patch(process.stdout);
  patch(process.stderr);

  for (const level of ["log", "info", "warn", "error"] as const) {
    const original = console[level].bind(console);
    console[level] = (...args: unknown[]) => {
      try {
        appendFileSync(logPath, `${format(...args)}\n`);
      } catch {
        // Logging must never interrupt scraping.
      }
      original(...args);
    };
  }

  const ts = new Date().toISOString();
  process.stderr.write(`[logger] writing logs to ${logPath} (${ts})\n`);
}
