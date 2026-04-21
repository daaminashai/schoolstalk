// ── centralized debug logging ──
//
// gated on SCHOOLYANK_DEBUG=1 (set by the --debug cli flag). when enabled,
// every decision point in the pipeline dumps full, untruncated context to
// stderr so you can trace why the scraper made a given call.
//
// output format: `[DEBUG +0.42s section] message`, followed by pretty-printed
// data on subsequent lines (indented). goes to stderr so it doesn't pollute
// the csv pipe if someone redirects stdout.

const startTime = Date.now();

// ansi color codes — kept inline to avoid pulling picocolors into hot paths.
// nothing fancy; just enough to make scanning easier when debug is gushing.
const C = {
  reset: "\x1b[0m",
  dim: "\x1b[2m",
  bold: "\x1b[1m",
  cyan: "\x1b[36m",
  magenta: "\x1b[35m",
  yellow: "\x1b[33m",
  green: "\x1b[32m",
  red: "\x1b[31m",
  gray: "\x1b[90m",
};

export function isDebug(): boolean {
  return process.env.SCHOOLYANK_DEBUG === "1";
}

function ts(): string {
  const elapsed = (Date.now() - startTime) / 1000;
  return `+${elapsed.toFixed(2)}s`.padStart(8);
}

function fmtData(data: unknown): string {
  if (data === undefined) return "";
  try {
    const json = JSON.stringify(data, null, 2);
    if (!json) return "";
    // indent every line 4 spaces so it visually subordinates to the header
    return "\n" + json.split("\n").map((l) => `    ${C.gray}${l}${C.reset}`).join("\n");
  } catch {
    return `\n    ${C.gray}${String(data)}${C.reset}`;
  }
}

/**
 * emit one debug record. no-op unless SCHOOLYANK_DEBUG=1. `data` is
 * JSON-serialized and pretty-printed underneath the header line.
 *
 * pick a short, stable `section` tag (ORCH, SCRAPER, JUDGE, NCES, LINKEDIN,
 * EMAIL, AI, BROWSER, HASURA, VALIDATOR) so the log stream is greppable.
 */
export function debug(section: string, message: string, data?: unknown): void {
  if (!isDebug()) return;
  const line = `${C.dim}[DEBUG ${ts()}${C.reset} ${C.cyan}${section.padEnd(9)}${C.reset}${C.dim}]${C.reset} ${message}${fmtData(data)}`;
  process.stderr.write(line + "\n");
}

/** emit an especially noisy full-text block (e.g. full LLM prompts). */
export function debugBlock(section: string, message: string, body: string): void {
  if (!isDebug()) return;
  const header = `${C.dim}[DEBUG ${ts()}${C.reset} ${C.cyan}${section.padEnd(9)}${C.reset}${C.dim}]${C.reset} ${message} ${C.gray}(${body.length} chars)${C.reset}`;
  const indented = body.split("\n").map((l) => `    ${C.gray}│${C.reset} ${l}`).join("\n");
  process.stderr.write(`${header}\n${indented}\n`);
}

/** warning-flavored debug line — yellow header. */
export function debugWarn(section: string, message: string, data?: unknown): void {
  if (!isDebug()) return;
  const line = `${C.dim}[DEBUG ${ts()}${C.reset} ${C.yellow}${section.padEnd(9)}${C.reset}${C.dim}]${C.reset} ${C.yellow}${message}${C.reset}${fmtData(data)}`;
  process.stderr.write(line + "\n");
}
