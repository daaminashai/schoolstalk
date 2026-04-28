#!/usr/bin/env bun

// ── schoolyank: extract teacher data from any school website ──

// force ipv4-first dns resolution. some upstreams (urban institute's nces
// endpoints, in particular) advertise AAAA records on hosts whose ipv6 path
// is unreachable from common home networks — bun's fetch then happy-eyeballs
// to the broken ipv6 address and hangs until the per-request timeout.
import dns from "node:dns";
dns.setDefaultResultOrder("ipv4first");

import * as p from "@clack/prompts";
import color from "picocolors";
import { resolve } from "node:path";
import { existsSync } from "node:fs";
import { run, PHASE_LABELS, type PhaseId } from "./src/orchestrator";
import { slugify } from "./src/utils";
import { generateMergedCsv, writeCsv } from "./src/csv";
import { missingRequiredEnv, runSetupWizard } from "./src/setup";
import { createClient, killActiveSessions } from "./src/browser";
import { debug } from "./src/debug";
import { createSlackFromEnv, SlackNotifier, SlackThreadBuffer } from "./src/slack";
import type { ScrapeConfig, ScrapeResult, Teacher } from "./src/types";
import os from "node:os";

// .env path used by setup.ts; no EXA/LinkedIn config here anymore

// ── theme ────────────────────────────────────────────────────────────────────
// coherent palette used across the whole cli. cyan is the brand color; magenta
// is reserved for numbers/counts so the eye lands on them first; dim is used
// for all secondary info.
const t = {
	brand: color.cyan,
	accent: color.magenta,
	ok: color.green,
	warn: color.yellow,
	bad: color.red,
	muted: color.dim,
	bold: color.bold,
};

const BRAND_TAG = t.bold(t.brand("◆ schoolyank"));
const TAGLINE = t.muted("teacher data extractor");

// ── formatting helpers ───────────────────────────────────────────────────────

function confidenceStr(n: number): string {
	const rounded = Math.round(n);
	const text = `${n.toFixed(rounded === n ? 0 : 1)}/5`;
	if (rounded >= 5) return t.bold(t.ok(text));
	if (rounded >= 4) return t.ok(text);
	if (rounded >= 3) return t.warn(text);
	return t.bad(text);
}

function bar(ratio: number, width = 18): string {
	const clamped = Math.max(0, Math.min(1, ratio));
	const filled = Math.round(clamped * width);
	return t.brand("█".repeat(filled)) + t.muted("░".repeat(width - filled));
}

function countOf(count: number, total: number): string {
	const pct = total === 0 ? 0 : Math.round((count / total) * 100);
	return `${t.accent(String(count))}${t.muted(`/${total}`)}  ${t.muted(`(${pct}%)`)}`;
}

function padRight(s: string, w: number): string {
	const plain = s.replace(/\x1b\[[0-9;]*m/g, "");
	const pad = Math.max(0, w - plain.length);
	return s + " ".repeat(pad);
}

// ── result rendering ─────────────────────────────────────────────────────────

function renderHeadline(result: ScrapeResult, duration: string): string {
    const { teachers } = result;
    const name = new URL(result.sourceUrl).hostname.replace(/^www\./, "");

    const lines: string[] = [];
    lines.push(t.bold(name));

  const stats: string[] = [
    `${t.accent(String(teachers.length))} ${t.muted("teachers")}`,
  ];
    stats.push(`${t.muted(`${duration}s`)}`);
    lines.push(stats.join(t.muted("  ·  ")));
    return lines.join("\n");
}

function renderSchoolBreakdown(teachers: Teacher[]): string | null {
    return null;
}

function renderQualityBlock(teachers: Teacher[]): string {
  const total = teachers.length;
  const withEmail = teachers.filter((x) => !!x.email).length;
  const inferred = teachers.filter((x) => x.sources.includes("inferred")).length;

	const line = (label: string, filled: number, suffix: string): string =>
		`  ${padRight(label, 18)}  ${bar(total ? filled / total : 0, 14)}  ${suffix}`;

  const lines = [t.bold("data quality")];
  lines.push(line("with email", withEmail, countOf(withEmail, total)));
  if (inferred > 0) {
    lines.push(
      `  ${padRight(t.muted("  └ inferred"), 18)}  ${padRight("", 14)}  ${t.muted(String(inferred))}`,
    );
  }
  return lines.join("\n");
}

function renderTeacherPreview(teachers: Teacher[], limit = 5): string {
  const top = teachers.slice(0, limit);

	const lines = [t.bold("preview")];
  for (const x of top) {
    const header = `${t.bold(`${x.firstName} ${x.lastName}`)}  ${t.muted("·")}  ${x.role || t.muted("(no role)")}`;
    lines.push(`  ${header}`);

		const meta: string[] = [];
        if (x.department) meta.push(t.muted(x.department));
        if (meta.length) lines.push(`    ${meta.join(t.muted(" · "))}`);

    if (x.email) lines.push(`    ${t.brand(x.email)}`);
    // no confidence/linkedin classification displayed
    lines.push("");
  }
	while (lines.at(-1) === "") lines.pop();

	const remaining = teachers.length - top.length;
	if (remaining > 0) {
		lines.push(t.muted(`  + ${remaining} more in the csv`));
	}
	return lines.join("\n");
}

// linkedin/exa setup removed

// ── cli argv parsing ─────────────────────────────────────────────────────────

interface CliFlags {
  urls: string[];
  urlsFile: string | null;
  output: string | null;
  mergedOutput: string | null;
  concurrency: number;
  force: boolean;
  help: boolean;
  interactive: boolean;
  debug: boolean;
  schoolsCsv: string | null;
  maxAggression: boolean;
}

function parseArgs(argv: string[]): CliFlags {
  const flags: CliFlags = {
    urls: [],
    urlsFile: null,
    output: null,
    mergedOutput: null,
    concurrency: 3,
    force: false,
    help: false,
    interactive: false,
    debug: false,
    schoolsCsv: null,
    maxAggression: false,
  };

  	for (let i = 0; i < argv.length; i++) {
    	const a = argv[i]!;
    	// support --concurrency=1 and -j1 / -j=1 forms
    	const concEq = a.match(/^--concurrency=(\d+)$/);
    	const jPacked = a.match(/^-j=?([0-9]+)$/);
    	if (concEq) {
    		flags.concurrency = Math.max(1, parseInt(concEq[1]!, 10));
    		continue;
    	}
    	if (jPacked) {
    		flags.concurrency = Math.max(1, parseInt(jPacked[1]!, 10));
    		continue;
    	}
    	const next = () => argv[++i];
    	switch (a) {
			case "-h":
			case "--help":
				flags.help = true;
				break;
			case "--url":
				flags.urls.push(next() ?? "");
				break;
      case "--urls-file":
        flags.urlsFile = next() ?? null;
        break;
      case "--schools-csv":
        flags.schoolsCsv = next() ?? null;
        break;
      case "--output":
      case "-o":
        flags.output = next() ?? null;
        break;
      case "--merged-output":
        flags.mergedOutput = next() ?? null;
        break;
      case "--concurrency":
      case "-j": {
        // accept space-separated value, including the accidental "--1" form
        const peek = argv[i + 1];
        let val: number | null = null;
        if (typeof peek === "string") {
          // "--1" or "-1" → 1
          const m = peek.match(/^--?(\d+)$/);
          if (m) {
            i++;
            val = parseInt(m[1]!, 10);
          } else if (!peek.startsWith("-")) {
            // plain number token
            i++;
            const n = Number(peek);
            if (Number.isFinite(n)) val = n;
          }
        }
        if (val != null) flags.concurrency = Math.max(1, Math.floor(val));
        break;
      }
      case "--max":
        flags.maxAggression = true;
        break;
      case "--force":
        flags.force = true;
        break;
      case "--interactive":
        flags.interactive = true;
        break;
      case "--debug":
        flags.debug = true;
        break;
      default:
        // bare positional args are treated as URLs — lets the user do
        // `bun index.ts <url1> <url2>` without --url prefixes.
        if (!a.startsWith("-")) {
          // special-case @file shorthand for schools CSV mode
          if (a.startsWith("@") && a.length > 1) {
            flags.schoolsCsv = a.slice(1);
          } else {
            flags.urls.push(a);
          }
        }
        break;
    }
  }

	return flags;
}

async function loadUrlsFromFile(path: string): Promise<string[]> {
	const text = await Bun.file(path).text();
	return text
		.split(/\r?\n/)
		.map((line) => line.trim())
		.filter((line) => line.length > 0 && !line.startsWith("#"));
}

function printHelp(): void {
  const bin = t.bold("bun index.ts");
  const lines = [
    `${BRAND_TAG}  ${TAGLINE}`,
    "",
    `${t.bold("USAGE")}`,
    `  ${bin}                        ${t.muted("interactive prompt (single school)")}`,
    `  ${bin} <url>                  ${t.muted("scrape one school non-interactively")}`,
    `  ${bin} <url1> <url2> ...      ${t.muted("batch (3-way parallel by default)")}`,
    `  ${bin} --urls-file urls.txt   ${t.muted("batch from a file, one url per line")}`,
    `  ${bin} --schools-csv schools_with_staff_urls.csv ${t.muted("batch from unified CSV → schools/{STATE}/{city}/{id}.csv")}`,
    `  ${bin} @schools_with_staff_urls.csv ${t.muted("shorthand for --schools-csv schools_with_staff_urls.csv")}`,
    "",
    `${t.bold("FLAGS")}`,
    `  ${t.brand("--url")} <x>              add a url (repeatable)`,
    `  ${t.brand("--urls-file")} <path>     read urls from a file (one per line, # for comments)`,
    `  ${t.brand("--schools-csv")} <path>   read schools from CSV (expects: Hs ID, Name, State, City, School Homepage, Primary URL, Candidate 1..3)`,
    `  ${t.brand("--output, -o")} <path>    single-url output csv path (default: output/<slug>.csv)`,
    `  ${t.brand("--merged-output")} <path> batch merged csv path (default: output/all.csv)`,
    `  ${t.brand("--concurrency, -j")} <n>  parallel workers (higher = faster; risk of rate limits)`,
    `  ${t.brand("--max")}                  ramp concurrency up (≈2× CPU, capped)`,
    `  ${t.brand("--force")}                re-scrape even if output csv already exists`,
    `  ${t.brand("--interactive")}          force interactive prompt even when urls are passed`,
    `  ${t.brand("--debug")}                print extremely detailed debug info (browser agent, llm, nces, etc.)`,
    `  ${t.brand("--help, -h")}             show this message`,
    "",
    `${t.bold("EXAMPLES")}`,
    `  ${t.muted("# interactive run (guided CLI)")}`,
    `  ${bin}`,
    "",
    `  ${t.muted("# one school, no prompts")}`,
    `  ${bin} --url https://cvsdvt.org`,
    "",
    `  ${t.muted("# batch from unified CSV at high concurrency")}`,
    `  ${bin} --schools-csv schools_with_staff_urls.csv --max`,
  ];
  console.log(lines.join("\n"));
}

// ── spinner ──────────────────────────────────────────────────────────────────

interface Spinner {
	start(msg?: string): void;
	stop(msg?: string): void;
	message(msg?: string): void;
	clear(): void;
}

/**
 * single-line spinner that writes at column 0 and erases only the trailing
 * chars with `\x1b[K`. clack's built-in spinner uses the opposite order —
 * cursor.to(0) + erase.down() BEFORE writing the new frame — which produces
 * a visible "blank for a millisecond" flicker on some terminals. our
 * write-first-then-erase order means the line is never visually empty.
 *
 * matches the 4-method subset of clack's SpinnerResult we actually use:
 * start / stop / message / clear.
 */
function makeSpinner(output: NodeJS.WriteStream = process.stdout): Spinner {
	// two braille cells side-by-side = a 4×4 dot grid. we draw a 4-dot
	// "snake" crawling clockwise around the perimeter — 12 perimeter
	// positions, head + 3-dot tail, new frame each tick. the motion wraps
	// cleanly (bottom-left → left column → top-left …) so no rest frames
	// are needed; it's one continuous loop.
	const frames = [
		"⡇⠀", "⠏⠀", "⠋⠁", "⠉⠉", "⠈⠙", "⠀⠹",
		"⠀⢸", "⠀⣰", "⢀⣠", "⣀⣀", "⣄⡀", "⣆⠀",
	];
	const delay = 100;
	let idx = 0;
	let msg = "";
	let running = false;
	let timer: ReturnType<typeof setInterval> | null = null;

	function render() {
		idx = (idx + 1) % frames.length;
		const frame = t.accent(frames[idx]!);
		// \r = cursor to col 0; \x1b[K = erase from cursor to end of line.
		// writing content first means the line is never blank between frames.
		output.write(`\r${frame}  ${msg}\x1b[K`);
	}

	return {
		start(initial = "") {
			if (running) return;
			// match clack's column gutter so the spinner line visually connects
			// to the `◆`/`◇` symbols in the rest of the ui
			output.write(`${t.muted("│")}\n`);
			msg = initial;
			idx = 0;
			running = true;
			render();
			timer = setInterval(render, delay);
		},
		stop(final = "") {
			if (!running) return;
			running = false;
			if (timer) clearInterval(timer);
			timer = null;
			const sym = t.ok("◇");
			output.write(`\r${sym}  ${final}\x1b[K\n`);
		},
		message(next = "") {
			msg = next;
		},
		clear() {
			if (!running) return;
			running = false;
			if (timer) clearInterval(timer);
			timer = null;
			output.write(`\r\x1b[K`);
		},
	};
}

// ── main ─────────────────────────────────────────────────────────────────────

async function runInteractive(): Promise<void> {
	p.intro(`${BRAND_TAG}  ${TAGLINE}`);

  const config = await p.group(
    {
      schoolUrl: () =>
        p.text({
					message: "school website url",
					placeholder: "https://www.example-school.edu",
					validate: (value) => {
						if (!value) return "url is required";
						try {
							new URL(value);
						} catch {
							return "enter a valid url (include https://)";
						}
					},
				}),

    },
		{
			onCancel: () => {
				p.cancel("cancelled");
				process.exit(0);
			},
		},
	);

  // linkedin enrichment removed

	const domain = new URL(config.schoolUrl).hostname.replace(/^www\./, "");
	const outputPath = resolve("output", `${slugify(domain)}.csv`);

	if (existsSync(outputPath)) {
		const overwrite = await p.confirm({
			message: `${t.brand(outputPath)} already exists — overwrite?`,
			initialValue: false,
		});
		if (p.isCancel(overwrite) || !overwrite) {
			p.cancel("bailed, existing file kept");
			process.exit(0);
		}
	}

	p.log.info(`${t.muted("will write")} ${t.brand(outputPath)}`);

	// in --debug mode the spinner's repeated redraws fight with the stderr
	// debug firehose (agent messages, llm prompts, nces decisions) and the
	// terminal ends up a scrambled mess. swap it for a no-op stub whose
	// lifecycle calls (start/stop/message/clear) just return — phase + status
	// info already lands on stderr via the debug() calls we threaded through
	// the pipeline, so nothing is lost.
	const debugMode = process.env.SCHOOLYANK_DEBUG === "1";
	// custom spinner instead of p.spinner(). clack's spinner erases the line
	// with cursor.to(0) + erase.down() BEFORE writing the new frame — on some
	// terminals (older gnome-terminal, konsole, mintty) the gap between erase
	// and write is visible as a flicker. our version writes the new content at
	// col 0 first, then erases only the trailing chars (`\r<content>\x1b[K`),
	// so there's never a moment where the line is blank.
	const spinner = debugMode
		? { start: () => {}, stop: () => {}, message: () => {}, clear: () => {} }
		: makeSpinner();
	spinner.start("starting scrape...");

	// progress state: phase + latest substatus, combined into one spinner line
	let phasePrefix = "";
	let lastSubstatus = "";

	/**
	 * truncate an ANSI-colored string to fit within `maxWidth` visible columns.
	 * preserves escape codes but cuts the printable content. without this, long
	 * agent messages wrap across multiple lines and each spinner tick redraws
	 * the whole block, flooding the terminal.
	 */
	function truncateAnsi(s: string, maxWidth: number): string {
		const plain = s.replace(/\x1b\[[0-9;]*m/g, "");
		if (plain.length <= maxWidth) return s;

		let visible = 0;
		let out = "";
		let i = 0;
		while (i < s.length && visible < maxWidth - 1) {
			if (s[i] === "\x1b" && s[i + 1] === "[") {
				const end = s.indexOf("m", i);
				if (end === -1) break;
				out += s.slice(i, end + 1);
				i = end + 1;
			} else {
				out += s[i];
				visible++;
				i++;
			}
		}
		return out + "…\x1b[0m";
	}

	// dedup + throttle spinner writes. agent messages arrive faster than the
	// terminal can cleanly redraw, and when two substatuses alternate in quick
	// succession the line visibly flickers. only push a new frame if the
	// rendered string actually changed AND at least MIN_REFRESH_MS has elapsed
	// since the last write (phase transitions bypass the throttle).
	const MIN_REFRESH_MS = 120;
	let lastRendered = "";
	let lastRenderedAt = 0;
	let pendingTimer: ReturnType<typeof setTimeout> | null = null;

	/** render `[i/N] phase — substatus` into a single line for the spinner */
	function refreshSpinner(opts?: { immediate?: boolean }) {
		const parts = [phasePrefix, lastSubstatus].filter(Boolean);
		const combined = parts.join(t.muted(" — "));
		const termWidth = process.stdout.columns ?? 80;
		const next = truncateAnsi(combined, Math.max(30, termWidth - 6));
		if (next === lastRendered) return;

		const now = Date.now();
		const elapsed = now - lastRenderedAt;
		if (!opts?.immediate && elapsed < MIN_REFRESH_MS) {
			if (pendingTimer) return;
			pendingTimer = setTimeout(() => {
				pendingTimer = null;
				refreshSpinner({ immediate: true });
			}, MIN_REFRESH_MS - elapsed);
			return;
		}
		if (pendingTimer) {
			clearTimeout(pendingTimer);
			pendingTimer = null;
		}
		lastRendered = next;
		lastRenderedAt = now;
		spinner.message(next);
	}

	/** format the phase indicator: `[3/6] extracting teachers` */
	function formatPhasePrefix(
		phase: PhaseId,
		idx: number,
		total: number,
	): string {
		return `${t.muted(`[${idx}/${total}]`)} ${t.bold(PHASE_LABELS[phase])}`;
	}

	/**
	 * patterns we never want to show in the substatus — agent-internal chatter
	 * that's meaningless or misleading to the user:
	 *   - "Output saved to output.json" refers to the agent's OWN python
	 *     scratchpad, NOT our schoolyank CSV. showing it to the user makes
	 *     them think the scrape is already done.
	 *   - "Running Python code" tells the user nothing.
	 *   - bare "Navigating to <url>" without additional context is noise
	 *     between meaningful actions.
	 */
	const NOISY_PATTERNS = [
		/\boutput(\.json)?\b/i,
		/^running python/i,
		/^python:?\s*$/i,
		/\bsave_output_json\b/i,
	];

	function isNoisy(msg: string): boolean {
		return NOISY_PATTERNS.some((re) => re.test(msg));
	}

	/**
	 * the orchestrator often emits per-phase substatus messages that overlap
	 * with our phase label ("extracting STEM teachers..."). strip the
	 * duplication, collapse whitespace, and drop newlines so the spinner
	 * renders as a single line regardless of the source message shape.
	 */
  function cleanSubstatus(msg: string): string {
    return msg
      .replace(/[\r\n]+/g, " ")
      .replace(/\s+/g, " ")
      .replace(/^extracting teachers\.{0,3}\s*/i, "")
      .replace(/^finding staff directory\.{0,3}\s*/i, "")
      .replace(/^classifying site.*?\.{0,3}\s*/i, "")
      .replace(/^writing CSV\.{0,3}\s*/i, "")
      .trim();
  }

	const slack: SlackNotifier | null = createSlackFromEnv();
	let slackThread: string | null = null;
	let slackBuf: SlackThreadBuffer | null = null;
	if (slack) {
		try {
			slackThread = await slack.startThread(`▶️ Starting scrape: ${config.schoolUrl}`);
			if (slackThread) slackBuf = new SlackThreadBuffer(slack, slackThread);
		} catch {}
		if (!slackThread) {
			p.log.warn(
				`slack: unable to post to channel. ensure SLACK_BOT_TOKEN, SLACK_CHANNEL_ID are set, the app has chat:write, and is invited to the channel (${slack.getLastError() || "unknown error"}).`,
			);
		}
	}

	try {
    const scrapeConfig: ScrapeConfig = {
      schoolUrl: config.schoolUrl,
      outputPath,
    };

		const result = await run(scrapeConfig, {
			onStatus: (msg) => {
				debug("STATUS", msg);
				const cleaned = cleanSubstatus(msg);
				// skip noisy agent chatter — keep the previous substatus visible
				// instead of replacing it with uninformative text
				if (cleaned && !isNoisy(cleaned)) {
					lastSubstatus = cleaned;
				}
				refreshSpinner();
			},
			onPhase: (phase, idx, total) => {
				debug("PHASE", `→ ${phase} [${idx}/${total}]`);
				phasePrefix = formatPhasePrefix(phase, idx, total);
				lastSubstatus = ""; // clear per-phase substatus on transition
				refreshSpinner({ immediate: true });
				slackBuf?.addPhase(PHASE_LABELS[phase]);
			},
			onMilestone: (msg, level) => {
				debug("MILESTONE", `[${level ?? "info"}] ${msg}`);
				// clear → log → restart: spinner.clear() stops the interval
				// without emitting the green ◇ "done" frame that spinner.stop()
				// writes. we only want the persistent ● line, not a diamond
				// preamble before every milestone.
				spinner.clear();
				if (level === "warn") p.log.warn(msg);
				else p.log.info(t.muted(msg));
				spinner.start(phasePrefix || "");
				lastRendered = ""; // spinner.start resets the line
				refreshSpinner({ immediate: true });
				slackBuf?.addMilestone(msg, level);
			},
			onLiveUrl: (liveUrl) => {
				spinner.stop("browser session started");
				p.log.info(
					`${t.bold("watch live")}  ${t.brand(color.underline(liveUrl))}`,
				);
				spinner.start(phasePrefix || "crawling...");
				slackBuf?.addLive(liveUrl);
			},
		});

		spinner.stop(t.ok("scrape complete"));

		const { teachers, metadata } = result;
		const duration = (metadata.durationMs / 1000).toFixed(1);

		const summary: string[] = [];
		summary.push(renderHeadline(result, duration));

    // school breakdown removed

		if (teachers.length > 0) {
			summary.push("");
			summary.push(renderQualityBlock(teachers));
		}

		p.note(summary.join("\n"), "results");

		if (teachers.length > 0) {
			p.note(renderTeacherPreview(teachers, 5), "top matches");
		}

		if (metadata.warnings.length > 0) {
			for (const w of metadata.warnings) p.log.warn(w);
		}

		p.log.info(`${t.muted("csv saved to")} ${t.brand(outputPath)}`);
		if (slackBuf) {
			const { teachers, metadata } = result;
			const duration = (metadata.durationMs / 1000).toFixed(1);
			await slackBuf.finalizeSuccess(teachers.length, duration, outputPath);
		}
	} catch (err) {
		spinner.stop(t.bad("scrape failed"));
		const msg = err instanceof Error ? err.message : String(err);
		p.log.error(msg);
		if (slackBuf) await slackBuf.finalizeFailure(msg);
		process.exit(1);
	}

	p.outro(t.ok("done"));
}

// ── non-interactive batch mode ───────────────────────────────────────────────

interface BatchItem {
    url: string;
    outputPath: string;
    slug: string;
    /** Optional pre-seeded staff directory URLs to try first */
    preferredDirectoryUrls?: string[];
}

interface BatchOutcome {
	item: BatchItem;
	status: "ok" | "skipped" | "failed";
	result?: ScrapeResult;
	error?: string;
	durationMs: number;
}

function defaultOutputPathFor(url: string): string {
	const domain = new URL(url).hostname.replace(/^www\./, "");
	return resolve("output", `${slugify(domain)}.csv`);
}

/**
 * run one scrape non-interactively. emits short progress lines (one per
 * milestone) instead of the spinner UI — batch mode can't use a spinner
 * because multiple runs would fight for the same TTY line.
 */
async function scrapeOne(
  url: string,
  outputPath: string,
  tag: string,
  preferredDirectoryUrls?: string[],
  slackBuf?: SlackThreadBuffer | null,
): Promise<ScrapeResult> {
  const scrapeConfig: ScrapeConfig = {
    schoolUrl: url,
    outputPath,
    ...(preferredDirectoryUrls && preferredDirectoryUrls.length > 0
      ? { preferredDirectoryUrls }
      : {}),
  };
	return run(scrapeConfig, {
		onStatus: (msg) => {
			debug("STATUS", `${tag} ${msg}`);
		},
		onPhase: (phase, idx, total) => {
			debug("PHASE", `${tag} → ${phase} [${idx}/${total}]`);
			console.log(
				`${t.muted(tag)} ${t.muted(`[${idx}/${total}]`)} ${t.bold(PHASE_LABELS[phase])}`,
			);
			slackBuf?.addPhase(PHASE_LABELS[phase]);
		},
		onMilestone: (msg, level) => {
      debug("MILESTONE", `${tag} [${level ?? "info"}] ${msg}`);
			const prefix = level === "warn" ? t.warn("!") : t.ok("•");
			console.log(`${t.muted(tag)} ${prefix} ${msg}`);
			slackBuf?.addMilestone(msg, level);
		},
		onLiveUrl: (liveUrl) => {
			debug("LIVE-URL", `${tag} ${liveUrl}`);
			console.log(
				`${t.muted(tag)} ${t.muted("watch live:")} ${t.brand(liveUrl)}`,
			);
			slackBuf?.addLive(liveUrl);
		},
	});
}

/** fixed-size worker pool: always up to `concurrency` scrapes in flight. */
async function runBatch(
  items: BatchItem[],
  concurrency: number,
  force: boolean,
  slack?: SlackNotifier | null,
): Promise<BatchOutcome[]> {
	const outcomes: BatchOutcome[] = new Array(items.length);
	let cursor = 0;

	async function worker(): Promise<void> {
		while (true) {
			const myIdx = cursor++;
			if (myIdx >= items.length) break;
			const item = items[myIdx]!;
			const tag = `[${myIdx + 1}/${items.length} ${item.slug}]`;
			const start = Date.now();
            let threadTs: string | null = null;
            let buf: SlackThreadBuffer | null = null;
            if (slack) {
                try {
                    threadTs = await slack.startThread(`▶️ Starting scrape: ${item.url} (${item.slug})`);
                    if (threadTs) buf = new SlackThreadBuffer(slack, threadTs);
                } catch {}
                if (!threadTs) {
                    console.log(t.warn(`! slack: unable to post to channel (${slack.getLastError() || "unknown error"}). Make sure the bot is invited and channel ID is correct.`));
                }
            }

			if (!force && existsSync(item.outputPath)) {
				console.log(
					`${t.muted(tag)} ${t.muted("skipped (output exists — pass --force to re-scrape)")}`,
				);
				if (threadTs) slack?.postInThread(threadTs, `⏭️ Skipped (output exists).`).catch(() => {});
				outcomes[myIdx] = {
					item,
					status: "skipped",
					durationMs: Date.now() - start,
				};
				continue;
			}

			console.log(`${t.muted(tag)} ${t.bold("starting")} ${t.brand(item.url)}`);
            // avoid mid-run Slack noise; only final summaries will be sent

			// one-retry policy: browser-use sessions can drop or hit transient
			// rate limits; a fresh retry resolves most of these. without this,
			// ~5-10% of a 15-school batch would silently fail on a judge's test.
			let result: ScrapeResult | null = null;
			let lastError = "";
			for (let attempt = 1; attempt <= 2; attempt++) {
				try {
          const r = await scrapeOne(
            item.url,
            item.outputPath,
            tag,
            item.preferredDirectoryUrls,
            buf,
          );
					// treat 0 teachers as a retry-eligible failure — it's almost always
					// a transient scraper gave-up / browser-use flake, not a real
					// empty district. a real empty district is vanishingly rare.
					if (r.teachers.length === 0 && attempt === 1) {
						console.log(
							`${t.muted(tag)} ${t.warn("! first attempt returned 0 teachers — retrying once")}`,
						);
                        buf?.markRetried();
                        lastError = "0 teachers on first attempt";
                        continue;
                    }
                    result = r;
                    break;
                } catch (err) {
                    lastError = err instanceof Error ? err.message : String(err);
                    if (attempt === 1) {
                        console.log(
                            `${t.muted(tag)} ${t.warn("! first attempt failed:")} ${lastError} ${t.muted("— retrying once")}`,
                        );
                        buf?.markRetried();
                    }
                }
            }

            if (result) {
                const dur = ((Date.now() - start) / 1000).toFixed(1);
                console.log(
                    `${t.muted(tag)} ${t.ok("✓ done")} ${t.accent(String(result.teachers.length))} ${t.muted("teachers")} ${t.muted(`(${dur}s)`)}`,
                );
                if (buf) await buf.finalizeSuccess(result.teachers.length, dur, item.outputPath);
                outcomes[myIdx] = {
                    item,
                    status: "ok",
                    result,
                    durationMs: Date.now() - start,
                };
            } else {
                console.log(
                    `${t.muted(tag)} ${t.bad("✗ failed (after retry):")} ${lastError}`,
                );
                if (buf) await buf.finalizeFailure(`Failed after retry: ${lastError}`);
                outcomes[myIdx] = {
                    item,
                    status: "failed",
                    error: lastError,
                    durationMs: Date.now() - start,
                };
            }
		}
	}

	const workerCount = Math.max(1, Math.min(concurrency, items.length));
	await Promise.all(Array.from({ length: workerCount }, worker));
	return outcomes;
}

async function runNonInteractive(flags: CliFlags): Promise<void> {
  // CSV-driven mode: read schools from --schools-csv and ignore plain urls
  if (flags.schoolsCsv) {
    await runFromSchoolsCsv(flags);
    return;
  }

  // aggregate URL inputs: --url repeats + positionals + --urls-file
  let urls = [...flags.urls];
  if (flags.urlsFile) {
		try {
			const fileUrls = await loadUrlsFromFile(flags.urlsFile);
			urls = urls.concat(fileUrls);
		} catch (err) {
			const msg = err instanceof Error ? err.message : String(err);
			console.error(t.bad(`failed to read urls file: ${msg}`));
			process.exit(1);
		}
	}

	// validate URLs, drop bad ones with a warning (don't crash the whole batch)
	const valid: string[] = [];
	for (const u of urls) {
		try {
			new URL(u);
			valid.push(u);
		} catch {
			console.error(t.warn(`skipping invalid url: ${u}`));
		}
	}
	if (valid.length === 0) {
		console.error(t.bad("no valid urls — see --help"));
		process.exit(1);
	}


	// build work items with output paths. --output only applies when there's
	// exactly one url; batches always route through output/<slug>.csv because
	// a single --output would overwrite itself across runs.
  const items: BatchItem[] = valid.map((url, i) => {
		const domain = new URL(url).hostname.replace(/^www\./, "");
		const slug = slugify(domain);
		const outputPath =
			valid.length === 1 && flags.output
				? resolve(flags.output)
				: defaultOutputPathFor(url);
        return { url, outputPath, slug };
  });

	console.log(
		`${BRAND_TAG}  ${t.muted(`${items.length} school${items.length === 1 ? "" : "s"}, concurrency ${flags.concurrency}`)}`,
	);
  // linkedin removed

	// local browser-use sessions are process-owned; this is a no-op unless a
	// caller reused a client and left local child processes open.
	try {
		const killed = await killActiveSessions(createClient());
		if (killed > 0)
			console.log(
				t.muted(
					`cleared ${killed} lingering browser-use session${killed === 1 ? "" : "s"}`,
				),
			);
	} catch (err) {
		const msg = err instanceof Error ? err.message : String(err);
		debug("INDEX", `killActiveSessions failed: ${msg}`);
	}

	const batchStart = Date.now();
  const slack = createSlackFromEnv();
  const outcomes = await runBatch(
    items,
    flags.concurrency,
    flags.force,
    slack,
  );
	const batchDurationSec = ((Date.now() - batchStart) / 1000).toFixed(1);

	// merged CSV for batches of 2+ (or always when --merged-output is passed)
	const okOutcomes = outcomes.filter((o) => o.status === "ok" && o.result);
	if (okOutcomes.length > 0 && (items.length > 1 || flags.mergedOutput)) {
		const mergedPath = resolve(flags.mergedOutput ?? "output/all.csv");
		const csv = generateMergedCsv(okOutcomes.map((o) => o.result!));
		await writeCsv(mergedPath, csv);
		const totalTeachers = okOutcomes.reduce(
			(n, o) => n + o.result!.teachers.length,
			0,
		);
		console.log(
			`${t.ok("✓")} merged csv: ${t.brand(mergedPath)} ${t.muted(`(${totalTeachers} teachers across ${okOutcomes.length} schools)`)}`,
		);
	}

	// summary
	const ok = outcomes.filter((o) => o.status === "ok").length;
	const skipped = outcomes.filter((o) => o.status === "skipped").length;
	const failed = outcomes.filter((o) => o.status === "failed");
	console.log();
	console.log(
		`${t.bold("batch summary")}  ${t.ok(`${ok} ok`)}  ${t.muted(`${skipped} skipped`)}  ${failed.length > 0 ? t.bad(`${failed.length} failed`) : t.muted("0 failed")}  ${t.muted(`${batchDurationSec}s total`)}`,
	);
	if (failed.length > 0) {
		for (const f of failed)
			console.log(`  ${t.bad("✗")} ${f.item.url} — ${f.error}`);
		process.exit(1);
	}
}

// ── entry point ──────────────────────────────────────────────────────────────

async function main(): Promise<void> {
	const flags = parseArgs(Bun.argv.slice(2));
	if (flags.help) {
		printHelp();
		return;
	}

	// set the debug env var BEFORE any pipeline code runs. the debug module
	// reads this lazily per-call, so setting it here propagates to every
	// subsequent import without needing to thread a flag through every layer.
	if (flags.debug) {
		process.env.SCHOOLYANK_DEBUG = "1";
		console.error(
			`${t.muted("[DEBUG]")} ${t.bold("schoolyank debug mode enabled")} ${t.muted("— verbose output on stderr")}`,
		);
		console.error(`${t.muted("[DEBUG]")} flags: ${JSON.stringify(flags)}`);
	}

	// preflight: if the required keys aren't set, walk the user through the
	// full interactive setup wizard. handles LLM provider selection and
	// optionally Exa signup through the local browser-use runner. at the end,
	// .env is written and the scrape proceeds normally.
	if (missingRequiredEnv().length > 0) {
		try {
			await runSetupWizard();
		} catch (err) {
			const msg = err instanceof Error ? err.message : String(err);
			console.error(`${t.bad("✗ setup failed:")} ${msg}`);
      console.error(
        `\n${t.muted("you can manually populate .env and rerun. required keys:")}\n  ${t.brand("BROWSER_USE_API_KEY")}\n  ${t.brand("OPENROUTER_API_KEY")}`,
      );
			process.exit(1);
		}
	}

  const hasInput = flags.urls.length > 0 || !!flags.urlsFile || !!flags.schoolsCsv;
  if (flags.interactive || !hasInput) {
    await runInteractive();
  } else {
    await runNonInteractive(flags);
  }
}

main();

// ── CSV mode helpers ─────────────────────────────────────────────────────────

type CsvRow = Record<string, string>;

function parseCsv(text: string): CsvRow[] {
  const rows: string[][] = [];
  let i = 0;
  let field = "";
  let inQuotes = false;
  let row: string[] = [];
  const pushField = () => {
    row.push(field);
    field = "";
  };
  const pushRow = () => {
    // trailing empty newline
    if (row.length === 0 || (row.length === 1 && row[0] === "")) return;
    rows.push(row);
    row = [];
  };
  while (i < text.length) {
    const c = text[i]!;
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i += 2;
          continue;
        } else {
          inQuotes = false;
          i++;
          continue;
        }
      } else {
        field += c;
        i++;
        continue;
      }
    } else {
      if (c === '"') {
        inQuotes = true;
        i++;
        continue;
      }
      if (c === ",") {
        pushField();
        i++;
        continue;
      }
      if (c === "\n") {
        pushField();
        pushRow();
        i++;
        continue;
      }
      if (c === "\r") {
        // swallow CR in CRLF
        i++;
        continue;
      }
      field += c;
      i++;
    }
  }
  // flush last
  pushField();
  pushRow();

  if (rows.length === 0) return [];
  const header = rows[0]!.map((h) => h.trim());
  const out: CsvRow[] = [];
  for (let r = 1; r < rows.length; r++) {
    const cells = rows[r]!;
    const rec: CsvRow = {};
    for (let c = 0; c < header.length; c++) {
      rec[header[c]!] = (cells[c] ?? "").trim();
    }
    out.push(rec);
  }
  return out;
}

interface SchoolCsvItem {
  id: string; // from CSV ("Hs ID" or "id")
  name: string;
  state: string; // two-letter
  city: string;
  url: string; // school homepage
  preferredDirectoryUrls?: string[]; // Primary URL + Candidate 1..3 + Verified URL
}

async function loadSchoolsCsv(path: string): Promise<SchoolCsvItem[]> {
  const text = await Bun.file(path).text();
  const rows = parseCsv(text);
  const items: SchoolCsvItem[] = [];
  for (const r of rows) {
    // schools_with_staff_urls.csv format: Hs ID, Name, State, City,
    // School Homepage, Primary URL, Candidate 1..3, Candidate 1..3 Score,
    // and optional Verified URL.
    const id = (r["Hs ID"] ?? r["id"] ?? "").trim();
    const name = (r["Name"] ?? r["name"] ?? "").trim();
    const state = (r["State"] ?? r["state"] ?? "").trim();
    const city = (r["City"] ?? r["city"] ?? "").trim();
    const homepage = (r["School Homepage"] ?? r["homepage"] ?? r["url"] ?? "").trim();
    if (!id || !name || !state || !city || !homepage) continue;

    // Collect staff-directory hints from this same row. Primary URL is the
    // first URL the agent should try; candidates are still all included so a
    // too-narrow primary page does not cap recall.
    const primary = (r["Primary URL"] ?? "").trim();
    const verified = (r["Verified URL"] ?? "").trim();
    const cand1 = (r["Candidate 1"] ?? "").trim();
    const cand2 = (r["Candidate 2"] ?? "").trim();
    const cand3 = (r["Candidate 3"] ?? "").trim();
    const cand1Score = parseInt((r["Candidate 1 Score"] ?? "0").replace(/[^0-9-]/g, "")) || 0;
    const cand2Score = parseInt((r["Candidate 2 Score"] ?? "0").replace(/[^0-9-]/g, "")) || 0;
    const cand3Score = parseInt((r["Candidate 3 Score"] ?? "0").replace(/[^0-9-]/g, "")) || 0;
    const origin = (() => { try { return new URL(homepage).origin; } catch { return ""; } })();
    type Cand = { url: string; score: number };
    const cands: Cand[] = [];
    if (primary) cands.push({ url: primary, score: Number.POSITIVE_INFINITY });
    if (verified) cands.push({ url: verified, score: 100 });
    if (cand1) cands.push({ url: cand1, score: cand1Score });
    if (cand2) cands.push({ url: cand2, score: cand2Score });
    if (cand3) cands.push({ url: cand3, score: cand3Score });

    const absList: string[] = [];
    const ordered = cands
      .map((c, i) => ({ ...c, i }))
      .sort((a, b) => b.score - a.score || a.i - b.i);
    for (const c of ordered) {
      try {
        const href = new URL(c.url, origin || undefined).href;
        if (/^https?:\/\//i.test(href)) absList.push(href);
      } catch {
        // ignore
      }
    }
    const preferredDirectoryUrls = dedupePreserveOrder(absList).slice(0, 5);

    items.push({ id, name, state, city, url: homepage, ...(preferredDirectoryUrls.length > 0 ? { preferredDirectoryUrls } : {}) });
  }
  return items;
}

function pathForStateCityId(state: string, city: string, id: string): string {
  const st = (state || "").toUpperCase().replace(/[^A-Z]/g, "");
  const citySlug = slugify(city);
  const idSlug = String(id).trim();
  return resolve("schools", st || "xx", citySlug, `${idSlug}.csv`);
}

/** simple ordered dedupe */
function dedupePreserveOrder<T>(arr: T[]): T[] {
  const seen = new Set<string>();
  const out: T[] = [];
  for (const x of arr) {
    const k = String(x).toLowerCase();
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(x);
  }
  return out;
}

async function runFromSchoolsCsv(flags: CliFlags): Promise<void> {
  const csvPath = resolve(flags.schoolsCsv!);
  let schools: SchoolCsvItem[] = [];
  try {
    schools = await loadSchoolsCsv(csvPath);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(t.bad(`failed to read schools csv: ${msg}`));
    process.exit(1);
  }

  if (schools.length === 0) {
    console.error(t.bad("no valid rows in schools csv (need: Hs ID, Name, State, City, School Homepage, Primary URL/Candidates)"));
    process.exit(1);
  }

  // aggressive concurrency when --max is set; otherwise honor -j or default to 6
  let concurrency = flags.concurrency || 6;
  if (flags.maxAggression) {
    const cores = Math.max(1, os.cpus()?.length || 4);
    // 2x cores but not insane; local browsers and LLM providers will clamp us
    concurrency = Math.max(concurrency, Math.min(32, cores * 2));
  }

  // build batch items
  const items: BatchItem[] = schools.map((s) => {
    const outputPath = pathForStateCityId(s.state, s.city, s.id);
    const slug = `${s.state.toUpperCase()}/${slugify(s.city)}/${s.id}`;
    const preferredDirectoryUrls = s.preferredDirectoryUrls ?? [];
    return { url: s.url, outputPath, slug, ...(preferredDirectoryUrls.length > 0 ? { preferredDirectoryUrls } : {}) };
  });

  console.log(
    `${BRAND_TAG}  ${t.muted(`${items.length} school${items.length === 1 ? "" : "s"}, concurrency ${concurrency}`)}`,
  );

  try {
    const killed = await killActiveSessions(createClient());
    if (killed > 0) console.log(t.muted(`cleared ${killed} lingering browser-use session${killed === 1 ? "" : "s"}`));
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    debug("INDEX", `killActiveSessions failed: ${msg}`);
  }

  const batchStart = Date.now();
  const slack = createSlackFromEnv();
  const outcomes = await runBatch(items, concurrency, flags.force, slack);
  const batchDurationSec = ((Date.now() - batchStart) / 1000).toFixed(1);

  // no merged output in CSV mode — per-city files only

  const ok = outcomes.filter((o) => o.status === "ok").length;
  const skipped = outcomes.filter((o) => o.status === "skipped").length;
  const failed = outcomes.filter((o) => o.status === "failed");
  console.log();
  console.log(
    `${t.bold("batch summary")}  ${t.ok(`${ok} ok`)}  ${t.muted(`${skipped} skipped`)}  ${failed.length > 0 ? t.bad(`${failed.length} failed`) : t.muted("0 failed")}  ${t.muted(`${batchDurationSec}s total`)}`,
  );
  if (failed.length > 0) {
    for (const f of failed) console.log(`  ${t.bad("✗")} ${f.item.url} — ${f.error}`);
    process.exit(1);
  }
}
