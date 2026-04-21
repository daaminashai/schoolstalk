// ── browser-use session management ──

import { BrowserUse } from "browser-use-sdk/v3";
import type { z } from "zod";
import { debug, debugBlock, isDebug } from "./debug";

export interface SessionInfo {
  id: string;
  liveUrl: string;
}

export interface ProfileInfo {
  id: string;
  name: string | null;
  cookieDomains: string[] | null;
  lastUsedAt: string | null;
}

// browser-use model tiers — see node_modules/browser-use-sdk/dist/v3.d.ts (BuModel).
// wider aliases ("bu-mini" etc.) also valid; we use the direct model names for clarity.
// NOTE: model is a per-run parameter on client.run(), not a session-level attribute.
export type BrowserModel =
  | "gemini-3-flash"
  | "claude-sonnet-4.6"
  | "claude-opus-4.6"
  | "gpt-5.4-mini";

/** create a browser-use client (picks up BROWSER_USE_API_KEY from env) */
export function createClient(): BrowserUse {
  return new BrowserUse();
}

/** create a new browser profile */
export async function createProfile(
  client: BrowserUse,
  name: string,
): Promise<ProfileInfo> {
  const profile = await client.profiles.create({ name });
  return {
    id: profile.id,
    name: profile.name ?? null,
    cookieDomains: profile.cookieDomains ?? null,
    lastUsedAt: profile.lastUsedAt ?? null,
  };
}

/** list existing profiles */
export async function listProfiles(client: BrowserUse): Promise<ProfileInfo[]> {
  const response = await client.profiles.list();
  return response.items.map((p) => ({
    id: p.id,
    name: p.name ?? null,
    cookieDomains: p.cookieDomains ?? null,
    lastUsedAt: p.lastUsedAt ?? null,
  }));
}

/** get a profile by id */
export async function getProfile(
  client: BrowserUse,
  profileId: string,
): Promise<ProfileInfo> {
  const profile = await client.profiles.get(profileId);
  return {
    id: profile.id,
    name: profile.name ?? null,
    cookieDomains: profile.cookieDomains ?? null,
    lastUsedAt: profile.lastUsedAt ?? null,
  };
}

/** spin up a new browser session, optionally tied to a linkedin profile */
export async function createSession(
  client: BrowserUse,
  options?: { profileId?: string },
): Promise<SessionInfo> {
  const session = await client.sessions.create({
    ...(options?.profileId && { profileId: options.profileId }),
  });

  return { id: session.id, liveUrl: session.liveUrl ?? "" };
}

// ── task running ────────────────────────────────────────────────────────────

interface RunTaskOptions {
  onMessage?: (msg: string) => void;
  /** browser-use model tier — default is claude-sonnet-4.6 server-side */
  model?: BrowserModel;
}

/**
 * browser-use agent messages that are internal chatter (its own python
 * scratchpad, file I/O about output.json, etc.) rather than useful progress
 * signal. filter them at the stream boundary so they never propagate upward
 * into the spinner. "Output saved to output.json" is particularly misleading
 * because users think that's THEIR output file (it's actually the agent's
 * internal python scratchpad — we capture data via the structured-output
 * schema, not the agent's file system).
 */
const NOISY_AGENT_PATTERNS = [
  /\boutput(\.json)?\b/i,
  /\bsave_output_json\b/i,
  /^\s*ran python\b/i,
  /^\s*running python\b/i,
  /^\s*python:?\s*$/i,
  /^\s*saved (to|in) /i,
  /\bwritten to\b/i,
  /\bscratchpad\b/i,
];

function isNoisyAgentMessage(summary: string): boolean {
  return NOISY_AGENT_PATTERNS.some((re) => re.test(summary));
}

/** format a browser-use stream message into a short, terminal-friendly line */
function formatBrowserMessage(summary: string): string {
  const termWidth = process.stdout.columns ?? 80;
  const maxLen = Math.max(40, termWidth - 10);
  const cleaned = summary.trim().replace(/\s+/g, " ");
  if (cleaned.length <= maxLen) return cleaned;
  return cleaned.slice(0, maxLen - 1) + "…";
}

/** the sdk types output as unknown | null — coerce into a usable string here */
function coerceOutput(output: unknown): string {
  if (output == null) return "";
  if (typeof output === "string") return output;
  try {
    return JSON.stringify(output);
  } catch {
    return String(output);
  }
}

/** drive the message stream with dedupe, noise-filter, and truncation */
async function drainMessages(
  run: AsyncIterable<{ role: string; summary?: string }>,
  onMessage: (msg: string) => void,
): Promise<void> {
  let last = "";
  let count = 0;
  for await (const msg of run) {
    const summary = (msg.summary ?? "").trim();
    if (!summary) continue;
    count++;
    // in debug mode, log EVERY agent message raw (including duplicates and
    // noisy ones) — this is exactly the firehose users want to inspect.
    if (isDebug()) {
      debug("BROWSER", `msg #${count} role=${msg.role} noisy=${isNoisyAgentMessage(summary)}`, { summary });
    }
    if (summary === last) continue;
    last = summary;
    // drop agent-internal chatter (file I/O about its scratchpad, etc.)
    // before it can reach the spinner. the user wants actionable progress,
    // not "Output saved to output.json" which refers to the agent's own
    // intermediate file rather than our schoolyank CSV.
    if (isNoisyAgentMessage(summary)) continue;
    onMessage(formatBrowserMessage(summary));
  }
  debug("BROWSER", `stream drained: ${count} total messages`);
}

/**
 * run a task inside an existing session.
 * returns the agent's free-form text output as a string.
 */
export async function runTask(
  client: BrowserUse,
  sessionId: string,
  prompt: string,
  options: RunTaskOptions = {},
): Promise<string> {
  const { onMessage, model } = options;

  debugBlock("BROWSER", `runTask start · session=${sessionId} model=${model ?? "(default)"}`, prompt);
  const taskStart = Date.now();

  try {
    if (onMessage || isDebug()) {
      const sink = onMessage ?? (() => {});
      const run = client.run(prompt, {
        sessionId,
        ...(model && { model }),
      });

      await drainMessages(run, sink);
      const output = coerceOutput(run.result?.output);
      debugBlock("BROWSER", `runTask done · ${((Date.now() - taskStart) / 1000).toFixed(2)}s`, output);
      return output;
    }

    const result = await client.run(prompt, {
      sessionId,
      ...(model && { model }),
    });
    return coerceOutput(result.output);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    debug("BROWSER", `runTask FAILED · ${((Date.now() - taskStart) / 1000).toFixed(2)}s`, { error: message });
    throw new Error(`browser task failed: ${message}`);
  }
}

/**
 * run a task with a zod schema for structured output — the agent is forced
 * to return data conforming to the schema, eliminating the "agent saves data
 * to a file and returns prose" failure mode that plagues free-form tasks.
 */
export async function runTaskStructured<T extends z.ZodType>(
  client: BrowserUse,
  sessionId: string,
  prompt: string,
  schema: T,
  options: RunTaskOptions = {},
): Promise<z.output<T>> {
  const { onMessage, model } = options;

  debugBlock("BROWSER", `runTaskStructured start · session=${sessionId} model=${model ?? "(default)"}`, prompt);
  const taskStart = Date.now();

  try {
    if (onMessage || isDebug()) {
      const sink = onMessage ?? (() => {});
      const run = client.run(prompt, {
        sessionId,
        schema,
        ...(model && { model }),
      });

      await drainMessages(run, sink);
      // structured runs return typed data in run.result.output
      const out = run.result?.output;
      if (out == null) {
        debug("BROWSER", `runTaskStructured returned NULL · ${((Date.now() - taskStart) / 1000).toFixed(2)}s`);
        throw new Error("structured task returned null output");
      }
      debug("BROWSER", `runTaskStructured done · ${((Date.now() - taskStart) / 1000).toFixed(2)}s`, out);
      return out as z.output<T>;
    }

    const result = await client.run(prompt, {
      sessionId,
      schema,
      ...(model && { model }),
    });
    if (result.output == null) {
      throw new Error("structured task returned null output");
    }
    return result.output as z.output<T>;
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    debug("BROWSER", `runTaskStructured FAILED · ${((Date.now() - taskStart) / 1000).toFixed(2)}s`, { error: message });
    throw new Error(`browser task failed: ${message}`);
  }
}

/** stop a session and clean up resources */
export async function stopSession(
  client: BrowserUse,
  sessionId: string,
): Promise<void> {
  await client.sessions.stop(sessionId);
}

/**
 * kill every non-terminal browser-use session on the current project.
 *
 * the free tier caps active sessions at 3. if a previous run was aborted
 * (ctrl-c, crash, timeout) its sessions can linger past their idle timeout
 * and burn that quota, so `sessions.create` on the next run returns a 429
 * before the first task even starts. calling this at startup clears the
 * decks idempotently.
 *
 * returns the count of sessions we actively asked to stop. errors on
 * individual stops are swallowed — a session that already terminated on
 * its own isn't a failure we care about here.
 */
export async function killActiveSessions(client: BrowserUse): Promise<number> {
  let killed = 0;
  try {
    const response = await client.sessions.list({ page_size: 100 });
    const sessions = (response as { sessions?: Array<{ id: string; status?: string }> }).sessions ?? [];
    const active = sessions.filter((s) => {
      const st = (s.status ?? "").toLowerCase();
      return st !== "stopped" && st !== "timed_out" && st !== "error" && st !== "finished";
    });
    debug("BROWSER", `killActiveSessions · found ${active.length} active / ${sessions.length} total`);
    await Promise.all(
      active.map(async (s) => {
        try {
          await client.sessions.stop(s.id);
          killed++;
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          debug("BROWSER", `killActiveSessions · could not stop ${s.id}: ${msg}`);
        }
      }),
    );
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    debug("BROWSER", `killActiveSessions · list failed: ${msg}`);
  }
  return killed;
}
