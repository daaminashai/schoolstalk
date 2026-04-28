// -- local browser-use session management --

import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import { debug, debugBlock, isDebug } from "./debug";
import { retryOnRateLimit } from "./retry";

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

export type BrowserModel = "default" | "extract";

interface RunTaskOptions {
  onMessage?: (msg: string) => void;
  /** local model slot; resolved by the Python runner from env overrides */
  model?: BrowserModel;
}

interface BrowserClient {
  sessions: Map<string, LocalBrowserSession>;
}

interface PendingRun {
  resolve: (value: unknown) => void;
  reject: (err: Error) => void;
  onMessage?: (msg: string) => void;
}

interface ProtocolMessage {
  type: "ready" | "progress" | "done" | "error";
  request_id?: number;
  session_id?: string;
  message?: string;
  output?: unknown;
}

const PROTOCOL_PREFIX = "__SCHOOLYANK_BROWSER_USE__";
const PROJECT_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const LOCAL_SCRIPT = resolve(
  PROJECT_ROOT,
  "scripts",
  "local_browser_use_agent.py",
);
let localBrowserStartupQueue: Promise<void> = Promise.resolve();

/** create a local browser-use client. no cloud API key is used. */
export function createClient(): BrowserClient {
  return { sessions: new Map() };
}

/** local Browser Use does not have cloud profiles; keep stubs for callers. */
export async function createProfile(_client: BrowserClient, name: string): Promise<ProfileInfo> {
  return { id: name, name, cookieDomains: null, lastUsedAt: null };
}

export async function listProfiles(_client: BrowserClient): Promise<ProfileInfo[]> {
  return [];
}

export async function getProfile(_client: BrowserClient, profileId: string): Promise<ProfileInfo> {
  return { id: profileId, name: profileId, cookieDomains: null, lastUsedAt: null };
}

/** spin up a new local browser-use process/session. */
export async function createSession(
  client: BrowserClient,
  _options?: { profileId?: string },
): Promise<SessionInfo> {
  const session = await withSerializedLocalBrowserStartup(async () => {
    const nextSession = new LocalBrowserSession();
    try {
      await nextSession.ready;
      return nextSession;
    } catch (err) {
      await nextSession.stop().catch(() => {});
      throw err;
    }
  });
  client.sessions.set(session.id, session);
  return { id: session.id, liveUrl: "" };
}

async function withSerializedLocalBrowserStartup<T>(fn: () => Promise<T>): Promise<T> {
  const previous = localBrowserStartupQueue;
  let release!: () => void;
  localBrowserStartupQueue = new Promise<void>((resolveQueue) => {
    release = resolveQueue;
  });

  await previous.catch(() => {});
  try {
    return await fn();
  } finally {
    release();
  }
}

class LocalBrowserSession {
  readonly id = `local-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  readonly proc: ChildProcessWithoutNullStreams;
  readonly ready: Promise<void>;

  private readyResolve!: () => void;
  private readyReject!: (err: Error) => void;
  private pending = new Map<number, PendingRun>();
  private stdoutBuffer = "";
  private stderrTail = "";
  private nextRequestId = 1;
  private stopped = false;
  private readySettled = false;

  constructor() {
    this.ready = new Promise((resolveReady, rejectReady) => {
      this.readyResolve = resolveReady;
      this.readyReject = rejectReady;
    });

    this.proc = spawn(pythonExecutable(), [LOCAL_SCRIPT], {
      cwd: PROJECT_ROOT,
      env: {
        ...process.env,
        SCHOOLYANK_BROWSER_SESSION_ID: this.id,
        TIMEOUT_BrowserStartEvent: process.env.TIMEOUT_BrowserStartEvent || "90",
        TIMEOUT_BrowserLaunchEvent: process.env.TIMEOUT_BrowserLaunchEvent || "90",
        TIMEOUT_BrowserConnectedEvent: process.env.TIMEOUT_BrowserConnectedEvent || "90",
      },
      stdio: ["pipe", "pipe", "pipe"],
    });

    this.proc.stdout.setEncoding("utf8");
    this.proc.stderr.setEncoding("utf8");
    this.proc.stdout.on("data", (chunk) => this.handleStdout(String(chunk)));
    this.proc.stderr.on("data", (chunk) => this.handleStderr(String(chunk)));
    this.proc.on("error", (err) => this.failAll(new Error(`local browser-use failed to start: ${err.message}`)));
    this.proc.on("exit", (code, signal) => {
      if (this.stopped) return;
      const suffix = this.stderrTail.trim() ? `\n${this.stderrTail.trim()}` : "";
      this.failAll(new Error(`local browser-use exited (${signal ?? code})${suffix}`));
    });
  }

  run(prompt: string, options: { schema?: unknown; model?: BrowserModel; onMessage?: (msg: string) => void } = {}): Promise<unknown> {
    const requestId = this.nextRequestId++;
    const payload = {
      type: "run",
      request_id: requestId,
      prompt,
      schema: options.schema,
      model: options.model,
    };

    return new Promise((resolveRun, rejectRun) => {
      this.pending.set(requestId, { resolve: resolveRun, reject: rejectRun, onMessage: options.onMessage });
      options.onMessage?.("running local browser-use agent...");
      this.proc.stdin.write(`${JSON.stringify(payload)}\n`, (err) => {
        if (!err) return;
        this.pending.delete(requestId);
        rejectRun(new Error(`failed to send browser task: ${err.message}`));
      });
    });
  }

  async stop(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    try {
      this.proc.stdin.write(`${JSON.stringify({ type: "stop" })}\n`);
      this.proc.stdin.end();
    } catch {}

    await new Promise<void>((resolveStop) => {
      const timeout = setTimeout(() => {
        if (!this.proc.killed) this.proc.kill("SIGTERM");
        resolveStop();
      }, 5_000);
      this.proc.once("exit", () => {
        clearTimeout(timeout);
        resolveStop();
      });
    });
  }

  private handleStdout(chunk: string): void {
    this.stdoutBuffer += chunk;
    let newline = this.stdoutBuffer.indexOf("\n");
    while (newline !== -1) {
      const line = this.stdoutBuffer.slice(0, newline).trimEnd();
      this.stdoutBuffer = this.stdoutBuffer.slice(newline + 1);
      this.handleLine(line);
      newline = this.stdoutBuffer.indexOf("\n");
    }
  }

  private handleLine(line: string): void {
    if (!line.startsWith(PROTOCOL_PREFIX)) {
      if (line.trim()) debug("BROWSER", `local stdout: ${line}`);
      return;
    }

    let msg: ProtocolMessage;
    try {
      msg = JSON.parse(line.slice(PROTOCOL_PREFIX.length)) as ProtocolMessage;
    } catch (err) {
      debug("BROWSER", "invalid local protocol line", { line, err });
      return;
    }

    if (msg.type === "ready") {
      this.readySettled = true;
      this.readyResolve();
      return;
    }

    const pending = msg.request_id ? this.pending.get(msg.request_id) : undefined;
    if (!pending) {
      if (msg.type === "error" && !this.readySettled) {
        this.readySettled = true;
        this.readyReject(new Error(msg.message ?? "local browser-use failed to start"));
      }
      return;
    }

    if (msg.type === "progress") {
      const summary = (msg.message ?? "").trim();
      if (summary) pending.onMessage?.(formatBrowserMessage(summary));
      return;
    }

    this.pending.delete(msg.request_id!);
    if (msg.type === "done") {
      pending.resolve(msg.output);
    } else {
      pending.reject(new Error(msg.message ?? "browser task failed"));
    }
  }

  private handleStderr(chunk: string): void {
    this.stderrTail = (this.stderrTail + chunk).slice(-4_000);
    if (isDebug()) debug("BROWSER", `local stderr: ${chunk.trimEnd()}`);
  }

  private failAll(err: Error): void {
    this.readyReject(err);
    for (const pending of this.pending.values()) pending.reject(err);
    this.pending.clear();
  }
}

/** browser-use agent messages that are internal chatter rather than useful progress. */
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
  if (isNoisyAgentMessage(summary)) return "";
  const termWidth = process.stdout.columns ?? 80;
  const maxLen = Math.max(40, termWidth - 10);
  const cleaned = summary.trim().replace(/\s+/g, " ");
  if (cleaned.length <= maxLen) return cleaned;
  return cleaned.slice(0, maxLen - 1) + "...";
}

/** drive a task inside an existing local browser session. */
export async function runTask(
  client: BrowserClient,
  sessionId: string,
  prompt: string,
  options: RunTaskOptions = {},
): Promise<string> {
  const session = requireSession(client, sessionId);
  const { onMessage, model } = options;

  debugBlock("BROWSER", `runTask start · session=${sessionId} model=${model ?? "default"}`, prompt);
  const taskStart = Date.now();

  try {
    const output = await retryOnRateLimit(
      () => session.run(prompt, { model, onMessage }),
      {
        label: "browser-use task",
        onRetry: onMessage,
      },
    );
    const text = coerceOutput(output);
    debugBlock("BROWSER", `runTask done · ${((Date.now() - taskStart) / 1000).toFixed(2)}s`, text);
    return text;
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    debug("BROWSER", `runTask FAILED · ${((Date.now() - taskStart) / 1000).toFixed(2)}s`, { error: message });
    throw new Error(`browser task failed: ${message}`);
  }
}

/** run a local task and validate the returned object against the existing Zod schema. */
export async function runTaskStructured<T extends z.ZodType>(
  client: BrowserClient,
  sessionId: string,
  prompt: string,
  schema: T,
  options: RunTaskOptions = {},
): Promise<z.output<T>> {
  const session = requireSession(client, sessionId);
  const { onMessage, model } = options;

  debugBlock("BROWSER", `runTaskStructured start · session=${sessionId} model=${model ?? "default"}`, prompt);
  const taskStart = Date.now();

  try {
    const jsonSchema = z.toJSONSchema(schema);
    const output = await retryOnRateLimit(
      () => session.run(prompt, { schema: jsonSchema, model, onMessage }),
      {
        label: "browser-use structured task",
        onRetry: onMessage,
      },
    );
    const parsed = schema.parse(output);
    debug("BROWSER", `runTaskStructured done · ${((Date.now() - taskStart) / 1000).toFixed(2)}s`, parsed);
    return parsed as z.output<T>;
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    debug("BROWSER", `runTaskStructured FAILED · ${((Date.now() - taskStart) / 1000).toFixed(2)}s`, { error: message });
    throw new Error(`browser task failed: ${message}`);
  }
}

/** stop a local session and clean up resources */
export async function stopSession(client: BrowserClient, sessionId: string): Promise<void> {
  const session = client.sessions.get(sessionId);
  if (!session) return;
  client.sessions.delete(sessionId);
  await session.stop();
}

/** no cloud sessions exist in local mode; stop anything owned by this process. */
export async function killActiveSessions(client: BrowserClient): Promise<number> {
  const sessions = [...client.sessions.values()];
  await Promise.all(sessions.map((session) => session.stop()));
  client.sessions.clear();
  return sessions.length;
}

function requireSession(client: BrowserClient, sessionId: string): LocalBrowserSession {
  const session = client.sessions.get(sessionId);
  if (!session) throw new Error(`unknown local browser session: ${sessionId}`);
  return session;
}

function pythonExecutable(): string {
  if (process.env.BROWSER_USE_PYTHON?.trim()) return process.env.BROWSER_USE_PYTHON.trim();

  const venvPython = process.env.VIRTUAL_ENV
    ? resolve(process.env.VIRTUAL_ENV, "bin", "python")
    : "";
  if (venvPython && existsSync(venvPython)) return venvPython;

  const projectVenvPython = resolve(PROJECT_ROOT, ".venv", "bin", "python");
  if (existsSync(projectVenvPython)) return projectVenvPython;

  return "python3";
}

/** coerce free-form output into a usable string */
function coerceOutput(output: unknown): string {
  if (output == null) return "";
  if (typeof output === "string") return output;
  try {
    return JSON.stringify(output);
  } catch {
    return String(output);
  }
}
