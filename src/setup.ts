// ── interactive setup wizard: walk the user from zero-env to ready-to-scrape ──
//
// runs when .env is missing keys. has three stages:
//   1. configure OpenRouter + get api key
//   2. sign up for browser-use via their public signup-challenge api, using
//      the LLM we just configured to solve the text challenge
//   3. (optional) use the fresh browser-use session to sign up for exa via a
//      vortex temp-email address, so linkedin enrichment works out of the box

import * as p from "@clack/prompts";
import color from "picocolors";
import OpenAI from "openai";
import { resolve } from "node:path";
import { existsSync } from "node:fs";
import { z } from "zod";
import {
  DEFAULT_OPENROUTER_MODEL,
  OPENROUTER_BASE_URL,
  getOpenRouterApiKey,
  getOpenRouterBaseUrl,
  getOpenRouterModel,
  openRouterHeaders,
} from "./aiConfig";
import {
  createClient,
  createSession,
  runTaskStructured,
  stopSession,
} from "./browser";

const ENV_FILE = resolve(".env");

const t = {
  brand: color.cyan,
  accent: color.magenta,
  ok: color.green,
  warn: color.yellow,
  bad: color.red,
  muted: color.dim,
  bold: color.bold,
};

// ── env file management ──────────────────────────────────────────────────────

/** required keys for the scraper to run. EXA is optional (linkedin fallback). */
export function missingRequiredEnv(): string[] {
  const missing: string[] = [];
  if (!getOpenRouterApiKey()) missing.push("OPENROUTER_API_KEY");
  return missing;
}

/** merge a set of KEY=VALUE pairs into .env, preserving unrelated lines */
export async function saveEnvVars(vars: Record<string, string>): Promise<void> {
  let existing = "";
  if (existsSync(ENV_FILE)) existing = await Bun.file(ENV_FILE).text();

  let output = existing;
  for (const [key, value] of Object.entries(vars)) {
    const line = `${key}=${value}`;
    const re = new RegExp(`^${key}\\s*=.*$`, "m");
    if (re.test(output)) {
      output = output.replace(re, line);
    } else {
      if (output && !output.endsWith("\n")) output += "\n";
      output += line + "\n";
    }
    process.env[key] = value;
  }
  await Bun.write(ENV_FILE, output);
}

// ── LLM provider selection ───────────────────────────────────────────────────

interface LlmConfig {
  baseUrl: string;
  model: string;
  apiKey: string;
}

interface ModelOption {
  value: string;
  label: string;
  hint?: string;
}

// curated OpenRouter model picks. kept short and tuned for
// the pipeline's needs: structured json, reasoning, cheap enough for a free
// tier. users can always swap the AI_MODEL line in .env later.
const OPENROUTER_MODELS: ModelOption[] = [
  { value: DEFAULT_OPENROUTER_MODEL, label: "GPT-5 Mini", hint: "balanced, paid" },
  { value: "anthropic/claude-sonnet-4.5", label: "Claude Sonnet 4.5", hint: "best quality, paid" },
  { value: "google/gemini-2.5-flash", label: "Gemini 2.5 Flash", hint: "fast + cheap" },
];

async function configureOpenRouter(): Promise<LlmConfig> {
  p.note(
    [
      `${t.bold("1.")} go to ${t.brand("https://openrouter.ai/keys")}`,
      `${t.bold("2.")} sign up / sign in with Google or GitHub`,
      `${t.bold("3.")} click ${t.muted("Create Key")}, give it a name, copy the ${t.muted("sk-or-...")} value`,
      `${t.bold("4.")} add a few dollars of credit if you want paid models`,
    ].join("\n"),
    "OpenRouter setup",
  );
  const apiKey = await askApiKey("OpenRouter");
  const model = await pickModel(OPENROUTER_MODELS);
  return { baseUrl: OPENROUTER_BASE_URL, model, apiKey };
}

function currentOpenRouterConfig(): LlmConfig {
  return {
    baseUrl: getOpenRouterBaseUrl(),
    model: getOpenRouterModel(),
    apiKey: getOpenRouterApiKey(),
  };
}

async function askApiKey(label: string): Promise<string> {
  const key = await p.password({
    message: `${label} API key`,
    validate: (v) => {
      if (!v || !v.trim()) return "required";
      if (v.trim().length < 10) return "that doesn't look like a valid key";
    },
  });
  if (p.isCancel(key)) process.exit(0);
  return (key as string).trim();
}

async function pickModel(options: ModelOption[]): Promise<string> {
  const picked = await p.select({ message: "pick a model", options });
  if (p.isCancel(picked)) process.exit(0);
  return picked as string;
}

// ── browser-use signup via public challenge api ──────────────────────────────

interface SignupChallenge {
  challenge_id: string;
  challenge_text: string;
}

interface SignupVerifyResponse {
  api_key: string;
}

const BU_BASE = "https://api.browser-use.com";

async function signUpForBrowserUse(llm: LlmConfig): Promise<string> {
  const proceed = await p.confirm({
    message: "auto-create a Browser Use account? will use the LLM to solve a text challenge",
    initialValue: true,
  });
  if (!proceed) {
    const manual = await p.password({
      message: "paste an existing BROWSER_USE_API_KEY",
      validate: (v) => (v && v.trim().length >= 10 ? undefined : "required"),
    });
    if (p.isCancel(manual)) process.exit(0);
    return (manual as string).trim();
  }

  const spinner = p.spinner();
  const client = new OpenAI({
    baseURL: llm.baseUrl,
    apiKey: llm.apiKey,
    defaultHeaders: openRouterHeaders(),
  });
  const MAX_ATTEMPTS = 4;

  let lastError = "";
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    spinner.start(
      attempt === 1
        ? "requesting signup challenge..."
        : `retrying (attempt ${attempt}/${MAX_ATTEMPTS})...`,
    );

    const challengeRes = await fetch(`${BU_BASE}/cloud/signup`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    if (!challengeRes.ok) {
      spinner.stop(t.bad("challenge request failed"));
      throw new Error(`browser-use signup challenge returned HTTP ${challengeRes.status}`);
    }
    const challenge = (await challengeRes.json()) as SignupChallenge;

    spinner.message(`solving challenge with ${t.brand(llm.model)}...`);

    const answer = await solveChallenge(client, llm.model, challenge.challenge_text);

    spinner.message("verifying answer...");

    const verifyRes = await fetch(`${BU_BASE}/cloud/signup/verify`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ challenge_id: challenge.challenge_id, answer }),
    });
    if (verifyRes.ok) {
      const { api_key } = (await verifyRes.json()) as SignupVerifyResponse;
      spinner.stop(t.ok("browser-use account created"));
      return api_key;
    }
    const body = await verifyRes.text();
    lastError = `HTTP ${verifyRes.status}: ${body.slice(0, 200)} — challenge was: ${challenge.challenge_text} — answered: ${answer}`;
    spinner.stop(
      t.warn(`attempt ${attempt} wrong (answered ${answer}) — ${attempt < MAX_ATTEMPTS ? "trying again" : "giving up"}`),
    );
  }

  throw new Error(
    `browser-use signup challenge failed after ${MAX_ATTEMPTS} attempts. last: ${lastError}`,
  );
}

/**
 * solve a browser-use text challenge. the challenges are noisy natural-language
 * math problems (CJK/roman numeral digits, random punctuation sprinkled between
 * characters) designed to be solvable by LLMs. we give the model room to reason
 * then extract the final answer token.
 */
async function solveChallenge(
  client: OpenAI,
  model: string,
  challengeText: string,
): Promise<string> {
  const res = await client.chat.completions.create({
    model,
    messages: [
      {
        role: "system",
        content:
          "You solve short natural-language math/word puzzles. The challenge text will be " +
          "deliberately noisy: random punctuation inserted between characters, mixed casing, " +
          "and numeric words in English, Chinese (一二三…), Japanese, or Roman. First, silently " +
          "denoise the prompt (drop stray punctuation, interpret numeric words as their values). " +
          "Then reason step-by-step in a brief scratchpad. Finally, on the very last line of your " +
          "response, output exactly: `FINAL: <answer>` where <answer> is a single number or word " +
          "with no units, punctuation, or extra commentary. Double-check arithmetic before " +
          "committing.",
      },
      {
        role: "user",
        content: `Challenge:\n${challengeText}`,
      },
    ],
  });

  const raw = res.choices[0]?.message?.content?.trim() ?? "";
  // prefer the explicit FINAL: marker; fall back to last non-empty line; then last token
  const finalMatch = raw.match(/FINAL\s*:\s*([^\s\n]+)/i);
  if (finalMatch?.[1]) return finalMatch[1].replace(/[.,!?;:]+$/, "");

  const lines = raw
    .split(/\r?\n/)
    .map((l: string) => l.trim())
    .filter(Boolean);
  const lastLine = lines.at(-1) ?? raw;
  const lastToken = lastLine.split(/\s+/).pop() ?? lastLine;
  return lastToken.replace(/[.,!?;:]+$/, "");
}

// ── exa signup via browser-use agent + vortex temp email ─────────────────────

const EXA_SIGNUP_URL =
  "https://auth.exa.ai/?callbackUrl=https%3A%2F%2Fdashboard.exa.ai%2F";
const VORTEX_URL = "https://vortex.skyfall.dev/";

const ExaSignupSchema = z.object({
  status: z.enum(["success", "failed"]),
  api_key: z.string().nullable(),
  error: z.string().nullable(),
});

async function signUpForExa(): Promise<string | null> {
  const proceed = await p.confirm({
    message: "auto-sign up for Exa too? (better LinkedIn hit rate; uses a local browser)",
    initialValue: true,
  });
  if (!proceed) return null;

  const client = createClient();

  const spinner = p.spinner();
  spinner.start("launching browser session...");
  const session = await createSession(client);
  spinner.stop("browser session started");
  p.log.info(`${t.bold("watch live")}  ${t.brand(color.underline(session.liveUrl))}`);

  const task = [
    `Goal: sign up for an Exa account using a temporary email from Vortex, complete onboarding, and return the generated API key.`,
    ``,
    `Step-by-step:`,
    `1. Navigate to ${EXA_SIGNUP_URL}`,
    `2. Open a new tab and navigate to ${VORTEX_URL}`,
    `3. On the Vortex tab, note/copy the temporary email address shown on the page.`,
    `4. Switch back to the Exa tab, enter that email in the email field, and click "Continue".`,
    `5. Switch back to the Vortex tab. Wait for a new email from Exa to appear in the inbox list (may take up to 60 seconds — if nothing appears, wait longer or refresh the page).`,
    `6. Click on the Exa email in the inbox list. IMPORTANT: Vortex uses a click-to-expand animation — when you click an email, it animates open to reveal the body. If you don't see the email body (verification code) immediately after clicking, DO NOT assume the click failed. Wait 2-3 seconds for the animation to finish, then look again. If the body is still not visible, try clicking the email again or clicking the expand/chevron icon.`,
    `7. Read the verification code from the email body (usually 6 digits or a short alphanumeric string). Remember it.`,
    `8. Switch back to the Exa tab. Enter the verification code in the code field and click "VERIFY CODE".`,
    `9. Onboarding form — fill it out exactly as follows:`,
    `   - "Coding agent" question: select "Other"`,
    `   - "API client" question: select "cURL"`,
    `   - "Use case" question: select "People + company search"`,
    `   - "Describe your product features and use case" text field: enter "The best search engine for everyone"`,
    `   Then click "Next".`,
    `10. On the next page, click "Generate Code".`,
    `11. An API key will be shown. Click the eye icon to reveal it. Copy the full key value (a long alphanumeric string).`,
    `12. Return the api_key in the structured output.`,
    ``,
    `If anything fails (e.g. Vortex email doesn't arrive after 2 minutes, signup is blocked, form fields have changed), return status=failed with a short reason in the error field.`,
  ].join("\n");

  const spinner2 = p.spinner();
  spinner2.start("signing up for Exa (can take 1-2 minutes)...");

  // browser-use occasionally returns transient 5xx (502/503/504) mid-task —
  // retry a couple times before giving up. structural failures (bad schema,
  // 4xx auth errors) bail immediately since retrying won't help.
  const MAX_ATTEMPTS = 3;
  let lastErr: unknown = null;
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      const result = await runTaskStructured(client, session.id, task, ExaSignupSchema, {
        onMessage: (msg) => spinner2.message(msg),
        model: "default",
      });
      if (result.status === "success" && result.api_key) {
        spinner2.stop(t.ok("Exa account created"));
        try {
          await stopSession(client, session.id);
        } catch {}
        return result.api_key.trim();
      }
      // agent returned status=failed — don't retry, it's a soft failure
      spinner2.stop(t.warn(`Exa signup skipped: ${result.error ?? "unknown reason"}`));
      try {
        await stopSession(client, session.id);
      } catch {}
      return null;
    } catch (err) {
      lastErr = err;
      const msg = err instanceof Error ? err.message : String(err);
      const isTransient = /HTTP 5\d\d/.test(msg) || /timeout|timed out|ECONNRESET|fetch failed/i.test(msg);
      if (!isTransient || attempt === MAX_ATTEMPTS) break;
      spinner2.message(
        `browser-use transient error (${msg.slice(0, 60)}) — retrying (${attempt + 1}/${MAX_ATTEMPTS})...`,
      );
    }
  }

  spinner2.stop(t.warn("Exa signup failed — falling back to DDG"));
  p.log.warn(lastErr instanceof Error ? lastErr.message : String(lastErr));
  try {
    await stopSession(client, session.id);
  } catch {
    // best-effort cleanup
  }
  return null;
}

// ── top-level wizard ─────────────────────────────────────────────────────────

export async function runSetupWizard(): Promise<void> {
  p.intro(`${t.bold(t.brand("◆ schoolyank setup"))}  ${t.muted("let's get your .env ready")}`);

  const missing = missingRequiredEnv();
  p.log.info(
    `missing keys: ${missing.map((k) => t.brand(k)).join(", ")}`,
  );

  // stage 1: OpenRouter (needed to solve browser-use challenge)
  let llm = currentOpenRouterConfig();
  if (!llm.apiKey) {
    llm = await configureOpenRouter();
    await saveEnvVars({
      AI_BASE_URL: llm.baseUrl,
      AI_MODEL: llm.model,
      OPENROUTER_API_KEY: llm.apiKey,
    });
    p.log.info(`${t.ok("✓")} OpenRouter config saved`);
  } else {
    p.log.info(`${t.ok("✓")} OpenRouter config found`);
  }

  p.note(
    [
      `local Browser Use needs Python dependencies installed once:`,
      `${t.brand("python3 -m pip install -r requirements.txt")}`,
      `${t.brand("browser-use install")}`,
      `If ${t.brand("browser-use install")} says ${t.brand("uvx")} is missing, rerun the pip install command in your venv.`,
    ].join("\n"),
    "Local Browser Use",
  );

  // stage 2: exa key (optional — uses the local browser-use runner)
  const exaKey = await signUpForExa();
  if (exaKey) {
    await saveEnvVars({ EXA_API_KEY: exaKey });
    p.log.info(`${t.ok("✓")} EXA_API_KEY saved`);
  } else {
    p.log.info(t.muted("skipped Exa — will use DDG fallback for LinkedIn"));
  }

  p.outro(t.ok("setup complete — starting scrape"));
}
