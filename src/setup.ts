// ── interactive setup wizard: walk the user from zero-env to ready-to-scrape ──
//
// runs when .env is missing keys. has two stages:
//   1. pick an LLM provider (hack club ai / openrouter / custom) + get api key
//   2. (optional) use the local browser-use runner to sign up for exa via a
//      vortex temp-email address, so linkedin enrichment works out of the box

import * as p from "@clack/prompts";
import color from "picocolors";
import { resolve } from "node:path";
import { existsSync } from "node:fs";
import { z } from "zod";
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
const REQUIRED_KEYS = ["AI_BASE_URL", "AI_MODEL", "AI_API_KEY"] as const;

export function missingRequiredEnv(): string[] {
  return REQUIRED_KEYS.filter((k) => !process.env[k]?.trim());
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

type ProviderId = "hackclub" | "openrouter" | "custom";

interface ModelOption {
  value: string;
  label: string;
  hint?: string;
}

// curated model picks per provider. kept short (2-3 options) and tuned for
// the pipeline's needs: structured json, reasoning, cheap enough for a free
// tier. users can always swap the AI_MODEL line in .env later.
const HACKCLUB_MODELS: ModelOption[] = [
  { value: "google/gemini-3-flash-preview", label: "Gemini 3 Flash Preview", hint: "fast, strong reasoning" },
  { value: "minimax/minimax-m2.5", label: "MiniMax M2.5", hint: "solid general-purpose" },
];

const OPENROUTER_MODELS: ModelOption[] = [
  { value: "anthropic/claude-sonnet-4.5", label: "Claude Sonnet 4.5", hint: "best quality, paid" },
  { value: "openai/gpt-5-mini", label: "GPT-5 Mini", hint: "balanced, paid" },
  { value: "google/gemini-2.5-flash", label: "Gemini 2.5 Flash", hint: "fast + cheap" },
];

async function selectLlmProvider(): Promise<LlmConfig> {
  const provider = await p.select<ProviderId>({
    message: "pick an LLM provider",
    options: [
      { value: "hackclub", label: "Hack Club AI", hint: "free for teens, requires Hack Club account" },
      { value: "openrouter", label: "OpenRouter", hint: "paid, access to most frontier models" },
      { value: "custom", label: "other OpenAI-compatible", hint: "enter your own base url + model" },
    ],
  });
  if (p.isCancel(provider)) process.exit(0);

  if (provider === "hackclub") {
    p.note(
      [
        `${t.bold("1.")} go to ${t.brand("https://ai.hackclub.com")}`,
        `${t.bold("2.")} sign in with your Hack Club account`,
        `${t.bold("3.")} click ${t.muted("Keys")} at the top → ${t.muted("Create New Key")}`,
        `${t.bold("4.")} copy the key and paste it below`,
      ].join("\n"),
      "Hack Club AI setup",
    );
    const apiKey = await askApiKey("Hack Club AI");
    const model = await pickModel(HACKCLUB_MODELS);
    return { baseUrl: "https://ai.hackclub.com/proxy/v1", model, apiKey };
  }

  if (provider === "openrouter") {
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
    return { baseUrl: "https://openrouter.ai/api/v1", model, apiKey };
  }

  // custom: user brings their own base URL + model string
  const baseUrl = await p.text({
    message: "base URL (OpenAI-compatible, ends in /v1)",
    placeholder: "https://api.example.com/v1",
    validate: (v) => {
      if (!v) return "required";
      try {
        new URL(v);
      } catch {
        return "not a valid url";
      }
    },
  });
  if (p.isCancel(baseUrl)) process.exit(0);

  const model = await p.text({
    message: "model name",
    placeholder: "e.g. claude-sonnet-4.5 or gpt-5-mini",
    validate: (v) => (v ? undefined : "required"),
  });
  if (p.isCancel(model)) process.exit(0);

  const apiKey = await askApiKey("your provider");
  return { baseUrl: baseUrl.trim(), model: (model as string).trim(), apiKey };
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

// ── exa signup via local browser-use agent + vortex temp email ────────────────

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

  // stage 1: LLM provider (used by local Browser Use and the LLM judges)
  const llm = await selectLlmProvider();
  await saveEnvVars({
    AI_BASE_URL: llm.baseUrl,
    AI_MODEL: llm.model,
    AI_API_KEY: llm.apiKey,
  });
  p.log.info(`${t.ok("✓")} LLM config saved`);

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
