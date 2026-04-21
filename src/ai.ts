// ── thin wrapper around an openai-compatible local endpoint ──

import OpenAI from "openai";
import { debug, debugBlock, isDebug } from "./debug";

const BASE_URL = process.env.AI_BASE_URL ?? "http://localhost:20128/v1";
const MODEL = process.env.AI_MODEL ?? "kr/claude-sonnet-4.5";
const API_KEY = process.env.AI_API_KEY ?? "not-needed";

// singleton client — reused across the entire pipeline
export const ai = new OpenAI({
  baseURL: BASE_URL,
  apiKey: API_KEY,
});

// send a system + user prompt and return the raw text reply
export async function ask(system: string, user: string): Promise<string> {
  if (isDebug()) {
    debug("AI", `ask() → model=${MODEL} system=${system.length}c user=${user.length}c`);
    debugBlock("AI", "system prompt", system);
    debugBlock("AI", "user prompt", user);
  }
  const start = Date.now();
  const res = await ai.chat.completions.create({
    model: MODEL,
    messages: [
      { role: "system", content: system },
      { role: "user", content: user },
    ],
  });

  const content = res.choices[0]?.message?.content ?? "";
  debugBlock("AI", `response · ${((Date.now() - start) / 1000).toFixed(2)}s`, content);
  return content;
}

// same as ask() but parses the response as JSON
export async function askJson<T>(system: string, user: string): Promise<T> {
  const text = await ask(
    system + "\n\nRespond with valid JSON only. No markdown, no commentary.",
    user,
  );

  return parseLooseJson<T>(text);
}

/**
 * best-effort json parser for model output. tries in order:
 *   1. strip markdown fences, parse as-is
 *   2. extract the first balanced object/array and parse that
 * throws a descriptive error (with a snippet of the raw text) on total failure.
 */
export function parseLooseJson<T>(raw: string): T {
  const stripped = raw
    .replace(/^\s*```(?:json)?\s*\n?/i, "")
    .replace(/\n?```\s*$/i, "")
    .trim();

  try {
    return JSON.parse(stripped) as T;
  } catch {}

  // find the outermost object or array in the text
  const firstObj = stripped.indexOf("{");
  const firstArr = stripped.indexOf("[");
  const firstStart =
    firstObj === -1 ? firstArr : firstArr === -1 ? firstObj : Math.min(firstObj, firstArr);

  if (firstStart !== -1) {
    const opener = stripped[firstStart]!;
    const closer = opener === "{" ? "}" : "]";
    const lastEnd = stripped.lastIndexOf(closer);
    if (lastEnd > firstStart) {
      try {
        return JSON.parse(stripped.slice(firstStart, lastEnd + 1)) as T;
      } catch {}
    }
  }

  const snippet = stripped.length > 200 ? stripped.slice(0, 200) + "…" : stripped;
  throw new Error(`failed to parse json from model output: ${snippet}`);
}
