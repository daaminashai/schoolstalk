export const OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1";
export const DEFAULT_OPENROUTER_MODEL = "openai/gpt-5-mini";

export function getOpenRouterBaseUrl(): string {
  const configured = process.env.AI_BASE_URL?.trim();
  return configured && isOpenRouterBaseUrl(configured) ? configured : OPENROUTER_BASE_URL;
}

export function getOpenRouterModel(): string {
  const configuredBase = process.env.AI_BASE_URL?.trim();
  const model = process.env.AI_MODEL?.trim();
  if (model && (!configuredBase || isOpenRouterBaseUrl(configuredBase))) return model;
  return DEFAULT_OPENROUTER_MODEL;
}

export function getOpenRouterApiKey(): string {
  const explicit = process.env.OPENROUTER_API_KEY?.trim();
  if (explicit) return explicit;

  const legacy = process.env.AI_API_KEY?.trim();
  if (legacy && isOpenRouterBaseUrl(process.env.AI_BASE_URL?.trim() ?? "")) return legacy;

  return "";
}

export function isOpenRouterBaseUrl(baseUrl: string): boolean {
  try {
    return new URL(baseUrl).hostname === "openrouter.ai";
  } catch {
    return false;
  }
}

export function openRouterHeaders(): Record<string, string> {
  return {
    "HTTP-Referer": process.env.OPENROUTER_SITE_URL?.trim() || "https://github.com/Hex-4/schoolyank",
    "X-OpenRouter-Title": process.env.OPENROUTER_APP_NAME?.trim() || "schoolyank",
  };
}
