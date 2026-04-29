import { debugWarn } from "./debug";

export function isRateLimitError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err);
  return /(?:\b429\b|rate[-\s]?limit|too many requests|resource exhausted)/i.test(msg);
}

export function isPaymentOrQuotaError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err);
  return /(?:\b402\b|payment required|quota|insufficient credits|out of credits|billing)/i.test(msg);
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function retryOnRateLimit<T>(
  fn: () => Promise<T>,
  opts: {
    label: string;
    attempts?: number;
    baseDelayMs?: number;
    onRetry?: (msg: string) => void;
  },
): Promise<T> {
  const attempts = opts.attempts ?? 4;
  const baseDelayMs = opts.baseDelayMs ?? 30_000;

  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (isPaymentOrQuotaError(err)) throw err;
      if (!isRateLimitError(err) || attempt >= attempts) throw err;
      const delay = baseDelayMs * attempt;
      const msg = `${opts.label}: rate limit on attempt ${attempt}/${attempts}; retrying in ${Math.round(delay / 1000)}s`;
      opts.onRetry?.(msg);
      debugWarn("RETRY", msg, err instanceof Error ? err.message : String(err));
      await sleep(delay);
    }
  }

  throw new Error(`${opts.label}: exhausted retry attempts`);
}
