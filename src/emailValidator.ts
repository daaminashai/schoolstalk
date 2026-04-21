// ── smtp + DNS email validation ──
//
// two cheap checks, no API keys:
//   1. DNS MX record lookup — confirms the domain accepts mail at all. catches
//      typos or fake domains. cached per-domain.
//   2. SMTP RCPT TO probe — connect to the domain's MX, HELO, MAIL FROM, then
//      send a RCPT TO for each email on the same connection. Google Workspace
//      and Microsoft 365 (which cover ~90% of K-12 districts) reject unknown
//      addresses with a 550/551/553 at RCPT TO — real mailboxes return 250.
//
// safety notes:
//   - we connect to ONE MX per domain and reuse the connection for every
//     email in that domain — ~46 RCPT TOs on one session, not 46 connections.
//     keeps us well under any threshold that'd trip rate limits or blacklists.
//   - 4xx temp failures, timeouts, or connection errors return "inconclusive"
//     (not "invalid") — we don't null out emails on a flaky probe.
//   - total per-run cost: ~2-5 DNS lookups + 1-3 SMTP connections. fast.

import { promises as dns } from "node:dns";
import { Socket } from "node:net";
import { debug, debugWarn } from "./debug";

export type EmailStatus =
  | "valid" // RCPT TO returned 250
  | "invalid" // RCPT TO returned 550/551/553
  | "no_mx" // domain has no MX records
  | "inconclusive"; // timeout / transient / catch-all

export interface EmailValidationResult {
  email: string;
  status: EmailStatus;
}

// ── DNS MX caching ───────────────────────────────────────────────────────────

const mxCache = new Map<string, string[] | null>();

export async function getMxRecords(domain: string): Promise<string[] | null> {
  const key = domain.toLowerCase();
  const cached = mxCache.get(key);
  if (cached !== undefined) return cached;
  try {
    const records = await dns.resolveMx(key);
    records.sort((a, b) => a.priority - b.priority);
    const hosts = records.map((r) => r.exchange).filter((h) => !!h);
    const result = hosts.length > 0 ? hosts : null;
    mxCache.set(key, result);
    return result;
  } catch {
    mxCache.set(key, null);
    return null;
  }
}

// ── SMTP session: reuse one connection per domain ────────────────────────────

interface SmtpProbeOptions {
  /** per-connection timeout — applies to reads + idle. */
  timeoutMs?: number;
  /** MAIL FROM address. must be a valid-looking throwaway on the same domain
   *  so the receiving server's sender-verification doesn't reject us. */
  fromAddress?: string;
  /** HELO hostname. arbitrary; we use "schoolyank.local". */
  heloHost?: string;
}

/**
 * probe every email at a given domain via one SMTP connection. returns a
 * status per email. if the initial handshake fails, ALL emails are marked
 * inconclusive — we never return "invalid" on transient errors.
 */
async function probeSmtpBatch(
  domain: string,
  emails: string[],
  options: SmtpProbeOptions = {},
): Promise<Map<string, EmailStatus>> {
  const results = new Map<string, EmailStatus>();
  for (const e of emails) results.set(e, "inconclusive");
  if (emails.length === 0) return results;

  const mx = await getMxRecords(domain);
  if (!mx) {
    for (const e of emails) results.set(e, "no_mx");
    return results;
  }

  const timeout = options.timeoutMs ?? 4000;
  const fromAddress = options.fromAddress ?? `postmaster@${domain}`;
  const heloHost = options.heloHost ?? "schoolyank.local";

  // quick port-25 probe on the top MX: if we can't even open a TCP socket in
  // 2s, port 25 is almost certainly blocked at our end. bail fast instead of
  // wasting N × timeout seconds across every MX host (google workspace has 5).
  const reachable = await tcpProbe(mx[0]!, 25, 2000);
  if (!reachable) return results;

  // try MX hosts in priority order until one connects
  for (const host of mx) {
    const outcome = await runSession(
      host,
      emails,
      fromAddress,
      heloHost,
      timeout,
    );
    if (outcome) {
      for (const [email, status] of outcome) results.set(email, status);
      return results;
    }
  }
  return results;
}

/** cheap TCP connectability check — resolves true only on established connect. */
function tcpProbe(host: string, port: number, timeoutMs: number): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = new Socket();
    let settled = false;
    const done = (ok: boolean) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      resolve(ok);
    };
    socket.setTimeout(timeoutMs);
    socket.once("connect", () => done(true));
    socket.once("timeout", () => done(false));
    socket.once("error", () => done(false));
    socket.connect(port, host);
  });
}

/**
 * single smtp session: HELO, MAIL FROM, then RCPT TO for every email (with
 * RSET between to keep the envelope clean). returns null if the handshake
 * fails — caller should try the next MX host.
 */
function runSession(
  host: string,
  emails: string[],
  fromAddress: string,
  heloHost: string,
  timeoutMs: number,
): Promise<Map<string, EmailStatus> | null> {
  return new Promise((resolve) => {
    const socket = new Socket();
    socket.setEncoding("utf8");
    socket.setTimeout(timeoutMs);

    type Step = "greeting" | "helo" | "mailFrom" | "rcpt" | "rset" | "mailFromAgain";
    let step: Step = "greeting";
    let rcptIdx = 0;
    let buffer = "";
    const results = new Map<string, EmailStatus>();
    let handshakeOk = false;
    let resolved = false;

    const finish = (value: Map<string, EmailStatus> | null) => {
      if (resolved) return;
      resolved = true;
      try {
        socket.write("QUIT\r\n");
      } catch {}
      socket.destroy();
      resolve(value);
    };

    socket.on("timeout", () => {
      finish(handshakeOk ? results : null);
    });
    socket.on("error", () => {
      finish(handshakeOk ? results : null);
    });
    socket.on("close", () => {
      finish(handshakeOk ? results : null);
    });

    socket.connect(25, host);

    socket.on("data", (chunk: string) => {
      buffer += chunk;
      // process complete lines. smtp multi-line responses use "250-" for
      // continuations and "250 " for the final line; we only act on finals.
      while (true) {
        const idx = buffer.indexOf("\r\n");
        if (idx < 0) break;
        const line = buffer.slice(0, idx);
        buffer = buffer.slice(idx + 2);
        if (line.length < 4) continue;
        if (line[3] === "-") continue;
        const code = parseInt(line.slice(0, 3), 10);
        handleCode(code);
      }
    });

    function handleCode(code: number): void {
      if (step === "greeting") {
        if (code !== 220) return finish(null);
        socket.write(`HELO ${heloHost}\r\n`);
        step = "helo";
        return;
      }
      if (step === "helo") {
        if (code !== 250) return finish(null);
        socket.write(`MAIL FROM:<${fromAddress}>\r\n`);
        step = "mailFrom";
        return;
      }
      if (step === "mailFrom") {
        if (code !== 250) return finish(null);
        handshakeOk = true;
        sendNextRcpt();
        return;
      }
      if (step === "rcpt") {
        const email = emails[rcptIdx - 1]!;
        // 250 = mailbox exists / accepted
        // 550/551/553 = user unknown
        // 451/452/421/other 4xx = temporary, inconclusive
        // 252 = VRFY-like "cannot verify but will attempt" → inconclusive
        let status: EmailStatus = "inconclusive";
        if (code === 250) status = "valid";
        else if (code === 550 || code === 551 || code === 553 || code === 554) status = "invalid";
        results.set(email, status);

        // send RSET before next RCPT to reset envelope state
        if (rcptIdx < emails.length) {
          socket.write("RSET\r\n");
          step = "rset";
        } else {
          finish(results);
        }
        return;
      }
      if (step === "rset") {
        // RSET returns 250; even if it doesn't, rebuild the envelope
        socket.write(`MAIL FROM:<${fromAddress}>\r\n`);
        step = "mailFromAgain";
        return;
      }
      if (step === "mailFromAgain") {
        // treat like mailFrom but go to rcpt
        if (code !== 250) return finish(results);
        sendNextRcpt();
        return;
      }
    }

    function sendNextRcpt(): void {
      if (rcptIdx >= emails.length) return finish(results);
      const email = emails[rcptIdx]!;
      rcptIdx++;
      socket.write(`RCPT TO:<${email}>\r\n`);
      step = "rcpt";
    }
  });
}

// ── top-level batch validator ────────────────────────────────────────────────

/**
 * validate a list of emails: groups by domain, one SMTP session per domain,
 * returns a map from email → status. any email whose domain has no MX record
 * is marked "no_mx"; transient errors are "inconclusive".
 *
 * never throws — the pipeline should not block on a flaky mail server.
 */
export async function validateEmailsBatched(
  emails: string[],
): Promise<Map<string, EmailStatus>> {
  const out = new Map<string, EmailStatus>();
  if (emails.length === 0) return out;

  // group by domain
  const byDomain = new Map<string, string[]>();
  for (const email of emails) {
    const domain = email.split("@")[1]?.toLowerCase().trim();
    if (!domain) {
      out.set(email, "invalid");
      continue;
    }
    const list = byDomain.get(domain) ?? [];
    list.push(email);
    byDomain.set(domain, list);
  }

  debug("EMAIL", `validateEmailsBatched · ${emails.length} emails across ${byDomain.size} domain(s)`, Object.fromEntries([...byDomain.entries()].map(([d, e]) => [d, e.length])));

  // run domains in parallel; each domain runs sequentially internally (one
  // connection, N rcpts). most districts use 1-2 domains total.
  await Promise.all(
    [...byDomain.entries()].map(async ([domain, batch]) => {
      const t0 = Date.now();
      try {
        const result = await probeSmtpBatch(domain, batch);
        for (const [email, status] of result) out.set(email, status);
        debug("EMAIL", `probed ${domain} · ${((Date.now() - t0) / 1000).toFixed(2)}s`, Object.fromEntries(result));
      } catch (err) {
        debugWarn("EMAIL", `probe threw for ${domain}`, { err: err instanceof Error ? err.message : String(err) });
        for (const email of batch) out.set(email, "inconclusive");
      }
    }),
  );

  return out;
}
