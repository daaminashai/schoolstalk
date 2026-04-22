#!/usr/bin/env bun

// Post-processing: rank top teachers per school from a CSV
// - Scores each teacher out of 100
// - Prefers HS STEM teachers (CS/Eng highest)
// - Uses web search (Exa API) to detect online presence and events
// - Adds labels and reasons for justification

import { canonicalizeDepartment } from "./names";
import { computeHackerScore } from "./validator";

type Row = Record<string, string>;

type RankedTeacher = {
  firstName: string;
  lastName: string;
  email: string | null;
  role: string;
  department: string | null;
  sourceUrl: string;
  subject: string | null;
  hackerScore: number; // 1-5
  presenceScore: number; // 0-5
  eventScore: number; // 0-3
  xtraScore: number; // 0-3
  hsBonus: number; // -30..+10
  score: number; // 0-100
  labels: string[];
  links: string[];
  reasons: string[];
};

// ---- CLI args ----

interface Flags {
  input: string | null;
  output: string | null;
  top: number;
  concurrency: number;
  exaKey: string | null;
  verbose: boolean;
}

function parseArgs(argv: string[]): Flags {
  const f: Flags = { input: null, output: null, top: 5, concurrency: 3, exaKey: null, verbose: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    const next = () => argv[++i] ?? null;
    if (a === "--input" || a === "-i") f.input = next();
    else if (a === "--output" || a === "-o") f.output = next();
    else if (a === "--top") f.top = Math.max(1, Number(next()) || 5);
    else if (a === "--concurrency" || a === "-j") f.concurrency = Math.max(1, Number(next()) || 3);
    else if (a === "--exa-key") f.exaKey = next();
    else if (a === "--verbose" || a === "-v") f.verbose = true;
    else if (!a.startsWith("-")) f.input = f.input ?? a;
  }
  return f;
}

// ---- CSV helpers (focused, not full-featured) ----

function parseCsv(text: string): Row[] {
  const rows: string[][] = [];
  let i = 0, field = "", inQuotes = false; const out: string[][] = [];
  let row: string[] = [];
  const pushField = () => { row.push(field); field = ""; };
  const pushRow = () => { if (row.length) out.push(row); row = []; };
  while (i < text.length) {
    const c = text[i]!;
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i += 2; }
        else { inQuotes = false; i++; }
      } else { field += c; i++; }
      continue;
    }
    if (c === '"') { inQuotes = true; i++; continue; }
    if (c === ',') { pushField(); i++; continue; }
    if (c === '\n') { pushField(); pushRow(); i++; continue; }
    if (c === '\r') { i++; continue; }
    field += c; i++;
  }
  pushField(); pushRow();
  if (out.length === 0) return [];
  const header = out[0]!.map((h) => h.trim());
  const recs: Row[] = [];
  for (let r = 1; r < out.length; r++) {
    const cells = out[r]!;
    const rec: Row = {};
    for (let c = 0; c < header.length; c++) rec[header[c]!] = (cells[c] ?? "").trim();
    recs.push(rec);
  }
  return recs;
}

function escapeCsv(v: string): string {
  if (v.includes(',') || v.includes('"') || v.includes('\n')) return '"' + v.replace(/"/g, '""') + '"';
  return v;
}

function toCsv(headers: string[], rows: string[][]): string {
  const head = headers.join(',');
  const body = rows.map((r) => r.map((c) => escapeCsv(c ?? "")).join(',')).join('\n');
  return head + '\n' + body + (rows.length ? '\n' : '');
}

// ---- Scoring helpers ----

function norm01(x: number, min: number, max: number): number {
  if (max <= min) return 0;
  const v = (x - min) / (max - min);
  return Math.max(0, Math.min(1, v));
}

const HS_POSITIVE = [
  /\bhigh\s+school\b/i,
  /\bHS\b/,
  /\bAP\b/,
  /\bIB\b/,
  /\bupper\s+school\b/i,
  /\b9\s*-\s*12\b/,
  /\bsecondary\b/i,
  /\bjunior\s*\/\s*senior\b/i,
];
const NON_HS = [
  /\belementary\b/i,
  /\bprimary\b/i,
  /\bintermediate\b/i,
  /\bmiddle\s+school\b/i,
  /\bMS\b/,
  /\bK\s*-\s*8\b/i,
  /\bK-?5\b/i,
];

function detectHsBonus(role: string, dept: string | null): { bonus: number; levelLabel: string } {
  const txt = [role, dept ?? ""].join(" ");
  if (NON_HS.some((re) => re.test(txt))) return { bonus: -30, levelLabel: "non_hs" };
  if (HS_POSITIVE.some((re) => re.test(txt))) return { bonus: +10, levelLabel: "hs" };
  return { bonus: 0, levelLabel: "unknown" };
}

// ---- Web presence via Exa ----

type ExaResult = { url: string; title?: string; text?: string; highlights?: string[] };

function normalizeName(s: string): string {
  return s
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z\s-]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function nameAppears(titleOrSlug: string, first: string, last: string): boolean {
  const f = normalizeName(first), l = normalizeName(last);
  const s = normalizeName(titleOrSlug);
  if (!f || !l || !s) return false;
  // require first AND at least one last-name token
  const lastParts = l.split(/[\s-]+/).filter((x) => x.length > 1);
  return s.includes(f) && lastParts.some((p) => s.includes(p));
}

function schoolTokensFromUrl(u: string): string[] {
  try {
    const host = new URL(u).hostname.replace(/^www\./, "");
    return host
      .split(/[\.\-]/)
      .map((w) => w.toLowerCase())
      .filter((w) => w.length > 2 && !["k12", "us", "edu", "org", "com", "net", "sd", "isd", "usd"].includes(w));
  } catch {
    return [];
  }
}

function phraseReferencesSchool(phrase: string, tokens: string[]): boolean {
  const lower = phrase.toLowerCase();
  return tokens.some((t) => lower.includes(t));
}

function urlSlug(path: string): string {
  const slug = path.split("/").pop() ?? path;
  return slug.replace(/[?#].*$/, "");
}

// Simple global rate gate for Exa (≤8 rps)
const EXA_MAX_RPS = 8;
let lastExaStart = 0;
async function exaGate(): Promise<void> {
  const minInterval = 1000 / EXA_MAX_RPS;
  const now = Date.now();
  const delta = now - lastExaStart;
  if (delta < minInterval) await new Promise((r) => setTimeout(r, minInterval - delta));
  lastExaStart = Date.now();
}

async function exaSearch(query: string, exaKey: string): Promise<ExaResult[]> {
  await exaGate();
  const res = await fetch("https://api.exa.ai/search", {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-api-key": exaKey },
    body: JSON.stringify({ query, numResults: 5, type: "auto" }),
  });
  if (!res.ok) throw new Error(`exa ${res.status}`);
  const data = (await res.json()) as { results?: ExaResult[] };
  return data.results ?? [];
}

type PresenceHit = { platform: string; url: string; title?: string };

async function findPresence(
  first: string,
  last: string,
  sourceUrl: string,
  exaKey: string,
  concurrency = 3,
  onLog?: (msg: string) => void,
): Promise<{ hits: PresenceHit[]; eventHits: string[]; xtraHits: string[] }> {
  const schoolToks = schoolTokensFromUrl(sourceUrl);
  const platformDomains = [
    { platform: "linkedin", site: "site:linkedin.com/in" },
    { platform: "x", site: "site:x.com" },
    { platform: "twitter", site: "site:twitter.com" },
    { platform: "github", site: "site:github.com" },
    { platform: "instagram", site: "site:instagram.com" },
    { platform: "facebook", site: "site:facebook.com" },
    { platform: "youtube", site: "site:youtube.com" },
  ];

  // platform queries (run in small batches)
  const hits: PresenceHit[] = [];
  for (let i = 0; i < platformDomains.length; i += concurrency) {
    const batch = platformDomains.slice(i, i + concurrency);
    const results = await Promise.allSettled(
      batch.map(async (pd) => {
        const q = `"${first} ${last}" ${pd.site}`;
        onLog?.(`[presence] query(${pd.platform}): ${q}`);
        const rs = await exaSearch(q, exaKey);
        onLog?.(`[presence] results(${pd.platform}): ${rs.length}`);
        for (const r of rs) {
          const u = r.url || "";
          if (!u) continue;
          const okName = nameAppears(r.title || urlSlug(u), first, last);
          onLog?.(`  • ${pd.platform} → ${u} | title=${r.title ?? "(none)"} | name_ok=${okName}`);
          if (!okName) continue;
          // Prefer matches referencing the school; relax for github/x
          const requireSchool = !(pd.platform === "github" || pd.platform === "x" || pd.platform === "twitter");
          const schoolOk = r.title ? phraseReferencesSchool(r.title, schoolToks) : false;
          if (requireSchool && !schoolOk) { onLog?.(`    ↳ rejected (no school context)`); continue; }
          hits.push({ platform: pd.platform, url: u.replace(/[?#].*$/, ""), title: r.title });
          onLog?.(`    ↳ accepted`);
          break; // take first good hit per platform
        }
      }),
    );
    // swallow rejections; presence is best-effort
    void results;
  }

  // personal site queries (try to find non-social website)
  const personalQueries = [
    `"${first} ${last}" teacher ${(schoolToks[0] ?? "")}`,
    `"${first} ${last}" site:sites.google.com`,
    `"${first} ${last}" site:.edu`,
  ];
  const socialHosts = /linkedin\.com|twitter\.com|x\.com|github\.com|instagram\.com|facebook\.com|youtube\.com/i;
  for (const q of personalQueries) {
    try {
      onLog?.(`[presence] query(website): ${q}`);
      const rs = await exaSearch(q, exaKey);
      onLog?.(`[presence] results(website): ${rs.length}`);
      for (const r of rs) {
        const u = r.url || "";
        if (!u || socialHosts.test(u)) continue;
        const okName = nameAppears(r.title || urlSlug(u), first, last);
        const schoolOk = phraseReferencesSchool(r.title || r.text || "", schoolToks);
        onLog?.(`  • website → ${u} | title=${r.title ?? "(none)"} | name_ok=${okName} | school_ok=${schoolOk}`);
        if (!okName || !schoolOk) continue;
        hits.push({ platform: "website", url: u.replace(/[?#].*$/, ""), title: r.title });
        onLog?.(`    ↳ accepted`);
        break;
      }
    } catch (err) {
      onLog?.(`[presence] website search failed: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  // events / extracurriculars queries
  const evKw = ["conference", "presenter", "panel", "workshop", "talk", "keynote", "ISTE", "CSTA", "NCTM", "NSTA", "SIGCSE", "Maker Faire", "FIRST", "VEX"];
  const xtraKw = ["club", "coach", "advisor", "sponsor", "Science Olympiad", "FIRST Robotics", "VEX Robotics", "Coding Club", "Hack Club", "CyberPatriot", "eSports", "Math Team", "Makerspace"];

  async function queryKw(kwList: string[], tag: string): Promise<string[]> {
    const found: string[] = [];
    const q = `"${first} ${last}" ${(schoolToks[0] ?? "teacher")}`;
    // run a few topic-augmented searches
    const topics = kwList.slice(0, 4);
    const rs = await Promise.allSettled(topics.map(async (k) => {
      const qq = `${q} ${k}`;
      onLog?.(`[${tag}] query: ${qq}`);
      const res = await exaSearch(qq, exaKey);
      onLog?.(`[${tag}] results: ${res.length}`);
      return res;
    }));
    for (const pr of rs) {
      if (pr.status !== "fulfilled") continue;
      for (const r of pr.value) {
        const u = r.url || "";
        if (!u) continue;
        const title = (r.title || "").toLowerCase();
        const text = (r.text || "").toLowerCase();
        const hitsKw = kwList.some((k) => title.includes(k.toLowerCase()) || text.includes(k.toLowerCase()));
        const okName = nameAppears(r.title || urlSlug(u), first, last);
        const schoolOk = phraseReferencesSchool(r.title || text, schoolToks);
        onLog?.(`  • ${tag} → ${u} | title=${r.title ?? "(none)"} | name_ok=${okName} | kw_ok=${hitsKw} | school_ok=${schoolOk}`);
        if (okName && hitsKw && schoolOk) {
          found.push(u.replace(/[?#].*$/, ""));
          if (found.length >= 3) break;
        }
      }
    }
    return Array.from(new Set(found)).slice(0, 3);
  }

  const [eventHits, xtraHits] = await Promise.all([queryKw(evKw, "events"), queryKw(xtraKw, "xtra")] );
  return { hits, eventHits, xtraHits };
}

// ---- Main ranking logic ----

function deriveSubject(role: string, dept: string | null): string | null {
  // Prefer canonical department; fall back to role-derived canonical subject
  const cDept = canonicalizeDepartment(dept);
  if (cDept) return cDept;
  const fromRole = canonicalizeDepartment(role);
  return fromRole;
}

function baseScore(first: string, last: string, role: string, dept: string | null): { subject: string | null; hackerScore: number; hsBonus: number; levelLabel: string } {
  const subject = deriveSubject(role, dept);
  const hs = detectHsBonus(role, subject);
  const hacker = computeHackerScore(role, subject);
  return { subject, hackerScore: hacker, hsBonus: hs.bonus, levelLabel: hs.levelLabel };
}

function aggregateScore(hackerScore: number, presence: number, events: number, xtra: number, hsBonus: number): number {
  const s = 0.45 * norm01(hackerScore, 1, 5) * 100
          + 0.35 * norm01(presence, 0, 5) * 100
          + 0.20 * norm01(events + xtra, 0, 6) * 100
          + hsBonus;
  return Math.max(0, Math.min(100, Math.round(s)));
}

function reasonsFor(rt: RankedTeacher): string[] {
  const rs: string[] = [];
  if (rt.hackerScore === 5) rs.push("CS/coding focus");
  else if (rt.hackerScore === 4) rs.push("Engineering/robotics focus");
  else if (rt.hackerScore === 3) rs.push("Physics/CAD/applied focus");
  if (rt.labels.includes("internet_active")) rs.push("active online presence");
  if (rt.labels.includes("event_attendee")) rs.push("conference/workshop participation");
  if (rt.labels.includes("extracurriculars")) rs.push("club advisor/coach");
  if (rt.labels.includes("hs_level")) rs.push("high school level");
  if (rt.score < 50) rs.push("limited signals / low confidence");
  return Array.from(new Set(rs)).slice(0, 2);
}

async function main(): Promise<void> {
  const flags = parseArgs(Bun.argv.slice(2));
  if (!flags.input) {
    console.error("Usage: bun src/rank.ts --input <teachers.csv> [--output <top.csv>] [--top 5] [--exa-key <key>] [--concurrency N] [--verbose]");
    process.exit(1);
  }
  const topN = flags.top || 5;
  const exaKey = (flags.exaKey ?? process.env.EXA_API_KEY ?? "").trim();
  if (!exaKey) console.error("Note: EXA_API_KEY not set — web presence scoring will be skipped");

  const inputPath = flags.input!;
  const raw = await Bun.file(inputPath).text();
  const rows = parseCsv(raw);
  if (rows.length === 0) {
    console.error("No rows found in input CSV");
    process.exit(1);
  }

  const teachers = rows.map((r) => {
    return {
      firstName: r["first_name"] ?? "",
      lastName: r["last_name"] ?? "",
      email: (r["email"] ?? "").trim() || null,
      role: r["role"] ?? "",
      department: (r["department"] ?? "").trim() || null,
      sourceUrl: r["source_url"] ?? "",
      data_sources: r["data_sources"] ?? "",
    };
  }).filter((t) => (t.firstName || t.lastName));

  // Baseline scoring
  const ranked: RankedTeacher[] = teachers.map((t) => {
    const base = baseScore(t.firstName, t.lastName, t.role, t.department);
    if (flags.verbose) {
      console.log(`[baseline] ${t.firstName} ${t.lastName} | role=${t.role} | dept=${t.department ?? ""} | subject=${base.subject ?? ""} | hacker=${base.hackerScore} | hs_bonus=${base.hsBonus}`);
    }
    const labels: string[] = [];
    if (base.hsBonus > 0) labels.push("hs_level");
    return {
      firstName: t.firstName,
      lastName: t.lastName,
      email: t.email,
      role: t.role,
      department: t.department,
      sourceUrl: t.sourceUrl,
      subject: base.subject,
      hackerScore: base.hackerScore,
      presenceScore: 0,
      eventScore: 0,
      xtraScore: 0,
      hsBonus: base.hsBonus,
      score: 0,
      labels,
      links: [],
      reasons: [],
    };
  });

  // Enrichment (best-effort)
  if (exaKey) {
    let cursor = 0; const conc = Math.max(1, flags.concurrency || 3);
    async function worker() {
      while (true) {
        const i = cursor++;
        if (i >= ranked.length) break;
        const rt = ranked[i]!;
        try {
          if (flags.verbose) console.log(`\n[teacher] ${rt.firstName} ${rt.lastName} — presence search`);
          const pres = await findPresence(rt.firstName, rt.lastName, rt.sourceUrl, exaKey, Math.min(3, conc), flags.verbose ? (m) => console.log(m) : undefined);
          const platforms = new Set(pres.hits.map((h) => h.platform));
          // presence score weights
          let pScore = 0;
          if (platforms.has("linkedin")) pScore += 1.0;
          if (platforms.has("x") || platforms.has("twitter")) pScore += 1.0;
          if (platforms.has("github")) pScore += 1.5;
          if (platforms.has("instagram")) pScore += 0.75;
          if (platforms.has("facebook")) pScore += 0.75;
          if (platforms.has("youtube")) pScore += 1.0;
          pScore = Math.min(5, pScore);
          const eScore = Math.min(3, pres.eventHits.length);
          const xScore = Math.min(3, pres.xtraHits.length);

          rt.presenceScore = pScore;
          rt.eventScore = eScore;
          rt.xtraScore = xScore;
          if (pScore >= 2) rt.labels.push("internet_active");
          if (eScore >= 1) rt.labels.push("event_attendee");
          if (xScore >= 1) rt.labels.push("extracurriculars");
          rt.links = Array.from(new Set([...pres.hits.map((h) => h.url), ...pres.eventHits, ...pres.xtraHits]));
          if (flags.verbose) {
            console.log(`[presence] scores: presence=${rt.presenceScore} events=${rt.eventScore} xtra=${rt.xtraScore}`);
            if (rt.links.length) console.log(`[presence] links: ${rt.links.join(" | ")}`);
          }
        } catch (err) {
          // ignore, keep baseline
          if (flags.verbose) console.log(`[presence] failed: ${err instanceof Error ? err.message : String(err)}`);
        }
      }
    }
    await Promise.all(Array.from({ length: Math.min(conc, ranked.length) }, () => worker()));
  }

  // Finalize scores
  for (const rt of ranked) {
    rt.score = aggregateScore(rt.hackerScore, rt.presenceScore, rt.eventScore, rt.xtraScore, rt.hsBonus);
    if (rt.score < 50) rt.labels.push("low_quality");
    rt.reasons = reasonsFor(rt);
    if (flags.verbose) {
      console.log(`[final] ${rt.firstName} ${rt.lastName} | score=${rt.score} | labels=${rt.labels.join(";")}`);
    }
  }

  // Sort and pick top N
  ranked.sort((a, b) => b.score - a.score || Number(!!b.email) - Number(!!a.email) || (b.presenceScore - a.presenceScore));
  const top = ranked.slice(0, topN);

  // Write output CSV (original + new columns)
  const headers = [
    "first_name",
    "last_name",
    "email",
    "role",
    "department",
    "source_url",
    "data_sources",
    "subject",
    "score",
    "labels",
    "links",
    "reasons",
  ];

  const originalRows = rows;
  const mapKey = (t: RankedTeacher) => `${t.firstName}\u0001${t.lastName}\u0001${t.email ?? ""}`;
  const rankedMap = new Map(top.map((t) => [mapKey(t), t] as const));

  const outRows: string[][] = [];
  for (const r of originalRows) {
    const key = `${(r["first_name"] ?? "").trim()}\u0001${(r["last_name"] ?? "").trim()}\u0001${(r["email"] ?? "").trim()}`;
    const t = rankedMap.get(key);
    if (!t) continue; // only emit top N
    outRows.push([
      r["first_name"] ?? "",
      r["last_name"] ?? "",
      r["email"] ?? "",
      r["role"] ?? "",
      r["department"] ?? "",
      r["source_url"] ?? "",
      r["data_sources"] ?? "",
      t.subject ?? "",
      String(t.score),
      t.labels.join(";"),
      t.links.join(";"),
      t.reasons.join("; "),
    ]);
  }

  // default output path: <input>.top.csv
  let outPath = flags.output;
  if (!outPath) {
    if (inputPath.toLowerCase().endsWith(".csv")) outPath = inputPath.replace(/\.csv$/i, ".top.csv");
    else outPath = inputPath + ".top.csv";
  }

  const csv = toCsv(headers, outRows);
  await Bun.write(outPath!, csv);
  // basic console summary
  console.log(`wrote top ${top.length} to ${outPath}`);
  const lows = top.filter((t) => t.score < 50).length;
  if (lows > 0) console.log(`${lows} of top ${top.length} scored low — flagged with low_quality`);
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exit(1);
});
