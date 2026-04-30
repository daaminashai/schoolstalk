#!/usr/bin/env bun

// Post-processing: rank top teachers per school from a CSV
// - Scores each teacher out of 100
// - Keeps HS/unknown-level STEM teachers only; CS/Eng highest, unknown level penalized
// - Uses web search (Exa API) to detect LinkedIn, web mentions, events, and extracurriculars
// - Adds labels and reasons for justification

import { canonicalizeDepartment } from "./names";
import { computeHackerScore, isStemRole } from "./validator";
import { mkdir, readdir, stat } from "node:fs/promises";
import { basename, dirname, extname, join, relative } from "node:path";

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
  hsBonus: number; // -10..+10; non-HS rows are filtered out
  levelLabel: string;
  score: number; // 0-100
  labels: string[];
  links: string[];
  reasons: string[];
};

// ---- CLI args ----

interface Flags {
  input: string;
  output: string | null;
  teachersOutput: string;
  schoolsComplete: string;
  dist: string;
  top: number;
  distTop: number;
  concurrency: number;
  teacherConcurrency: number;
  exaKey: string | null;
  verbose: boolean;
  quiet: boolean;
}

function parseArgs(argv: string[]): Flags {
  const f: Flags = {
    input: "schools",
    output: null,
    teachersOutput: "teachers",
    schoolsComplete: "schools_1.csv",
    dist: "dist",
    top: 5,
    distTop: 1,
    concurrency: 3,
    teacherConcurrency: 1,
    exaKey: null,
    verbose: false,
    quiet: false,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    const next = () => argv[++i] ?? null;
    if (a === "--input" || a === "-i") f.input = next() ?? f.input;
    else if (a === "--output" || a === "-o") f.output = next();
    else if (a === "--teachers-output")
      f.teachersOutput = next() ?? f.teachersOutput;
    else if (a === "--schools-complete")
      f.schoolsComplete = next() ?? f.schoolsComplete;
    else if (a === "--dist") f.dist = next() ?? f.dist;
    else if (a === "--top") f.top = Math.max(1, Number(next()) || 5);
    else if (a === "--dist-top")
      f.distTop = Math.max(1, Number(next()) || 1);
    else if (a === "--concurrency" || a === "-j")
      f.concurrency = 1000;
    else if (a === "--teacher-concurrency")
      f.teacherConcurrency = Math.max(1, Number(next()) || 1);
    else if (a === "--exa-key") f.exaKey = next();
    else if (a === "--verbose" || a === "-v") f.verbose = true;
    else if (a === "--quiet" || a === "-q") f.quiet = true;
    else if (!a.startsWith("-")) f.input = a;
  }
  return f;
}

// ---- CSV helpers (focused, not full-featured) ----

function parseCsvDocument(text: string): { headers: string[]; rows: Row[] } {
  let i = 0,
    field = "",
    inQuotes = false;
  const out: string[][] = [];
  let row: string[] = [];
  const pushField = () => {
    row.push(field);
    field = "";
  };
  const pushRow = () => {
    if (row.length) out.push(row);
    row = [];
  };
  while (i < text.length) {
    const c = text[i]!;
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i += 2;
        } else {
          inQuotes = false;
          i++;
        }
      } else {
        field += c;
        i++;
      }
      continue;
    }
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
      i++;
      continue;
    }
    field += c;
    i++;
  }
  pushField();
  pushRow();
  if (out.length === 0) return { headers: [], rows: [] };
  const header = out[0]!.map((h) => h.trim());
  const recs: Row[] = [];
  for (let r = 1; r < out.length; r++) {
    const cells = out[r]!;
    const rec: Row = {};
    for (let c = 0; c < header.length; c++)
      rec[header[c]!] = (cells[c] ?? "").trim();
    recs.push(rec);
  }
  return { headers: header, rows: recs };
}

function parseCsv(text: string): Row[] {
  return parseCsvDocument(text).rows;
}

function escapeCsv(v: string): string {
  if (v.includes(",") || v.includes('"') || v.includes("\n"))
    return '"' + v.replace(/"/g, '""') + '"';
  return v;
}

function toCsv(headers: string[], rows: string[][]): string {
  const head = headers.join(",");
  const body = rows
    .map((r) => r.map((c) => escapeCsv(c ?? "")).join(","))
    .join("\n");
  return head + "\n" + body + (rows.length ? "\n" : "");
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

function detectHsBonus(
  role: string,
  dept: string | null,
): { bonus: number; levelLabel: string } {
  const txt = [role, dept ?? ""].join(" ");
  if (NON_HS.some((re) => re.test(txt)))
    return { bonus: 0, levelLabel: "non_hs" };
  if (HS_POSITIVE.some((re) => re.test(txt)))
    return { bonus: +10, levelLabel: "hs" };
  return { bonus: -10, levelLabel: "unknown" };
}

// ---- Web presence via Exa ----

type ExaResult = {
  url: string;
  title?: string;
  text?: string;
  highlights?: string[];
};

function normalizeName(s: string): string {
  return s
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z\s-]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function nameAppears(
  titleOrSlug: string,
  first: string,
  last: string,
): boolean {
  const f = normalizeName(first),
    l = normalizeName(last);
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
      .filter(
        (w) =>
          w.length > 2 &&
          ![
            "k12",
            "us",
            "edu",
            "org",
            "com",
            "net",
            "sd",
            "isd",
            "usd",
          ].includes(w),
      );
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

// Derive a human-ish school context string from a source URL host for employer matching
function deriveSchoolContextFromUrl(u: string): string {
  try {
    const host = new URL(u).hostname.replace(/^www\./, "").toLowerCase();
    const base = host.split(".")[0] ?? host; // take leftmost label
    let s = base.replace(/[-_]+/g, " ");
    // insert spaces before common subwords if the domain is concatenated
    const SUBWORDS = [
      "charter",
      "school",
      "academy",
      "district",
      "unified",
      "union",
      "public",
      "prep",
      "preparatory",
      "high",
      "middle",
      "elementary",
    ];
    for (const sub of SUBWORDS) {
      s = s.replace(new RegExp(sub, "g"), ` ${sub} `);
    }
    s = s.replace(/\s+/g, " ").trim();
    return s;
  } catch {
    return "";
  }
}

// Simple global rate gate for Exa (≤8 rps)
const EXA_MAX_RPS = 8;
let lastExaStart = 0;
const exaGateState: { chain: Promise<void> } = { chain: Promise.resolve() };
async function exaGate(): Promise<void> {
  const wait = exaGateState.chain.then(async () => {
    const minInterval = 1000 / EXA_MAX_RPS;
    const now = Date.now();
    const delta = now - lastExaStart;
    if (delta < minInterval)
      await new Promise((r) => setTimeout(r, minInterval - delta));
    lastExaStart = Date.now();
  });
  exaGateState.chain = wait.catch(() => {});
  await wait;
}

async function exaSearch(
  query: string,
  exaKey: string,
  opts: { includeText?: boolean; numResults?: number } = {},
): Promise<ExaResult[]> {
  await exaGate();
  const body: Record<string, unknown> = {
    query,
    numResults: opts.numResults ?? 5,
    type: "auto",
  };
  if (opts.includeText) body.contents = { text: true };
  const res = await fetch("https://api.exa.ai/search", {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-api-key": exaKey },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`exa ${res.status}`);
  const data = (await res.json()) as { results?: ExaResult[] };
  return data.results ?? [];
}

type PresenceHit = {
  platform: "linkedin";
  url: string;
  title?: string;
  score?: number;
  signals?: string[];
};

function resultText(r: ExaResult): string {
  return [r.title, r.text, ...(r.highlights ?? [])].filter(Boolean).join(" ");
}

function subjectTerms(
  subject: string | null | undefined,
  role: string | null | undefined,
): string[] {
  const text = [subject, role].filter(Boolean).join(" ").toLowerCase();
  const terms = new Set<string>();
  if (subject) terms.add(subject.toLowerCase());
  const groups: Array<[RegExp, string[]]> = [
    [
      /\b(computer\s+science|comp\s*sci|coding|programming|software|cyber|web\s+dev|app\s+dev|data\s+science)\b/,
      [
        "computer science",
        "coding",
        "programming",
        "software",
        "cybersecurity",
        "data science",
      ],
    ],
    [
      /\b(engineering|robotics|maker|makerspace|stem|steam|electronics|3d\s+printing|pltw)\b/,
      [
        "engineering",
        "robotics",
        "maker",
        "makerspace",
        "stem",
        "steam",
        "electronics",
        "3d printing",
        "pltw",
      ],
    ],
    [
      /\b(physics|cad|drafting|applied\s+science|astronomy|forensic\s+science)\b/,
      [
        "physics",
        "cad",
        "drafting",
        "applied science",
        "astronomy",
        "forensic science",
      ],
    ],
    [
      /\b(math|mathematics|algebra|geometry|calculus|statistics|probability)\b/,
      [
        "math",
        "mathematics",
        "algebra",
        "geometry",
        "calculus",
        "statistics",
        "probability",
      ],
    ],
    [
      /\b(chemistry|biology|environmental\s+science|earth\s+science|anatomy|physiology|science)\b/,
      [
        "chemistry",
        "biology",
        "environmental science",
        "earth science",
        "anatomy",
        "physiology",
        "science",
      ],
    ],
  ];
  for (const [re, kws] of groups) {
    if (re.test(text)) for (const kw of kws) terms.add(kw);
  }
  return [...terms].filter((t) => t.length > 1);
}

function phraseReferencesAny(phrase: string, terms: string[]): boolean {
  const lower = phrase.toLowerCase();
  return terms.some((t) =>
    new RegExp(`\\b${t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i").test(
      lower,
    ),
  );
}

function scoreLinkedinProfile(
  r: ExaResult,
  schoolToks: string[],
  terms: string[],
): { score: number; signals: string[] } {
  const text = resultText(r);
  const signals: string[] = [];
  let score = 1.0; // verified profile URL exists, but richer metadata earns more.
  if (
    /\b(about|bio|summary|experience|education|current|present)\b/i.test(text)
  ) {
    signals.push("profile_detail");
    score += 0.75;
  }
  if (
    /\b(teacher|educator|instructor|faculty|advisor|coach|curriculum|classroom)\b/i.test(
      text,
    )
  ) {
    signals.push("educator_detail");
    score += 0.5;
  }
  if (phraseReferencesAny(text, terms)) {
    signals.push("subject_match");
    score += 0.75;
  }
  if (phraseReferencesSchool(text, schoolToks)) {
    signals.push("school_match");
    score += 0.5;
  }
  return { score: Math.min(3, score), signals };
}

async function findPresence(
  first: string,
  last: string,
  sourceUrl: string,
  exaKey: string,
  onLog?: (msg: string) => void,
  role?: string,
  subject?: string | null,
): Promise<{
  hits: PresenceHit[];
  eventHits: string[];
  xtraHits: string[];
  mentionHits: string[];
}> {
  const schoolToks = schoolTokensFromUrl(sourceUrl);
  const schoolCtx = deriveSchoolContextFromUrl(sourceUrl);
  const terms = subjectTerms(subject, role);
  const hits: PresenceHit[] = [];
  const mentionHits: string[] = [];

  // Keep this intentionally small: one high-value LinkedIn site search plus one
  // generic search. The generic pass also infers events/extracurriculars.
  try {
    const q = schoolCtx
      ? `"${first} ${last}" "${schoolCtx}" site:linkedin.com/in`
      : `"${first} ${last}" site:linkedin.com/in`;
    onLog?.(`[presence] query(linkedin): ${q}`);
    const rs = await exaSearch(q, exaKey, { numResults: 3 });
    onLog?.(`[presence] results(linkedin): ${rs.length}`);
    for (const r of rs) {
      const u = r.url || "";
      if (!u) continue;
      const text = resultText(r) || urlSlug(u);
      const okName = nameAppears(text, first, last);
      const schoolOk = phraseReferencesSchool(text, schoolToks);
      const subjectOk = phraseReferencesAny(text, terms);
      onLog?.(
        `  - linkedin ${u} name=${okName} school=${schoolOk} subject=${subjectOk}`,
      );
      if (!okName || (!schoolOk && !subjectOk)) continue;
      const li = scoreLinkedinProfile(r, schoolToks, terms);
      hits.push({
        platform: "linkedin",
        url: u.replace(/[?#].*$/, ""),
        title: r.title,
        score: li.score,
        signals: li.signals,
      });
      break;
    }
  } catch (err) {
    onLog?.(
      `[presence] linkedin search failed: ${err instanceof Error ? err.message : String(err)}`,
    );
  }

  const evKw = [
    "conference",
    "presenter",
    "workshop",
    "keynote",
    "ISTE",
    "CSTA",
    "NCTM",
    "NSTA",
    "SIGCSE",
  ];
  const xtraKw = [
    "club",
    "coach",
    "advisor",
    "sponsor",
    "Science Olympiad",
    "FIRST Robotics",
    "VEX Robotics",
    "Coding Club",
    "Hack Club",
    "CyberPatriot",
    "Math Team",
  ];
  const eventHits: string[] = [];
  const xtraHits: string[] = [];

  const genericParts = [
    `"${first} ${last}"`,
    schoolCtx ? `"${schoolCtx}"` : schoolToks[0],
    subject ? `"${subject}"` : "teacher",
  ];
  const genericQuery = genericParts.filter(Boolean).join(" ");
  try {
    onLog?.(`[presence] query(generic): ${genericQuery}`);
    const rs = await exaSearch(genericQuery, exaKey, {
      includeText: true,
      numResults: 5,
    });
    onLog?.(`[presence] results(generic): ${rs.length}`);
    for (const r of rs) {
      const u = r.url || "";
      if (!u) continue;
      const text = resultText(r) || urlSlug(u);
      const okName = nameAppears(text, first, last);
      const schoolOk = phraseReferencesSchool(text, schoolToks);
      const subjectOk = phraseReferencesAny(text, terms);
      onLog?.(
        `  - generic ${u} name=${okName} school=${schoolOk} subject=${subjectOk}`,
      );
      if (!okName || (!schoolOk && !subjectOk)) continue;

      const cleanUrl = u.replace(/[?#].*$/, "");
      if (
        /linkedin\.com\/in/i.test(u) &&
        !hits.some((h) => h.platform === "linkedin")
      ) {
        const li = scoreLinkedinProfile(r, schoolToks, terms);
        hits.push({
          platform: "linkedin",
          url: cleanUrl,
          title: r.title,
          score: li.score,
          signals: li.signals,
        });
      } else if (!mentionHits.includes(cleanUrl)) {
        mentionHits.push(cleanUrl);
      }

      const lower = text.toLowerCase();
      if (
        eventHits.length < 2 &&
        evKw.some((kw) => lower.includes(kw.toLowerCase()))
      )
        eventHits.push(cleanUrl);
      if (
        xtraHits.length < 2 &&
        xtraKw.some((kw) => lower.includes(kw.toLowerCase()))
      )
        xtraHits.push(cleanUrl);
      if (
        mentionHits.length >= 3 &&
        eventHits.length >= 1 &&
        xtraHits.length >= 1
      )
        break;
    }
  } catch (err) {
    onLog?.(
      `[presence] generic search failed: ${err instanceof Error ? err.message : String(err)}`,
    );
  }

  return {
    hits,
    eventHits: Array.from(new Set(eventHits)).slice(0, 2),
    xtraHits: Array.from(new Set(xtraHits)).slice(0, 2),
    mentionHits: Array.from(new Set(mentionHits)).slice(0, 3),
  };
}

// ---- Main ranking logic ----

function deriveSubject(role: string, dept: string | null): string | null {
  // Prefer canonical department; fall back to role-derived canonical subject
  const cDept = canonicalizeDepartment(dept);
  if (cDept) return cDept;
  const fromRole = canonicalizeDepartment(role);
  return fromRole;
}

function baseScore(
  first: string,
  last: string,
  role: string,
  dept: string | null,
): {
  subject: string | null;
  hackerScore: number;
  hsBonus: number;
  levelLabel: string;
} {
  const subject = deriveSubject(role, dept);
  const hs = detectHsBonus(role, subject);
  const hacker = computeHackerScore(role, subject);
  return {
    subject,
    hackerScore: hacker,
    hsBonus: hs.bonus,
    levelLabel: hs.levelLabel,
  };
}

function aggregateScore(
  hackerScore: number,
  presence: number,
  events: number,
  xtra: number,
  hsBonus: number,
): number {
  // Heavier weight on hackerScore so CS/Engineering/Robotics outrank generic Math/Science
  const s =
    0.55 * norm01(hackerScore, 1, 5) * 100 +
    0.3 * norm01(presence, 0, 5) * 100 +
    0.15 * norm01(events + xtra, 0, 6) * 100 +
    hsBonus;
  return Math.max(0, Math.min(100, Math.round(s)));
}

const MAX_ENRICHMENT_TARGETS_PER_SCHOOL = 3;

const RANKED_TEACHER_HEADERS = [
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

type RankOptions = {
  top: number;
  exaKey: string;
  teacherConcurrency: number;
  verbose: boolean;
  logPrefix?: string;
  onProgress?: (message: string) => void;
};

type RankedCsv = {
  top: RankedTeacher[];
  topRows: string[][];
  csv: string;
};

function enrichmentPriority(rt: RankedTeacher): number {
  return aggregateScore(
    rt.hackerScore,
    rt.presenceScore,
    rt.eventScore,
    rt.xtraScore,
    rt.hsBonus,
  );
}

function reasonsFor(rt: RankedTeacher): string[] {
  const rs: string[] = [];
  if (rt.hackerScore === 5) rs.push("CS/coding focus");
  else if (rt.hackerScore === 4) rs.push("Engineering/robotics focus");
  else if (rt.hackerScore === 3) rs.push("Physics/CAD/applied focus");
  if (rt.labels.includes("internet_active")) rs.push("active online presence");
  if (rt.labels.includes("linkedin_detail")) rs.push("detailed LinkedIn match");
  if (rt.labels.includes("web_mentions")) rs.push("external web mentions");
  if (rt.labels.includes("event_attendee"))
    rs.push("conference/workshop participation");
  if (rt.labels.includes("extracurriculars")) rs.push("club advisor/coach");
  if (rt.labels.includes("hs_level")) rs.push("high school level");
  if (rt.score < 50) rs.push("limited signals / low confidence");
  return Array.from(new Set(rs)).slice(0, 2);
}

async function rankTeacherRows(
  rows: Row[],
  options: RankOptions,
): Promise<RankedCsv> {
  const topN = options.top || 5;
  const log = (msg: string) => {
    if (!options.verbose) return;
    console.log(options.logPrefix ? `${options.logPrefix} ${msg}` : msg);
  };
  const progressLog = (message: string) => options.onProgress?.(message);

  const teachers = rows
    .map((r) => {
      return {
        firstName: r["first_name"] ?? "",
        lastName: r["last_name"] ?? "",
        email: (r["email"] ?? "").trim() || null,
        role: r["role"] ?? "",
        department: (r["department"] ?? "").trim() || null,
        sourceUrl: r["source_url"] ?? "",
        data_sources: r["data_sources"] ?? "",
      };
    })
    .filter((t) => t.firstName || t.lastName);

  // Baseline scoring
  let ranked: RankedTeacher[] = teachers.map((t) => {
    const base = baseScore(t.firstName, t.lastName, t.role, t.department);
    log(
      `[baseline] ${t.firstName} ${t.lastName} | role=${t.role} | dept=${t.department ?? ""} | subject=${base.subject ?? ""} | hacker=${base.hackerScore} | level=${base.levelLabel} | hs_bonus=${base.hsBonus}`,
    );
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
      levelLabel: base.levelLabel,
      score: 0,
      labels,
      links: [],
      reasons: [],
    };
  });

  // Strict level filter: rank only high-school or unknown-level teachers.
  ranked = ranked.filter((rt) => rt.levelLabel !== "non_hs");
  const levelCandidateCount = ranked.length;

  const stemRanked = ranked.filter((rt) => isStemRole(rt.role, rt.subject));
  if (stemRanked.length > 0) {
    ranked = stemRanked;
    progressLog(
      `filter: ${stemRanked.length}/${levelCandidateCount} HS/unknown candidates matched STEM roles`,
    );
  } else {
    ranked = [];
    progressLog(
      `skip: no STEM-specific roles found in ${levelCandidateCount} HS/unknown candidates`,
    );
  }

  const enrichmentLimit = Math.min(
    ranked.length,
    Math.min(MAX_ENRICHMENT_TARGETS_PER_SCHOOL, Math.max(1, topN)),
  );
  const enrichmentTargets = [...ranked]
    .sort(
      (a, b) =>
        enrichmentPriority(b) - enrichmentPriority(a) ||
        Number(!!b.email) - Number(!!a.email),
    )
    .slice(0, enrichmentLimit);
  if (enrichmentTargets.length < ranked.length)
    log(
      `[enrichment] shortlisting ${enrichmentTargets.length}/${ranked.length} teachers for external searches`,
    );
  if (!options.exaKey) progressLog("web: skipped (EXA_API_KEY not set)");
  else if (enrichmentTargets.length === 0)
    progressLog("web: skipped (no ranked candidates)");
  else
    progressLog(
      `web: checking ${enrichmentTargets.length} shortlisted candidate${enrichmentTargets.length === 1 ? "" : "s"}`,
    );

  // Enrichment (best-effort)
  if (options.exaKey && enrichmentTargets.length > 0) {
    let cursor = 0;
    const conc = Math.max(1, options.teacherConcurrency || 1);
    async function worker() {
      while (true) {
        const i = cursor++;
        if (i >= enrichmentTargets.length) break;
        const rt = enrichmentTargets[i]!;
        try {
          log(`[teacher] ${rt.firstName} ${rt.lastName} - presence search`);
          const pres = await findPresence(
            rt.firstName,
            rt.lastName,
            rt.sourceUrl,
            options.exaKey,
            options.verbose ? (m) => log(m) : undefined,
            rt.role,
            rt.subject,
          );
          // presence score weights
          let pScore = rt.presenceScore || 0; // carry any linkedin boost from enrichment
          for (const hit of pres.hits) {
            pScore += hit.score ?? 1.0;
            if (!rt.labels.includes("linkedin")) rt.labels.push("linkedin");
            if (hit.signals?.length && !rt.labels.includes("linkedin_detail"))
              rt.labels.push("linkedin_detail");
          }
          if (pres.mentionHits.length > 0) {
            pScore += Math.min(2, pres.mentionHits.length * 0.75);
            if (!rt.labels.includes("web_mentions"))
              rt.labels.push("web_mentions");
          }
          pScore = Math.min(5, pScore);
          const eScore = Math.min(3, pres.eventHits.length);
          const xScore = Math.min(3, pres.xtraHits.length);

          rt.presenceScore = pScore;
          rt.eventScore = eScore;
          rt.xtraScore = xScore;
          if (pScore >= 2 && !rt.labels.includes("internet_active"))
            rt.labels.push("internet_active");
          if (eScore >= 1 && !rt.labels.includes("event_attendee"))
            rt.labels.push("event_attendee");
          if (xScore >= 1 && !rt.labels.includes("extracurriculars"))
            rt.labels.push("extracurriculars");
          const newLinks = [
            ...pres.hits.map((h) => h.url),
            ...pres.mentionHits,
            ...pres.eventHits,
            ...pres.xtraHits,
          ];
          rt.links = Array.from(new Set([...rt.links, ...newLinks]));
          if (options.verbose) {
            log(
              `[presence] scores: presence=${rt.presenceScore} events=${rt.eventScore} xtra=${rt.xtraScore}`,
            );
            if (rt.links.length)
              log(`[presence] links: ${rt.links.join(" | ")}`);
          }
        } catch (err) {
          // ignore, keep baseline
          log(
            `[presence] failed: ${err instanceof Error ? err.message : String(err)}`,
          );
        }
      }
    }
    await Promise.all(
      Array.from({ length: Math.min(conc, enrichmentTargets.length) }, () =>
        worker(),
      ),
    );
  }

  // Finalize scores
  for (const rt of ranked) {
    rt.score = aggregateScore(
      rt.hackerScore,
      rt.presenceScore,
      rt.eventScore,
      rt.xtraScore,
      rt.hsBonus,
    );
    if (rt.score < 50) rt.labels.push("low_quality");
    rt.reasons = reasonsFor(rt);
    log(
      `[final] ${rt.firstName} ${rt.lastName} | score=${rt.score} | labels=${rt.labels.join(";")}`,
    );
  }

  // Sort and pick top N
  ranked.sort(
    (a, b) =>
      b.score - a.score ||
      Number(!!b.email) - Number(!!a.email) ||
      b.presenceScore - a.presenceScore,
  );
  const top = ranked.slice(0, topN);

  const originalRows = rows;
  const mapKey = (t: RankedTeacher) =>
    `${t.firstName}\u0001${t.lastName}\u0001${t.email ?? ""}`;
  const originalByKey = new Map<string, Row>();
  for (const r of originalRows) {
    const key = `${(r["first_name"] ?? "").trim()}\u0001${(r["last_name"] ?? "").trim()}\u0001${(r["email"] ?? "").trim()}`;
    if (!originalByKey.has(key)) originalByKey.set(key, r);
  }

  const outRows: string[][] = [];
  for (const t of top) {
    const r = originalByKey.get(mapKey(t)) ?? {};
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

  return { top, topRows: outRows, csv: toCsv(RANKED_TEACHER_HEADERS, outRows) };
}

async function listCsvFiles(root: string): Promise<string[]> {
  const entries = await readdir(root, { withFileTypes: true });
  const files: string[] = [];

  for (const entry of entries) {
    const fullPath = join(root, entry.name);
    if (entry.isDirectory()) files.push(...(await listCsvFiles(fullPath)));
    else if (entry.isFile() && entry.name.toLowerCase().endsWith(".csv"))
      files.push(fullPath);
  }

  return files.sort((a, b) => a.localeCompare(b));
}

function defaultTopPath(inputPath: string): string {
  return inputPath.toLowerCase().endsWith(".csv")
    ? inputPath.replace(/\.csv$/i, ".top.csv")
    : `${inputPath}.top.csv`;
}

function topPathForSchool(
  inputRoot: string,
  csvPath: string,
  outputRoot: string,
): string {
  return join(outputRoot, relative(inputRoot, csvPath)).replace(
    /\.csv$/i,
    ".top.csv",
  );
}

function schoolIdFromPath(csvPath: string): string {
  return basename(csvPath, extname(csvPath));
}

async function fileExists(path: string): Promise<boolean> {
  try {
    return (await stat(path)).isFile();
  } catch {
    return false;
  }
}

function usage(): string {
  return [
    "Usage: bun src/rank.ts [--input schools] [--teachers-output teachers] [--schools-complete schools_complete.csv] [--dist dist] [--top 5] [--dist-top 1] [--concurrency N] [--teacher-concurrency N] [--exa-key KEY] [--verbose] [--quiet]",
    "",
    "Batch mode is the default: every schools/**/*.csv file is ranked and written to teachers/{state}/{city}/{hs_id}.top.csv.",
    "If --input points to a single CSV, only that file is ranked and --output controls the destination.",
    "Use --dist-top N to export up to N ranked teachers per school to dist/teachers.csv.",
    "Progress is printed by default; --verbose adds teacher-level debug logs, and --quiet suppresses progress logs.",
  ].join("\n");
}

function progress(flags: Flags, message: string): void {
  if (!flags.quiet) console.log(message);
}

async function runSingleFile(flags: Flags, exaKey: string): Promise<void> {
  progress(flags, `ranking ${flags.input}`);
  const raw = await Bun.file(flags.input).text();
  const rows = parseCsv(raw);
  progress(flags, `loaded ${rows.length} teacher rows`);
  const ranked = await rankTeacherRows(rows, {
    top: flags.top,
    exaKey,
    teacherConcurrency: flags.teacherConcurrency,
    verbose: flags.verbose,
    onProgress: (message) => progress(flags, message),
  });
  const outPath = flags.output ?? defaultTopPath(flags.input);
  await mkdir(dirname(outPath), { recursive: true });
  await Bun.write(outPath, ranked.csv);
  progress(flags, `wrote top ${ranked.top.length} to ${outPath}`);
}

async function runBatch(flags: Flags, exaKey: string): Promise<void> {
  const files = await listCsvFiles(flags.input);
  if (files.length === 0)
    throw new Error(`No CSV files found under ${flags.input}`);
  progress(flags, `found ${files.length} school CSVs under ${flags.input}`);
  progress(
    flags,
    `processing with concurrency=${Math.min(flags.concurrency, files.length)}; teacher_concurrency=${flags.teacherConcurrency}`,
  );
  progress(flags, `writing up to ${flags.distTop} ranked teacher${flags.distTop === 1 ? "" : "s"} per school to dist`);

  const schoolDoc = parseCsvDocument(
    await Bun.file(flags.schoolsComplete).text(),
  );
  const schoolHeaders =
    schoolDoc.headers.length > 0 ? schoolDoc.headers : ["ID"];
  const schoolById = new Map<string, Row>();
  for (const row of schoolDoc.rows) {
    const id = (row["ID"] ?? "").trim();
    if (id && !schoolById.has(id)) schoolById.set(id, row);
  }

  const distHeaders = [
    ...schoolHeaders,
    ...RANKED_TEACHER_HEADERS.map((h) => `teacher_${h}`),
  ];
  type ProcessedSchool = {
    outputPath: string;
    topCount: number;
    summaryRows: string[][];
    error: string | null;
    skipped: boolean;
  };
  const results: ProcessedSchool[] = new Array(files.length);
  let cursor = 0;
  let completed = 0;

  function schoolValuesFor(csvPath: string): string[] {
    const schoolId = schoolIdFromPath(csvPath);
    const schoolRow = schoolById.get(schoolId) ?? { ID: schoolId };
    return schoolHeaders.map((header) => schoolRow[header] ?? "");
  }

  async function processOne(csvPath: string): Promise<ProcessedSchool> {
    const outputPath = topPathForSchool(
      flags.input,
      csvPath,
      flags.teachersOutput,
    );
    try {
      if (await fileExists(outputPath)) {
        const existingDoc = parseCsvDocument(await Bun.file(outputPath).text());
        const existingRows = existingDoc.rows.filter((row) =>
          RANKED_TEACHER_HEADERS.some((header) => (row[header] ?? "").trim()),
        );
        const topTeachers = existingRows.slice(0, flags.distTop).map((row) =>
          RANKED_TEACHER_HEADERS.map((header) => row[header] ?? ""),
        );
        return {
          outputPath,
          topCount: existingRows.length,
          summaryRows: topTeachers.map((teacher) => [
            ...schoolValuesFor(csvPath),
            ...teacher,
          ]),
          error: null,
          skipped: true,
        };
      }

      const rows = parseCsv(await Bun.file(csvPath).text());
      const relPath = relative(flags.input, csvPath);
      progress(flags, `${relPath}: loaded ${rows.length} rows`);
      const ranked = await rankTeacherRows(rows, {
        top: Math.max(flags.top, flags.distTop),
        exaKey,
        teacherConcurrency: flags.teacherConcurrency,
        verbose: flags.verbose,
        logPrefix: `[${relPath}]`,
        onProgress: (message) => progress(flags, `${relPath}: ${message}`),
      });

      await mkdir(dirname(outputPath), { recursive: true });
      await Bun.write(outputPath, ranked.csv);

      const topTeachers = ranked.topRows.slice(0, flags.distTop);
      if (topTeachers.length === 0)
        return {
          outputPath,
          topCount: ranked.top.length,
          summaryRows: [],
          error: null,
          skipped: false,
        };

      return {
        outputPath,
        topCount: ranked.top.length,
        summaryRows: topTeachers.map((teacher) => [
          ...schoolValuesFor(csvPath),
          ...teacher,
        ]),
        error: null,
        skipped: false,
      };
    } catch (err) {
      return {
        outputPath,
        topCount: 0,
        summaryRows: [],
        error: err instanceof Error ? err.message : String(err),
        skipped: false,
      };
    }
  }

  async function worker(): Promise<void> {
    while (true) {
      const index = cursor++;
      if (index >= files.length) break;
      const result = await processOne(files[index]!);
      results[index] = result;
      const done = ++completed;
      const relPath = relative(flags.input, files[index]!);
      if (result.error)
        console.error(
          `[${done}/${files.length}] ${relPath} failed: ${result.error}`,
        );
      else if (result.skipped)
        progress(
          flags,
          `[${done}/${files.length}] ${relPath} skipped existing ${result.outputPath} (${result.topCount} ranked)`,
        );
      else if (result.topCount === 0)
        progress(
          flags,
          `[${done}/${files.length}] ${relPath} wrote ${result.outputPath} (no ranked teachers)`,
        );
      else
        progress(
          flags,
          `[${done}/${files.length}] ${relPath} wrote ${result.outputPath} (${result.topCount} ranked)`,
        );
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(flags.concurrency, files.length) }, () =>
      worker(),
    ),
  );

  const summaryRows = results.flatMap((r) => r?.summaryRows ?? []);
  const failures = results.filter((r) => r?.error);
  const skipped = results.filter((r) => r?.skipped).length;
  const distPath = join(flags.dist, "teachers.csv");
  await mkdir(flags.dist, { recursive: true });
  await Bun.write(distPath, toCsv(distHeaders, summaryRows));

  progress(
    flags,
    `processed ${files.length - failures.length - skipped}/${files.length} school CSVs; skipped ${skipped} existing`,
  );
  progress(
    flags,
    `wrote ${summaryRows.length} ranked teacher rows to ${distPath}`,
  );
  if (failures.length > 0) {
    for (const failure of failures.slice(0, 10))
      console.error(`failed ${failure.outputPath}: ${failure.error}`);
    if (failures.length > 10)
      console.error(`and ${failures.length - 10} more failures`);
    process.exitCode = 1;
  }
}

async function main(): Promise<void> {
  const argv = Bun.argv.slice(2);
  if (argv.includes("--help") || argv.includes("-h")) {
    console.log(usage());
    return;
  }

  const flags = parseArgs(argv);
  const exaKey = (flags.exaKey ?? process.env.EXA_API_KEY ?? "").trim();
  if (flags.exaKey && !process.env.EXA_API_KEY)
    process.env.EXA_API_KEY = exaKey;
  if (!exaKey)
    console.error(
      "Note: EXA_API_KEY not set - web presence scoring will be skipped",
    );

  const inputStat = await stat(flags.input);
  if (inputStat.isDirectory()) await runBatch(flags, exaKey);
  else if (inputStat.isFile()) await runSingleFile(flags, exaKey);
  else throw new Error(`Input is neither a file nor directory: ${flags.input}`);
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exit(1);
});
