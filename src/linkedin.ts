// ── linkedin enrichment via web search ──
//
// we never touch linkedin.com directly. instead we search the public web for
// a teacher's profile and extract the URL + job title from the SERP metadata.
//
// two providers, in order of preference:
//   1. Exa API (category="people") — purpose-built for linkedin profile lookup,
//      indexes 1B+ profiles, 1000 queries/month recurring free, no CC
//   2. DuckDuckGo HTML scrape — free, no auth, aggressively rate-limited
//
// the provider is picked at runtime based on whether EXA_API_KEY is set. exa's
// "people" category is specifically gated to linkedin domains with neural+auto
// search, so hit rate is ~5× DDG and we avoid bot-detection entirely.
//
// (tried and dropped: browser-use + linkedin cookies got login-walled every
// session; SearXNG public instances all disabled JSON + added Anubis JS
// challenges; Brave Search moved to CC-required free tier; Tavily works but
// Exa's people category is more targeted for exact name-to-profile lookup.)

import type { Teacher, ConfidenceScore } from "./types";
import { judgeLinkedinCandidates, type LinkedinCandidate } from "./judge";
import { debug, debugWarn } from "./debug";

// DDG tuning — only used when EXA_API_KEY isn't set. DDG rate-limits parallel
// queries from one IP aggressively (serves an "anomaly in your search query"
// page that returns zero results). low concurrency + per-worker jitter keeps
// us under the threshold. exa doesn't need any of this.
const DDG_CONCURRENCY = Number(process.env.LINKEDIN_CONCURRENCY ?? 2);
const DDG_MIN_DELAY_MS = 800;
const DDG_MAX_DELAY_MS = 1800;

// exa enforces 10 requests/second. keep concurrency modest AND gate every call
// through a global token bucket so bursts from N workers don't trip the 429.
// target 8 rps leaves headroom for retries.
const EXA_CONCURRENCY = 3;
const EXA_MAX_RPS = 8;
const EXA_MIN_INTERVAL_MS = 1000 / EXA_MAX_RPS;
const EXA_MAX_RETRIES = 3;

let exaLastCallAt = 0;
const exaGate: { chain: Promise<void> } = { chain: Promise.resolve() };

/**
 * serialize exa request starts through a chained promise so that every call
 * waits at least EXA_MIN_INTERVAL_MS after the previous one began. this keeps
 * the global rps under exa's 10/sec limit regardless of worker count.
 */
async function exaRateLimit(): Promise<void> {
  const wait = exaGate.chain.then(async () => {
    const now = Date.now();
    const delta = now - exaLastCallAt;
    if (delta < EXA_MIN_INTERVAL_MS) {
      await sleep(EXA_MIN_INTERVAL_MS - delta);
    }
    exaLastCallAt = Date.now();
  });
  exaGate.chain = wait.catch(() => {});
  await wait;
}

const USER_AGENTS = [
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
];

function randomUserAgent(): string {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)]!;
}

function ddgJitter(): number {
  return DDG_MIN_DELAY_MS + Math.random() * (DDG_MAX_DELAY_MS - DDG_MIN_DELAY_MS);
}

const sleep = (ms: number): Promise<void> =>
  new Promise((r) => setTimeout(r, ms));

interface LinkedinResult {
  found: boolean;
  title?: string | null;
  profileUrl?: string | null;
}

export interface LinkedinEnrichmentStats {
  processed: number;
  matched: number;
  noMatch: number;
  failed: number;
  firstFailure: string | null;
}

function bumpConfidence(current: ConfidenceScore): ConfidenceScore {
  return Math.min(current + 1, 5) as ConfidenceScore;
}

/** prioritize teachers who'd benefit most from enrichment */
function prioritize(teachers: Teacher[]): Teacher[] {
  return [...teachers].sort((a, b) => rank(a) - rank(b));
}
function rank(t: Teacher): number {
  if (!t.email) return 0;
  if (t.confidence <= 3) return 1;
  if (t.confidence === 4) return 2;
  return 3;
}

function contextFor(teacher: Teacher, fallback: string): string {
  return teacher.schoolName?.trim() || fallback;
}

/** decode html entities commonly found in SERP text */
function decodeEntities(s: string): string {
  return s
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, " ");
}

/** strip all HTML tags and collapse whitespace */
function stripHtml(s: string): string {
  return decodeEntities(s.replace(/<[^>]+>/g, "")).replace(/\s+/g, " ").trim();
}

/**
 * pull the profile url and job title from a DDG SERP. DDG result blocks look like:
 *   class="result__a" href="//duckduckgo.com/l/?uddg=<URL-ENCODED-TARGET>&...">Title text</a>
 *   ...
 *   class="result__snippet" ...>Snippet html with <b>highlights</b></a>
 *
 * title format on linkedin hits is usually "Name - Job Title - LinkedIn".
 * snippets typically start with the job title (e.g. "Science Teacher at X School...").
 * we prefer the snippet for title extraction because titles get truncated with "...".
 */
function parseLinkedinFromDdg(html: string): LinkedinResult {
  const resultRx =
    /<a[^>]+class="result__a"[^>]+href="([^"]+)"[^>]*>([\s\S]*?)<\/a>[\s\S]{0,2000}?class="result__snippet"[^>]*>([\s\S]*?)<\/a>/;
  const m = html.match(resultRx);
  if (!m) return { found: false };

  const [, redirectHref, rawTitle, rawSnippet] = m;

  // DDG wraps target urls in /l/?uddg=<encoded>; peel it out.
  const uddg = redirectHref!.match(/[?&]uddg=([^&]+)/);
  if (!uddg) return { found: false };
  const profileUrl = decodeURIComponent(uddg[1]!);
  if (!/linkedin\.com\/in\//.test(profileUrl)) return { found: false };

  // prefer snippet for the job title — it's unabbreviated. snippet commonly
  // looks like "Science Teacher at Champlain Valley Union High School · ..."
  const snippet = stripHtml(rawSnippet!);
  let title: string | null = null;

  if (snippet) {
    const firstSeg = snippet.split(/\s[·|]\s/)[0]!.trim();
    if (firstSeg.length > 0 && firstSeg.length < 200) title = firstSeg;
  }

  // fallback: parse the result title "Name - Job Title - LinkedIn"
  if (!title) {
    const titleText = stripHtml(rawTitle!).replace(/\s*[-–|]\s*LinkedIn\s*$/i, "").trim();
    const dashIdx = titleText.indexOf(" - ");
    if (dashIdx > 0) {
      const candidate = titleText.slice(dashIdx + 3).replace(/\.{3}$/, "").trim();
      if (candidate) title = candidate;
    }
  }

  return { found: true, profileUrl, title };
}

/** distinguish DDG bot-check pages from genuinely empty results */
function isBotCheckPage(html: string): boolean {
  // DDG's anomaly page contains this phrase + lacks any real result__a blocks
  return (
    html.includes("anomaly in your search query") ||
    !html.includes('class="result__a"')
  );
}

async function ddgFetch(query: string): Promise<string> {
  const url = `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`;
  const res = await fetch(url, {
    headers: {
      "User-Agent": randomUserAgent(),
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9",
      "Accept-Language": "en-US,en;q=0.9",
      Referer: "https://duckduckgo.com/",
    },
  });
  if (!res.ok) throw new Error(`DDG returned ${res.status}`);
  return res.text();
}

async function searchLinkedinViaDdg(
  teacher: Teacher,
  context: string,
): Promise<LinkedinResult> {
  // quote only the name — exact-phrase matching on the school name is too
  // strict (linkedin profiles often use an abbreviated school/district label).
  const query = `"${teacher.firstName} ${teacher.lastName}" ${context} site:linkedin.com/in`;

  let html = await ddgFetch(query);

  // one retry on bot-check with a backoff — sometimes DDG flags a burst but
  // relaxes after a short pause.
  if (isBotCheckPage(html)) {
    await sleep(3000 + Math.random() * 2000);
    html = await ddgFetch(query);
    if (isBotCheckPage(html)) {
      throw new Error("DDG rate-limited (bot-check page)");
    }
  }

  return parseLinkedinFromDdg(html);
}

// ── exa search provider ──────────────────────────────────────────────────────

interface ExaResult {
  url: string;
  title?: string;
  text?: string;
  highlights?: string[];
  score?: number;
}

/**
 * normalize a name for fuzzy comparison: lowercase, strip non-letters, collapse
 * whitespace. "O'Brien" → "obrien", "María José" → "mara jos".
 */
function normalizeName(s: string): string {
  return s
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "") // strip diacritics
    .replace(/[^a-z\s-]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * parse an exa result's title. linkedin profiles follow a predictable format:
 *   "Firstname Lastname | Role at Employer"
 *   "Firstname Lastname - Role - LinkedIn"
 *   "Firstname Lastname"
 * returns the name portion + the role portion (may be null).
 */
function parseExaTitle(title: string): { name: string; role: string | null } {
  const cleaned = title.replace(/\s*[-–|]\s*LinkedIn\s*$/i, "").trim();

  // primary format: "Name | Role" (exa's canonical people format)
  const pipeIdx = cleaned.indexOf(" | ");
  if (pipeIdx > 0) {
    return {
      name: cleaned.slice(0, pipeIdx).trim(),
      role: cleaned.slice(pipeIdx + 3).trim() || null,
    };
  }

  // fallback: "Name - Role" (older format)
  const dashIdx = cleaned.indexOf(" - ");
  if (dashIdx > 0) {
    return {
      name: cleaned.slice(0, dashIdx).trim(),
      role: cleaned.slice(dashIdx + 3).trim() || null,
    };
  }

  // title is just the name with no role segment
  return { name: cleaned, role: null };
}

/**
 * tokens that appear in lots of school names and carry no discriminative
 * signal ("high", "school", "union", "central" etc.). filtered out before
 * employer-match comparison — otherwise "Mount Abraham Union High School"
 * would match "Champlain Valley Union High School" on "Union" + "High" +
 * "School" alone.
 */
const GENERIC_SCHOOL_TOKENS = new Set([
  "school", "schools", "high", "middle", "elementary", "primary",
  "union", "central", "community", "county", "district",
  "public", "private", "preparatory", "academy", "institute",
  "unified", "consolidated", "regional", "k-12", "k12",
  "the", "of", "and", "at", "for",
]);

/** extract non-generic tokens from a school name for employer matching */
function discriminativeTokens(schoolName: string): string[] {
  return schoolName
    .toLowerCase()
    .split(/\s+/)
    .map((w) => w.replace(/[^a-z0-9]/g, ""))
    .filter((w) => w.length > 2 && !GENERIC_SCHOOL_TOKENS.has(w));
}

/**
 * does a phrase reference the target school? checks both full-token overlap
 * and acronym prefix. for "Champlain Valley Union High School" accepts:
 *   - phrases containing "champlain" or "valley" (discriminative tokens)
 *   - phrases containing "CVU" or "CVUHS" (acronym or its prefix)
 */
function phraseReferencesSchool(phrase: string, schoolName: string): boolean {
  const tokens = discriminativeTokens(schoolName);
  const lowerPhrase = phrase.toLowerCase();
  if (tokens.length > 0 && tokens.some((t) => lowerPhrase.includes(t))) return true;

  const targetAcronym = schoolName
    .split(/\s+/)
    .map((w) => w[0]?.toUpperCase() ?? "")
    .join("");
  if (targetAcronym.length >= 2) {
    // any 2-5 letter all-caps word in the phrase must be a prefix of the target's
    // acronym (e.g. "CVU" ⊂ "CVUHS"). catches Geoff Glaspie's "Math Teacher at
    // CVU High School" matching "Champlain Valley Union High School".
    const upperWords = phrase.match(/\b([A-Z]{2,5})\b/g) ?? [];
    if (upperWords.some((w) => targetAcronym.startsWith(w))) return true;
  }

  return false;
}

/**
 * detect linkedin "roles" that are really just the employer/school name with
 * no actual job title. e.g. "The Loomis Chaffee School" — happens when someone's
 * linkedin headline is just their current company.
 */
/**
 * true when the site-scraped role is too generic to carry useful info on its
 * own (blank, or a bare word like "Teacher" / "Staff"). these are the only
 * cases where we should fall through to a LinkedIn headline — a structured
 * site role like "Mathematics Teacher, HS" should always win because it's
 * more authoritative than a self-authored LinkedIn bio.
 */
function isSiteRolePlaceholder(role: string | null | undefined): boolean {
  if (!role) return true;
  const trimmed = role.trim().toLowerCase();
  if (trimmed.length === 0) return true;
  return /^(teacher|educator|instructor|staff|faculty|employee|professional)s?$/.test(trimmed);
}

/**
 * pick the best role to emit when the site role is a placeholder ("Teacher").
 * prefers a clean synthesized `"${department} Teacher"` when we have the
 * subject — linkedin headlines carry bio noise ("MBA", "PhD Student",
 * "State Employed at State of California") that shouldn't leak into the csv.
 * falls back to the linkedin title only when department is also missing.
 */
function pickEnrichedRole(
  currentRole: string | null | undefined,
  department: string | null | undefined,
  linkedinRole: string | null | undefined,
): string | null {
  if (!isSiteRolePlaceholder(currentRole)) return null;
  const dept = department?.trim();
  if (dept) return `${dept} Teacher`;
  const li = linkedinRole?.trim();
  if (li && li.length <= 120 && !isJustEmployerName(li)) return li;
  return null;
}

function isJustEmployerName(role: string): boolean {
  const ORG_PATTERNS = /\b(school|schools|academy|university|college|institute|district|corporation|inc|corp|llc|foundation|center|centre)\b/i;
  const JOB_SIGNALS = /\b(teacher|faculty|instructor|professor|coach|director|coordinator|head|chair|dean|educator|tutor|specialist|researcher|scientist|engineer|developer|counselor|librarian|aide|assistant|associate|advisor|mentor|fellow|candidate|staff|admin|administrator|manager|superintendent|principal|department|teaching|math|science|computer|physics|chemistry|biology|engineering|robotics)\b/i;
  if (!JOB_SIGNALS.test(role) && ORG_PATTERNS.test(role)) return true;
  return false;
}

/**
 * extract the employer from a linkedin role. common patterns:
 *   "Role at Employer" — most frequent
 *   "Role | Employer" — occasional
 * returns null if no clear employer context is in the role.
 */
function extractEmployer(role: string): string | null {
  // "Role at Employer" — employer runs until a separator or end of string
  const atMatch = role.match(/\bat\s+([^,|*·]+?)(?:\s*[,|*·]|\s*$)/i);
  if (atMatch?.[1]) return atMatch[1].trim();
  return null;
}

/**
 * detect linkedin profiles where the person is a RETIRED or FORMER educator.
 * the name + school might still match, but attaching a stale profile to a
 * currently-active teacher is silent data corruption.
 */
function isRetiredOrFormer(title: string): boolean {
  return /\b(retired|former|ex[\s-]|previously)\b/i.test(title);
}

/**
 * K-12-specific educator signals. deliberately DOES NOT include "university",
 * "college", or "professor" — those catch people in higher-ed roles who
 * happen to share a name with a K-12 teacher (e.g. Sharon Waxman's linkedin
 * "Nonprofit CEO * University Lecturer").
 */
const K12_KEYWORDS = [
  /\bteacher\b/i,
  /\beducator\b/i,
  /\binstructor\b/i,
  /\bprincipal\b/i,
  /\bfaculty\b/i,
  /\bK-?12\b/i,
  /\bPre-?K\b/i,
  /\bgrade\b/i,
  /\bclassroom\b/i,
  /\bcurriculum\b/i,
  /\binterventionist\b/i,
  /\bsuperintendent\b/i,
  /\btutor/i,
];

function titleSuggestsK12Educator(title: string): boolean {
  return K12_KEYWORDS.some((re) => re.test(title));
}

/**
 * verify an exa result actually belongs to the teacher we searched for. exa's
 * neural search returns the nearest embedding match even when no true match
 * exists, so we need stricter validation than just "a linkedin URL came back".
 *
 * primary signal: the result's title starts with the teacher's name. this is
 * MUCH more reliable than slug matching — linkedin profile titles are always
 * "Firstname Lastname | ..." so we can directly compare names.
 *
 * secondary signal: URL slug contains a name token (catches edge cases where
 * the title is weirdly formatted).
 *
 * tertiary guard: the title's role portion must contain at least one
 * education-related keyword. blocks wrong-profession false positives where
 * the name happened to match (e.g. a same-named salesperson).
 */
function nameMatchesTeacher(result: ExaResult, teacher: Teacher): boolean {
  const teacherFirst = normalizeName(teacher.firstName);
  const teacherLast = normalizeName(teacher.lastName);

  // primary: match against parsed title name
  if (result.title) {
    const { name } = parseExaTitle(result.title);
    const resultName = normalizeName(name);

    // require BOTH first and last names to appear in the result's name. handles
    // hyphenated/multi-word last names: "Bloxham-Fisher" vs "Bloxham" or
    // "Fisher" would be accepted as long as some part matches.
    const firstOk =
      !!teacherFirst &&
      (resultName.includes(teacherFirst) || teacherFirst.includes(resultName.split(" ")[0] ?? ""));
    const lastOk =
      !!teacherLast &&
      teacherLast
        .split(/[\s-]+/)
        .some((part) => part.length > 1 && resultName.includes(part));

    if (firstOk && lastOk) return true;
  }

  // secondary: slug-token backstop. rare case where the title is malformed but
  // the URL slug still carries the name.
  const slug = result.url.match(/\/in\/([^/?#]+)/i)?.[1] ?? "";
  const slugTokens = new Set(
    slug.toLowerCase().split(/[-_]/).filter((w) => w.length > 1),
  );
  if (slugTokens.size === 0) return false;

  const firstInSlug =
    !!teacherFirst &&
    [...slugTokens].some((t) => t.includes(teacherFirst) || teacherFirst.includes(t));
  const lastParts = teacherLast.split(/[\s-]+/).filter((p) => p.length > 1);
  const lastInSlug =
    lastParts.length > 0 &&
    lastParts.some((part) => [...slugTokens].some((t) => t.includes(part)));
  return firstInSlug && lastInSlug;
}

function resultMatchesTeacher(
  result: ExaResult,
  teacher: Teacher,
  fallbackContext: string,
): boolean {
  // phase 1: name on the linkedin profile must match the teacher's name
  if (!nameMatchesTeacher(result, teacher)) return false;

  if (!result.title) return false;

  // phase 2: reject retired/former profiles — stale data, not a currently-active
  // teacher. catches "Retired math teacher at CVU" matches.
  if (isRetiredOrFormer(result.title)) return false;

  const targets = [teacher.schoolName, fallbackContext].filter(
    (x): x is string => !!x && x.trim().length > 0,
  );

  // phase 3: if the title has an explicit employer ("at X"), that employer
  // MUST reference the teacher's school. catches Samantha Kayhart's "at Mount
  // Abraham Union High School" mismatch vs her actual school (CVU).
  const employer = extractEmployer(result.title);
  if (employer) {
    if (targets.length === 0) return true;
    return targets.some((target) => phraseReferencesSchool(employer, target));
  }

  // phase 4: no explicit employer in the title. accept only if there's a K-12
  // educator signal (teacher / educator / interventionist / etc). deliberately
  // excludes "university", "professor", "lecturer" — those catch unrelated
  // higher-ed people with matching names (Sharon Waxman's "University Lecturer").
  // If we have a target school/district context, require an employer reference;
  // don't accept generic educator titles with no employer — prevents same-name
  // matches from other districts.
  if (targets.length > 0) return false;
  return titleSuggestsK12Educator(result.title);
}

/**
 * query exa for a linkedin profile using the people category — purpose-built
 * for exactly this lookup (1B+ indexed linkedin profiles). the people category
 * is gated to linkedin domains so we don't need to filter manually. returns
 * the first linkedin.com/in hit with title + snippet extracted for the job
 * title. this path is the reliable one when EXA_API_KEY is set.
 */
async function searchLinkedinViaExa(
  teacher: Teacher,
  context: string,
  fallbackContext: string,
  apiKey: string,
): Promise<LinkedinResult> {
  const query = `${teacher.firstName} ${teacher.lastName} ${context}`;

  let res: Response | null = null;
  let lastStatus = 0;
  let lastDetail = "";
  for (let attempt = 0; attempt <= EXA_MAX_RETRIES; attempt++) {
    await exaRateLimit();
    res = await fetch("https://api.exa.ai/search", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
      },
      body: JSON.stringify({
        query,
        category: "people",
        // type: "auto" is the default and is specifically what works. "fast"
        // returns irrelevant near-misses; "keyword" is rejected (400) with
        // category="people". leave type unset.
        numResults: 5,
        // we don't request contents.text / highlights — probing showed they're
        // noisy (always prefixed with "# Name" markdown) while the title field
        // already has a clean "Name | Role at Employer" format.
      }),
    });

    if (res.ok) break;

    lastStatus = res.status;
    lastDetail = await res.text().catch(() => "");

    // 429 (rate limit) and 5xx (transient) → exponential backoff + retry.
    // 4xx other than 429 is a client error, bail immediately.
    const retriable = res.status === 429 || res.status >= 500;
    if (!retriable || attempt === EXA_MAX_RETRIES) {
      throw new Error(
        `exa returned ${res.status}${lastDetail ? `: ${lastDetail.slice(0, 150)}` : ""}`,
      );
    }
    const backoff = 500 * Math.pow(2, attempt) + Math.random() * 250;
    await sleep(backoff);
  }

  if (!res || !res.ok) {
    throw new Error(
      `exa returned ${lastStatus}${lastDetail ? `: ${lastDetail.slice(0, 150)}` : ""}`,
    );
  }

  const data = (await res.json()) as { results?: ExaResult[] };
  const results = data.results ?? [];

  for (const r of results) {
    if (!r.url || !/linkedin\.com\/in\//i.test(r.url)) continue;

    // profile validation — name + employer + currently-active. drops:
    //   1. wrong-person matches where name matches but profile is unrelated
    //   2. wrong-school matches (e.g. Samantha Kayhart at Mount Abraham, not CVU)
    //   3. retired / former profiles (e.g. Karen Kirkland's "L.L.Bean... Retired")
    if (!resultMatchesTeacher(r, teacher, fallbackContext)) continue;

    const profileUrl = r.url.replace(/[?#].*$/, "");
    const role = r.title ? parseExaTitle(r.title).role : null;

    return { found: true, profileUrl, title: role };
  }

  return { found: false };
}

/**
 * variant used when an LLM judge will adjudicate matches after the fact.
 * returns up to N candidates per teacher that pass CHEAP deterministic checks
 * (name match + not retired/former) — leaves employer/school matching + any
 * subtle currently-active judgment to the batch LLM pass.
 */
async function fetchLinkedinCandidatesViaExa(
  teacher: Teacher,
  context: string,
  apiKey: string,
): Promise<ExaResult[]> {
  const query = `${teacher.firstName} ${teacher.lastName} ${context}`;

  let res: Response | null = null;
  let lastStatus = 0;
  let lastDetail = "";
  for (let attempt = 0; attempt <= EXA_MAX_RETRIES; attempt++) {
    await exaRateLimit();
    res = await fetch("https://api.exa.ai/search", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
      },
      body: JSON.stringify({ query, category: "people", numResults: 5 }),
    });
    if (res.ok) break;
    lastStatus = res.status;
    lastDetail = await res.text().catch(() => "");
    const retriable = res.status === 429 || res.status >= 500;
    if (!retriable || attempt === EXA_MAX_RETRIES) {
      throw new Error(
        `exa returned ${res.status}${lastDetail ? `: ${lastDetail.slice(0, 150)}` : ""}`,
      );
    }
    await sleep(500 * Math.pow(2, attempt) + Math.random() * 250);
  }
  if (!res || !res.ok) {
    throw new Error(
      `exa returned ${lastStatus}${lastDetail ? `: ${lastDetail.slice(0, 150)}` : ""}`,
    );
  }
  const data = (await res.json()) as { results?: ExaResult[] };
  const results = data.results ?? [];

  const passing: ExaResult[] = [];
  for (const r of results) {
    if (!r.url || !/linkedin\.com\/in\//i.test(r.url)) continue;
    if (!nameMatchesTeacher(r, teacher)) continue;
    if (r.title && isRetiredOrFormer(r.title)) continue;
    passing.push(r);
  }
  return passing;
}

/**
 * exa-path enrichment with batch LLM adjudication. two phases:
 *
 *   1. workers fetch per-teacher candidate lists (up to 5 each) that pass a
 *      cheap deterministic filter (name match + not retired). wrong-school or
 *      ambiguous candidates are KEPT — the LLM decides.
 *   2. a single LLM batch call judges every surviving (teacher, candidate)
 *      pair. for each teacher we take the first LLM-approved candidate.
 *
 * falls back to the per-candidate keyword validator (resultMatchesTeacher) if
 * the LLM call errors — the pipeline never blocks on this.
 */
async function enrichViaExaBatched(
  teachers: Teacher[],
  fallbackContext: string,
  exaKey: string,
  log: (msg: string) => void,
): Promise<{ teachers: Teacher[]; stats: LinkedinEnrichmentStats }> {
  const stats: LinkedinEnrichmentStats = {
    processed: 0,
    matched: 0,
    noMatch: 0,
    failed: 0,
    firstFailure: null,
  };

  const queue = prioritize(teachers);
  const total = queue.length;

  // per-teacher candidate bucket (parallel array with queue)
  const buckets: Array<ExaResult[] | null> = new Array(total).fill(null);
  const errors: Array<Error | null> = new Array(total).fill(null);

  let cursor = 0;
  const workerCount = Math.max(1, Math.min(EXA_CONCURRENCY, total));
  log(`fetching exa candidates for ${total} teachers across ${workerCount} workers`);

  async function worker(): Promise<void> {
    while (true) {
      const myIdx = cursor++;
      if (myIdx >= total) break;
      const teacher = queue[myIdx]!;
      const context = contextFor(teacher, fallbackContext);
      try {
        const candidates = await fetchLinkedinCandidatesViaExa(
          teacher,
          context,
          exaKey,
        );
        buckets[myIdx] = candidates;
        debug("LINKEDIN", `exa · ${teacher.firstName} ${teacher.lastName} → ${candidates.length} candidates`, candidates.map((c) => ({ url: c.url, title: c.title })));
      } catch (err) {
        errors[myIdx] = err instanceof Error ? err : new Error(String(err));
        debugWarn("LINKEDIN", `exa fetch failed · ${teacher.firstName} ${teacher.lastName}`, { err: err instanceof Error ? err.message : String(err) });
      }
    }
  }
  await Promise.all(Array.from({ length: workerCount }, worker));

  // flatten surviving candidates into a single batch for the LLM
  type FlatEntry = {
    teacherIdx: number;
    candidate: ExaResult;
  };
  const flat: FlatEntry[] = [];
  for (let i = 0; i < total; i++) {
    const bucket = buckets[i];
    if (!bucket) continue;
    for (const c of bucket) flat.push({ teacherIdx: i, candidate: c });
  }

  log(`judging ${flat.length} candidate matches with LLM...`);
  const llmInput: LinkedinCandidate[] = flat.map((f, i) => {
    const teacher = queue[f.teacherIdx]!;
    return {
      teacherIndex: f.teacherIdx,
      teacher: {
        firstName: teacher.firstName,
        lastName: teacher.lastName,
        schoolName: teacher.schoolName ?? null,
        districtName: fallbackContext || null,
        role: teacher.role,
      },
      candidate: {
        url: f.candidate.url,
        title: f.candidate.title ?? "",
      },
    };
  });

  const judgments = await judgeLinkedinCandidates(llmInput);
  const approved = new Set<number>();
  if (judgments && judgments.length === flat.length) {
    for (const j of judgments) if (j.isMatch) approved.add(j.index);
    log(`LLM approved ${approved.size}/${flat.length} candidates`);
  } else {
    // fallback: use the inline keyword validator on every candidate
    log("LLM judge unavailable, falling back to keyword validation");
    for (let i = 0; i < flat.length; i++) {
      const { teacherIdx, candidate } = flat[i]!;
      const teacher = queue[teacherIdx]!;
      if (resultMatchesTeacher(candidate, teacher, fallbackContext)) {
        approved.add(i);
      }
    }
  }

  // pick first approved candidate per teacher
  const firstApprovedPerTeacher = new Map<number, ExaResult>();
  for (let i = 0; i < flat.length; i++) {
    if (!approved.has(i)) continue;
    const { teacherIdx, candidate } = flat[i]!;
    if (firstApprovedPerTeacher.has(teacherIdx)) continue;
    firstApprovedPerTeacher.set(teacherIdx, candidate);
  }

  // apply to teachers
  for (let i = 0; i < total; i++) {
    const teacher = queue[i]!;
    const label = `${teacher.firstName} ${teacher.lastName}`;
    stats.processed++;

    if (errors[i]) {
      stats.failed++;
      const msg = errors[i]!.message;
      if (!stats.firstFailure) stats.firstFailure = msg;
      log(`[${stats.processed}/${total}] failed — ${label}: ${msg}`);
      continue;
    }

    const picked = firstApprovedPerTeacher.get(i);
    if (!picked) {
      stats.noMatch++;
      log(`[${stats.processed}/${total}] no match — ${label}`);
      continue;
    }

    const profileUrl = picked.url.replace(/[?#].*$/, "");
    const role = picked.title ? parseExaTitle(picked.title).role : null;

    teacher.linkedinUrl = profileUrl;
    const pickedRole = pickEnrichedRole(teacher.role, teacher.department, role);
    if (pickedRole) teacher.role = pickedRole;
    if (!teacher.sources.includes("linkedin")) {
      teacher.sources.push("linkedin");
    }
    teacher.confidence = bumpConfidence(teacher.confidence);
    stats.matched++;
    log(`[${stats.processed}/${total}] found — ${label} — ${role ?? "(no title)"}`);
  }

  return { teachers, stats };
}

/**
 * enrich teachers with linkedin profile urls + job titles.
 *
 * provider is picked based on env: EXA_API_KEY → Exa people search (cleaner,
 * ~5× hit rate); otherwise falls back to DDG HTML scrape.
 */
export async function enrichWithLinkedin(
  teachers: Teacher[],
  fallbackContext: string,
  onStatus?: (msg: string) => void,
): Promise<{ teachers: Teacher[]; stats: LinkedinEnrichmentStats }> {
  const log = onStatus ?? (() => {});
  const stats: LinkedinEnrichmentStats = {
    processed: 0,
    matched: 0,
    noMatch: 0,
    failed: 0,
    firstFailure: null,
  };
  if (teachers.length === 0) return { teachers, stats };

  const exaKey = process.env.EXA_API_KEY?.trim();
  const useExa = !!exaKey;

  debug("LINKEDIN", `enrichWithLinkedin · ${teachers.length} teachers, provider=${useExa ? "exa" : "ddg"}, fallbackContext="${fallbackContext}"`);

  // exa path uses a two-phase flow: (1) concurrent fetch of ALL candidates
  // passing cheap deterministic checks, (2) one LLM batch call to pick the
  // true match per teacher. catches false positives (wrong school) AND false
  // negatives (employer-context heuristic too strict) that the old inline
  // keyword validator missed. falls back to the inline validator if the LLM
  // call fails.
  if (useExa) {
    const exaResult = await enrichViaExaBatched(
      teachers,
      fallbackContext,
      exaKey,
      log,
    );
    return { teachers: exaResult.teachers, stats: exaResult.stats };
  }

  const teacherMap = new Map(
    teachers.map((t) => [`${t.firstName} ${t.lastName}`, t]),
  );

  const queue = prioritize(teachers);
  const total = queue.length;
  let cursor = 0;
  let completed = 0;

  const workerCount = Math.max(
    1,
    Math.min(useExa ? EXA_CONCURRENCY : DDG_CONCURRENCY, total),
  );
  log(
    useExa
      ? `starting ${workerCount} exa workers for ${total} teachers`
      : `starting ${workerCount} ddg workers for ${total} teachers`,
  );

  async function worker(id: number): Promise<void> {
    let isFirst = true;
    while (true) {
      const myIdx = cursor++;
      if (myIdx >= queue.length) break;

      // DDG needs per-worker jitter between requests to stay below the rate
      // limit; exa doesn't. skip wait on first request so startup isn't sluggish.
      if (!isFirst && !useExa) await sleep(ddgJitter());
      isFirst = false;

      const teacher = queue[myIdx]!;
      const label = `${teacher.firstName} ${teacher.lastName}`;
      const context = contextFor(teacher, fallbackContext);

      try {
        const result = useExa
          ? await searchLinkedinViaExa(teacher, context, fallbackContext, exaKey)
          : await searchLinkedinViaDdg(teacher, context);
        completed++;
        stats.processed++;

        if (!result.found || !result.profileUrl) {
          stats.noMatch++;
          log(`[${completed}/${total}] no match — ${label}`);
          continue;
        }

        const original = teacherMap.get(label);
        if (!original) continue;

        original.linkedinUrl = result.profileUrl;
        const pickedRole = pickEnrichedRole(original.role, original.department, result.title);
        if (pickedRole) original.role = pickedRole;
        if (!original.sources.includes("linkedin")) {
          original.sources.push("linkedin");
        }
        original.confidence = bumpConfidence(original.confidence);
        stats.matched++;

        log(`[${completed}/${total}] found — ${label} — ${result.title ?? "(no title)"}`);
      } catch (err) {
        completed++;
        stats.processed++;
        stats.failed++;
        const msg = err instanceof Error ? err.message : String(err);
        if (!stats.firstFailure) stats.firstFailure = msg;
        log(`[${completed}/${total}] failed — ${label}: ${msg}`);
      }
    }
  }

  await Promise.all(
    Array.from({ length: workerCount }, (_, i) => worker(i + 1)),
  );

  log(`linkedin enrichment complete: ${stats.matched}/${total} matched`);
  return { teachers, stats };
}
