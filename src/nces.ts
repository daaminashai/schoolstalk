// ── nces.ts — look up verified school info from the federal NCES database ──

import type { Address, NCESDistrictRecord, NCESSchoolRecord } from "./types";
import { debug, debugWarn } from "./debug";

const PRIMARY_HOST = "https://educationdata.urban.org";
// urban institute's prod api occasionally hangs post-tls (see prod issue #123 on
// UrbanInstitute/education-data-package-r). the staging subdomain exposes the
// same schema and stays up during prod outages, so we use it as a fallback.
const FALLBACK_HOST = "https://educationdata-stg.urban.org";
const API_BASE = `${PRIMARY_HOST}/api/v1/schools/ccd/directory/2023`;
const DISTRICT_API_BASE = `${PRIMARY_HOST}/api/v1/school-districts/ccd/directory/2023`;

// per-state LEA roster cache. the urban institute district endpoint returns
// ALL ~1000 districts for a state in one response, so we fetch once per state
// and reuse for every subsequent lookup in the session.
const districtCache = new Map<string, NCESDistrictRecord[]>();

// circuit breaker: the urban institute API periodically goes down (cloudflare
// 520/524 gateway errors with connections that hang rather than error). once
// we've seen N consecutive failures, short-circuit further calls for the rest
// of the session so a down API doesn't block the whole pipeline.
const NCES_TIMEOUT_MS = 15_000;
const NCES_FAILURE_THRESHOLD = 3;
let ncesConsecutiveFailures = 0;
let ncesCircuitOpen = false;

function recordNcesOutcome(ok: boolean): void {
  if (ok) {
    ncesConsecutiveFailures = 0;
    return;
  }
  ncesConsecutiveFailures++;
  if (ncesConsecutiveFailures >= NCES_FAILURE_THRESHOLD && !ncesCircuitOpen) {
    ncesCircuitOpen = true;
    console.error(
      `[nces] disabled after ${NCES_FAILURE_THRESHOLD} consecutive failures — API appears down`,
    );
  }
}

async function tryFetch(url: string): Promise<Response | null> {
  try {
    return await fetch(url, { signal: AbortSignal.timeout(NCES_TIMEOUT_MS) });
  } catch (err) {
    debugWarn("NCES", `fetch failed for ${url}`, { err: err instanceof Error ? err.message : String(err) });
    return null;
  }
}

async function ncesFetch(url: string): Promise<Response | null> {
  if (ncesCircuitOpen) return null;
  const primary = await tryFetch(url);
  if (primary && primary.ok) {
    recordNcesOutcome(true);
    return primary;
  }

  // fallback: retry on staging subdomain. same schema, stays up during prod
  // outages. only count a failure if BOTH hosts fail, so the circuit breaker
  // doesn't trip on a recoverable prod hiccup.
  if (url.startsWith(PRIMARY_HOST)) {
    const fallbackUrl = FALLBACK_HOST + url.slice(PRIMARY_HOST.length);
    debug("NCES", `primary failed (${primary?.status ?? "timeout"}), retrying on staging: ${fallbackUrl}`);
    const secondary = await tryFetch(fallbackUrl);
    if (secondary && secondary.ok) {
      recordNcesOutcome(true);
      return secondary;
    }
  }

  recordNcesOutcome(false);
  return primary;
}

// ── public api ──────────────────────────────────────────────────────────────────

/**
 * queries the urban institute education data portal for a school by name.
 * optionally filters by 2-letter state code. returns the best fuzzy match,
 * or null if nothing reasonable comes back.
 */
export async function lookupSchool(
  schoolName: string,
  state?: string,
): Promise<NCESSchoolRecord | null> {
  const params = new URLSearchParams({ school_name: schoolName });
  if (state) params.set("state_location", state.toLowerCase());

  const url = `${API_BASE}/?${params}`;
  debug("NCES", `lookupSchool · name="${schoolName}" state=${state ?? "-"}`, { url });

  try {
    const res = await ncesFetch(url);
    if (!res) return null;
    if (!res.ok) {
      debugWarn("NCES", `lookupSchool ${res.status} for ${url}`);
      console.error(`[nces] api returned ${res.status} for ${url}`);
      return null;
    }

    const data = (await res.json()) as { results: NCESSchoolRecord[] };
    if (!data.results?.length) {
      debug("NCES", `lookupSchool · 0 results for "${schoolName}"`);
      return null;
    }

    // single result — just return it
    if (data.results.length === 1) {
      debug("NCES", `lookupSchool · 1 result → ${data.results[0]?.school_name}`, data.results[0]);
      return data.results[0] ?? null;
    }

    // multiple results — pick the best fuzzy match
    const pick = bestMatch(schoolName, data.results);
    debug("NCES", `lookupSchool · ${data.results.length} results, picked "${pick?.school_name ?? "(none)"}"`, {
      top5: data.results.slice(0, 5).map((r) => ({ name: r.school_name, city: r.city_location, ncessch: r.ncessch })),
      picked: pick,
    });
    return pick;
  } catch (err) {
    debugWarn("NCES", `lookupSchool threw`, { err: err instanceof Error ? err.message : String(err) });
    console.error(`[nces] failed to fetch school data:`, err);
    return null;
  }
}

/**
 * find the NCES LEA that best matches a scraped district name, scoped to a
 * state. hits the district directory endpoint (which actually respects the
 * state filter even though school_name/lea_name are ignored), loads the full
 * roster for the state, then fuzzy-matches client-side. caches per-state so
 * each state is fetched once per session.
 */
export async function lookupDistrict(
  districtName: string,
  state: string,
  scrapedCity?: string,
): Promise<NCESDistrictRecord | null> {
  if (!districtName.trim()) return null;
  const stateKey = state.toLowerCase();

  let roster = districtCache.get(stateKey);
  if (!roster) {
    debug("NCES", `lookupDistrict · fetching ${state} LEA roster (not cached)`);
    roster = await fetchDistrictRoster(stateKey);
    districtCache.set(stateKey, roster);
    debug("NCES", `lookupDistrict · fetched ${roster.length} LEAs for ${state}`);
  } else {
    debug("NCES", `lookupDistrict · reusing cached ${state} roster (${roster.length} LEAs)`);
  }
  if (roster.length === 0) return null;

  const fuzzy = fuzzyMatchDistrict(districtName, roster);
  if (fuzzy) {
    debug("NCES", `lookupDistrict · fuzzy picked "${fuzzy.lea_name}"`);
    return fuzzy;
  }

  // last-resort fallback: if fuzzy matching comes up empty but we have a
  // scraped city, try the most closely-named LEA headquartered in that city.
  if (scrapedCity) {
    const qNorm = normalize(districtName);
    const cityNorm = scrapedCity.trim().toLowerCase();
    const cityMatches = roster.filter(
      (r) =>
        r.city_location?.toLowerCase() === cityNorm ||
        r.city_mailing?.toLowerCase() === cityNorm,
    );

    if (cityMatches.length > 0) {
      let cityBest: NCESDistrictRecord | null = null;
      let cityBestScore = -Infinity;
      for (const r of cityMatches) {
        const dist = levenshtein(qNorm, normalize(r.lea_name));
        const maxLen = Math.max(districtName.length, r.lea_name.length, 1);
        const score = 1 - dist / maxLen;
        if (score > cityBestScore) {
          cityBestScore = score;
          cityBest = r;
        }
      }
      if (cityBest) {
        debug("NCES", `lookupDistrict · city fallback "${cityBest.lea_name}" via city="${scrapedCity}" (${cityMatches.length} candidates)`);
        return cityBest;
      }
    }
  }

  return null;
}

/**
 * deterministic district matcher. this is less semantically capable than the
 * removed LLM path, but it is cheap, predictable, and token-free.
 */
function fuzzyMatchDistrict(
  districtName: string,
  roster: NCESDistrictRecord[],
): NCESDistrictRecord | null {
  if (roster.length === 0) return null;

  const qTokens = tokenize(districtName);
  const qNorm = normalize(districtName);
  const qInitialism = extractInitialism(districtName);

  let best: NCESDistrictRecord | null = null;
  let bestScore = -Infinity;

  for (const r of roster) {
    const nTokens = tokenize(r.lea_name);
    const overlap = setOverlap(qTokens, nTokens);
    const dist = levenshtein(qNorm, normalize(r.lea_name));
    const maxLen = Math.max(districtName.length, r.lea_name.length, 1);
    const lev = 1 - dist / maxLen;

    const initialsMatch =
      qInitialism && acronymOf(r.lea_name).includes(qInitialism);
    const boost = initialsMatch ? 0.4 : 0;

    const score = overlap * 0.6 + lev * 0.2 + boost;
    if (score > bestScore) {
      bestScore = score;
      best = r;
    }
  }

  // 0.3 clears real matches (even with abbreviations) but rejects the
  // "nothing matched" case.
  return bestScore >= 0.3 ? best : null;
}

async function fetchDistrictRoster(state: string): Promise<NCESDistrictRecord[]> {
  const records: NCESDistrictRecord[] = [];
  let page = 1;
  while (true) {
    const params = new URLSearchParams({ state_location: state, page: String(page) });
    const url = `${DISTRICT_API_BASE}/?${params}`;
    const res = await ncesFetch(url);
    if (!res) break;
    if (!res.ok) {
      console.error(`[nces] district api returned ${res.status} for ${url}`);
      break;
    }
    try {
      const data = (await res.json()) as {
        results: NCESDistrictRecord[];
        next?: string | null;
      };
      if (!data.results?.length) break;
      records.push(...data.results);
      if (!data.next) break;
      page++;
    } catch (err) {
      console.error(`[nces] failed to parse district roster:`, err);
      break;
    }
  }
  return records;
}

/**
 * lists every school in a district by its NCES LEA id. walks paginated results
 * until the api reports no more pages. the LEA roster is the authoritative list
 * for per-teacher school resolution in district mode.
 */
export async function lookupSchoolsInDistrict(
  leaid: string,
): Promise<NCESSchoolRecord[]> {
  const records: NCESSchoolRecord[] = [];
  let page = 1;

  while (true) {
    const params = new URLSearchParams({ leaid, page: String(page) });
    const url = `${API_BASE}/?${params}`;

    const res = await ncesFetch(url);
    if (!res) break;
    if (!res.ok) {
      console.error(`[nces] api returned ${res.status} for ${url}`);
      break;
    }
    try {
      const data = (await res.json()) as {
        results: NCESSchoolRecord[];
        next?: string | null;
      };

      if (!data.results?.length) break;
      records.push(...data.results);

      if (!data.next) break;
      page++;
    } catch (err) {
      debugWarn("NCES", `lookupSchoolsInDistrict threw`, { err: err instanceof Error ? err.message : String(err) });
      console.error(`[nces] failed to parse district schools:`, err);
      break;
    }
  }

  return records;
}

/**
 * given a list of schools in a district and a scraped school name, return the
 * nces record that best matches. uses word-overlap + levenshtein on normalized
 * names. returns null if no candidate is reasonably close.
 */
export function matchSchoolInDistrict(
  scrapedName: string,
  schools: NCESSchoolRecord[],
): NCESSchoolRecord | null {
  if (!scrapedName.trim() || schools.length === 0) return null;

  // extract any uppercase initialism from the scraped name (e.g. "CVU" from
  // "CVU High School"). we'll boost roster candidates whose first-letter
  // acronym matches this — catches cases like "CVU High School" vs
  // "Champlain Valley Union High School" that pure word-overlap misses.
  const initialism = extractInitialism(scrapedName);

  // tokenize the scraped name (excluding generic fillers) for word overlap
  const qWords = tokenize(scrapedName);

  let best: NCESSchoolRecord | null = null;
  let bestScore = -Infinity;

  for (const s of schools) {
    const nWords = tokenize(s.school_name);
    const overlap = setOverlap(qWords, nWords);

    const dist = levenshtein(
      normalize(scrapedName),
      normalize(s.school_name),
    );
    const maxLen = Math.max(scrapedName.length, s.school_name.length, 1);
    const lev = 1 - dist / maxLen;

    // initialism boost: if the scraped name has an all-caps word like "CVU"
    // and the candidate's first-letters-of-words match it, give a big bump.
    const initialsMatch =
      initialism && acronymOf(s.school_name).includes(initialism);
    const boost = initialsMatch ? 0.4 : 0;

    const score = overlap * 0.6 + lev * 0.2 + boost;
    if (score > bestScore) {
      bestScore = score;
      best = s;
    }
  }

  // threshold tuned after abbreviation expansion — real matches score ≥0.3 on
  // real rosters; no-signal matches score well below.
  return bestScore >= 0.28 ? best : null;
}

/** pull out any all-caps word of 2-5 letters — typical school acronym shape */
function extractInitialism(s: string): string | null {
  const match = s.match(/\b([A-Z]{2,5})\b/);
  return match ? match[1]!.toLowerCase() : null;
}

/** build a first-letter acronym from a multi-word name */
function acronymOf(s: string): string {
  return (s.match(/\b[a-zA-Z]/g) ?? []).join("").toLowerCase();
}

// words we ignore when comparing names — they carry no discriminating info.
// NOTE: "academy", "institute", "center", "centre" were in this list originally
// but are too often a distinctive part of a district/school name
// (e.g. "Academy District 20" becomes just {20} if "academy" is generic, which
// then fuzzy-matches any district with "20" in the name). Keep them in.
const GENERIC_TOKENS = new Set([
  "school", "schools", "the", "of", "and", "at", "for",
  "district", "public", "unified", "consolidated", "union",
  "county", "city", "township", "twp", "area", "regional",
  // "independent" is part of nearly every Texas district name ("Austin ISD" =
  // "Austin Independent School District"); treating it as discriminative
  // previously let "Austin Discovery School" beat the real LEA match on
  // jaccard because the scraper's "Austin ISD" vs the roster's "Austin
  // Independent..." only overlapped on "austin".
  "independent",
]);

// common abbreviations → canonical forms. applied before tokenizing so that
// "Shaker Hts HS" tokenizes the same as "Shaker Heights High School".
const ABBREVIATIONS: Array<[RegExp, string]> = [
  [/\bhts\b/gi, "heights"],
  [/\bht\b/gi, "heights"],
  [/\bmt\b/gi, "mount"],
  [/\bst\b/gi, "saint"],
  [/\bft\b/gi, "fort"],
  [/\bsr\b/gi, "senior"],
  [/\bjr\b/gi, "junior"],
  [/\belem\b/gi, "elementary"],
  [/\bprim\b/gi, "primary"],
  [/\bhs\b/gi, "high"],
  [/\bms\b/gi, "middle"],
  [/\bes\b/gi, "elementary"],
  [/\bctr\b/gi, "center"],
  [/\bintermed\b/gi, "intermediate"],
  [/\bhsd\b/gi, "district"],
  [/\busd\b/gi, "district"],
  [/\bisd\b/gi, "district"],
  [/\bpsd\b/gi, "district"],
  [/\bsd\b/gi, "district"],
  [/\bsch\b/gi, "school"],
  [/\bno\.\s*(\d+)/gi, "$1"],
  [/\b#\s*(\d+)/gi, "$1"],
];

function expandAbbrev(name: string): string {
  let out = name;
  for (const [re, repl] of ABBREVIATIONS) out = out.replace(re, repl);
  return out;
}

/** tokenize a school name into content words, dropping generic fillers */
function tokenize(name: string): Set<string> {
  // lowercase first, then split on anything non-alphanumeric. using the
  // project-wide `normalize()` here would have dropped spaces and collapsed
  // the whole string into a single token.
  const expanded = expandAbbrev(name).toLowerCase();
  const words = expanded.match(/[a-z0-9]+/g) ?? [];
  return new Set(words.filter((w) => !GENERIC_TOKENS.has(w) && w.length > 1));
}

/** district-record → Address (district endpoint has different field set). */
export function districtRecordToAddress(record: NCESDistrictRecord): Address {
  const mailingMissing =
    record.street_mailing === "-1" ||
    record.city_mailing === "-1" ||
    record.zip_mailing === "-1";
  if (mailingMissing) {
    return {
      street: titleCaseAddressPart(record.street_location),
      city: titleCaseAddressPart(record.city_location),
      state: record.state_location,
      zip: "",
      source: "nces",
    };
  }
  return {
    street: titleCaseAddressPart(record.street_mailing),
    city: titleCaseAddressPart(record.city_mailing),
    state: record.state_mailing,
    zip: record.zip_mailing,
    source: "nces",
  };
}

// NCES ships address strings in ALL CAPS. title-case for display so CSV rows
// don't go "1057 POST RD" next to "134 Mamaroneck Rd". 2-letter tokens that
// are known US state codes or common directional abbreviations stay upper.
const ADDRESS_KEEP_UPPER = new Set([
  "N", "S", "E", "W", "NE", "NW", "SE", "SW",
  "NNE", "NNW", "SSE", "SSW", "ENE", "ESE", "WNW", "WSW",
  "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
  "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
  "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
  "TX","UT","VT","VA","WA","WV","WI","WY","DC",
  "US", "USA", "PO", "RR", "HC",
]);

// NCES uses non-standard street-suffix abbreviations (e.g. "BV" for Boulevard,
// "PK" for Parkway) in some records. normalize to the canonical USPS form
// during title-casing so "4700 STONECROFT BV" renders as "4700 Stonecroft Blvd"
// — otherwise the map above turns "BV" into "Bv" and readers can't tell what
// street type it is.
const STREET_SUFFIX_CANONICAL: Record<string, string> = {
  BV: "Blvd",
  BL: "Blvd",
  BLVD: "Blvd",
  BOULEVARD: "Boulevard",
  AV: "Ave",
  AVE: "Ave",
  AVENUE: "Avenue",
  ST: "St",
  STREET: "Street",
  RD: "Rd",
  ROAD: "Road",
  DR: "Dr",
  DRIVE: "Drive",
  LN: "Ln",
  LANE: "Lane",
  CT: "Ct",
  COURT: "Court",
  CIR: "Cir",
  CIRCLE: "Circle",
  PL: "Pl",
  PLACE: "Place",
  TER: "Ter",
  TERRACE: "Terrace",
  HWY: "Hwy",
  HIGHWAY: "Highway",
  PKY: "Pkwy",
  PKWY: "Pkwy",
  PARKWAY: "Parkway",
  PLZ: "Plaza",
  PLAZA: "Plaza",
  SQ: "Sq",
  SQUARE: "Square",
  TRL: "Trl",
  TRAIL: "Trail",
  WY: "Way",
  WAY: "Way",
  EXPY: "Expy",
  EXPRESSWAY: "Expressway",
  FWY: "Fwy",
  FREEWAY: "Freeway",
  ALY: "Aly",
  ALLEY: "Alley",
  BLDG: "Bldg",
  STE: "Ste",
  APT: "Apt",
  RM: "Rm",
};

function titleCaseAddressPart(raw: string | null | undefined): string {
  if (!raw) return "";
  const s = raw.trim();
  if (!s) return "";

  // NCES data is inconsistent: some records come back ALL CAPS ("4700 STONECROFT BV")
  // and others are already title-cased but with non-standard suffix abbrevs
  // ("4700 Stonecroft Bv"). always walk tokens so we can canonicalize both.
  const alreadyMixedCase = /[a-z]/.test(s);
  return s
    .split(/(\s+|-)/)
    .map((w) => {
      if (!w.trim() || w === "-") return w;
      const upper = w.toUpperCase();
      const canonical = STREET_SUFFIX_CANONICAL[upper];
      if (canonical) return canonical;
      if (ADDRESS_KEEP_UPPER.has(upper)) return upper;
      // when input was already mixed case and the token wasn't a known
      // suffix/directional/state code, leave it alone — preserves proper
      // nouns ("O'Brien", "MacPherson") that would lose casing otherwise.
      if (alreadyMixedCase) return w;
      // preserve tokens starting with a digit (e.g. "1st", "2ND")
      if (/^\d/.test(w)) {
        return w.replace(/([a-zA-Z]+)$/, (m) => m.toLowerCase());
      }
      return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
    })
    .join("");
}

/**
 * converts an NCES record into our Address type.
 * prefers mailing address; falls back to location address when mailing
 * fields are "-1" (the api's sentinel for missing data).
 */
export function ncesRecordToAddress(record: NCESSchoolRecord): Address {
  const mailingMissing =
    record.street_mailing === "-1" ||
    record.city_mailing === "-1" ||
    record.zip_mailing === "-1";

  if (mailingMissing) {
    return {
      street: titleCaseAddressPart(record.street_location),
      city: titleCaseAddressPart(record.city_location),
      state: record.state_location,
      zip: record.zip_location,
      source: "nces",
    };
  }

  return {
    street: titleCaseAddressPart(record.street_mailing),
    city: titleCaseAddressPart(record.city_mailing),
    state: record.state_mailing,
    zip: record.zip_mailing,
    source: "nces",
  };
}

/**
 * tries to pull a human-readable school name out of a url.
 *   "https://www.lincolnhigh.edu"              → "lincoln high"
 *   "https://schools.district.org/jefferson-middle" → "jefferson middle"
 * returns null if nothing useful can be extracted.
 */
export function extractSchoolNameFromUrl(url: string): string | null {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return null;
  }

  // try the last meaningful path segment first (e.g. /jefferson-middle)
  const segments = parsed.pathname.split("/").filter(Boolean);
  const lastSegment = segments.at(-1);
  if (lastSegment && !looksLikeJunk(lastSegment)) {
    return humanize(lastSegment);
  }

  // fall back to the hostname — strip www/schools subdomains and the tld
  const hostParts = parsed.hostname.split(".");
  // drop common prefixes
  while (hostParts.length > 1 && ["www", "schools", "school", "sites"].includes(hostParts[0]!)) {
    hostParts.shift();
  }
  // drop tld (and second-level tld like .co.uk)
  if (hostParts.length > 1) hostParts.pop();
  if (hostParts.length > 1 && hostParts.at(-1)!.length <= 3) hostParts.pop();

  const candidate = hostParts.join("");
  if (!candidate || looksLikeJunk(candidate)) return null;

  return humanize(candidate);
}

// ── helpers ─────────────────────────────────────────────────────────────────────

/** picks the NCESSchoolRecord whose name best matches the query */
function bestMatch(query: string, records: NCESSchoolRecord[]): NCESSchoolRecord {
  const q = normalize(query);
  let best = records[0]!;
  let bestScore = Infinity;

  for (const record of records) {
    const score = levenshtein(q, normalize(record.school_name));
    if (score < bestScore) {
      bestScore = score;
      best = record;
    }
  }

  return best;
}

/** lowercases and strips non-alphanumeric chars for comparison */
function normalize(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9]/g, "");
}

/** jaccard overlap between two string sets */
function setOverlap(a: Set<string>, b: Set<string>): number {
  if (a.size === 0 && b.size === 0) return 1;
  if (a.size === 0 || b.size === 0) return 0;
  let inter = 0;
  for (const w of a) if (b.has(w)) inter++;
  const union = new Set([...a, ...b]).size;
  return inter / union;
}

/** classic levenshtein distance — no external deps needed */
function levenshtein(a: string, b: string): number {
  if (a === b) return 0;
  if (!a.length) return b.length;
  if (!b.length) return a.length;

  // single-row dp — only need the previous row at any point
  const row = Array.from({ length: b.length + 1 }, (_, i) => i);

  for (let i = 1; i <= a.length; i++) {
    let prev = i;
    for (let j = 1; j <= b.length; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      const val = Math.min(
        row[j]! + 1,      // deletion
        prev + 1,          // insertion
        row[j - 1]! + cost // substitution
      );
      row[j - 1] = prev;
      prev = val;
    }
    row[b.length] = prev;
  }

  return row[b.length]!;
}

/**
 * splits camelCase / kebab-case / run-together words into a readable name.
 * "lincolnhigh" → "lincoln high", "jefferson-middle" → "jefferson middle"
 */
function humanize(slug: string): string {
  return slug
    .replace(/[-_]/g, " ")          // kebab/snake → spaces
    .replace(/([a-z])([A-Z])/g, "$1 $2") // camelCase → spaces
    .replace(/\d+/g, "")            // drop numbers
    .toLowerCase()
    .trim()
    .replace(/\s+/g, " ") || null as unknown as string;
}

/** returns true if a url segment is too generic to be a school name */
function looksLikeJunk(s: string): boolean {
  const junk = new Set([
    "index", "home", "about", "main", "default",
    "staff", "directory", "contact", "info", "page",
  ]);
  return s.length < 3 || junk.has(s.toLowerCase());
}
