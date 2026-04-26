// ── validate, enrich, and deduplicate raw teacher data ──

import type {
  RawTeacherData,
  Teacher,
  ConfidenceScore,
  DataSource,
  HackerScore,
} from "./types";
import { parseName, normalizeEmail } from "./utils";
import { canonicalizeDepartment } from "./names";

// ── stem keyword lists ──

const MATH_KEYWORDS = [
  "algebra", "geometry", "calculus", "precalculus", "trigonometry",
  "statistics", "probability", "math", "mathematics",
];

const SCIENCE_KEYWORDS = [
  "physics", "chemistry", "biology", "environmental science", "earth science",
  "geology", "anatomy", "physiology", "forensic science", "marine biology",
  "astronomy", "physical science", "life science", "science",
];

const TECH_KEYWORDS = [
  "computer science", "engineering", "stem", "technology", "robotics",
  "coding", "programming", "information technology",
];

const STEM_KEYWORDS = [...TECH_KEYWORDS, ...SCIENCE_KEYWORDS, ...MATH_KEYWORDS];

// phrases that look like STEM but aren't
const GENERIC_DEPT_LABELS = new Set(["Science", "STEM", "Technology"]);

const FALSE_POSITIVE_PATTERNS = [
  /\bpolitical\s+science\b/i,
  /\bsocial\s+science\b/i,
  /\blibrary\s+science\b/i,
  /\bexercise\s+science\b/i,
  /\bsports?\s+science\b/i,
  /\bscience\s+of\b(?!\s+(?:physics|chemistry|biology|engineering|computing|mathematics))/i,
  /\baftermath\b/i,
];

// tokens that indicate a value is a school/building name rather than a subject
const SCHOOL_NAME_MARKERS = [
  /\bschool\b/i,
  /\bacademy\b/i,
  /\belementary\b/i,
  /\bmiddle\b/i,
  /\bhigh\s+school\b/i,
  /\binstitute\b/i,
  /\bcampus\b/i,
];

/**
 * reconcile a scraper-assigned department with the subject(s) named in the
 * role text. handles three failure modes:
 *   1. dept is missing → derive from role text ("Math Teacher" → "Mathematics")
 *   2. dept is non-STEM ("Grade 2") → swap in a STEM subject from the role
 *   3. dept is STEM but doesn't match the role (andover: scraper stuffed
 *      "Computer Science" into every teacher's dept even for pure math
 *      instructors) → swap in the role-derived subject
 * guard: never downgrade a specific subject to a generic label, and never
 * override when the role text actually mentions the current dept.
 */
function reconcileDepartment(
  department: string | null,
  role: string | null | undefined,
): string | null {
  const fromRole = canonicalizeDepartment(role ?? null);

  // case 1: no dept — leave blank. Do NOT infer from role.
  // Previously we derived a STEM subject from the role text; this caused
  // hallucinated departments. The new contract is: if the site doesn't name
  // a department, keep it null and carry subject/grade info only in `role`.
  if (!department) return null;

  if (!fromRole || !isStemRole("", fromRole) || fromRole === department) return department;

  // case 2: current dept isn't STEM — always prefer role-derived STEM dept.
  if (!isStemRole("", department)) return fromRole;

  // case 3: current dept is STEM but role text disagrees. skip generic labels
  // and bail if the role actually mentions the current dept's keywords.
  if (GENERIC_DEPT_LABELS.has(fromRole)) return department;
  const roleLower = (role ?? "").toLowerCase();
  const deptWords = department.toLowerCase().split(" ").filter((w) => w.length > 2);
  if (deptWords.some((w) => roleLower.includes(w))) return department;
  return fromRole;
}

// ── public api ──

/**
 * checks if a role/department string indicates a STEM teacher.
 * matches against known stem keywords while filtering out false positives.
 */
export function isStemRole(role: string, department?: string | null): boolean {
  const combined = [role, department].filter(Boolean).join(" ").toLowerCase();

  if (!combined) return false;

  // reject false positives first
  for (const pattern of FALSE_POSITIVE_PATTERNS) {
    if (pattern.test(combined)) {
      // if the false positive consumes the only "science" or "math" in the string,
      // strip it and continue checking what's left
      const stripped = combined.replace(pattern, " ").trim();
      if (stripped === combined) continue; // pattern didn't match (shouldn't happen)

      // check if the stripped version still has stem keywords
      return hasStemKeyword(stripped);
    }
  }

  return hasStemKeyword(combined);
}

/**
 * returns true if the given value looks like a school/building name rather
 * than a subject department. used to scrub values that the scraper misfiled
 * into the department column on district sites.
 */
export function looksLikeSchoolName(value: string): boolean {
  if (!value) return false;
  for (const pattern of SCHOOL_NAME_MARKERS) {
    if (pattern.test(value)) return true;
  }
  return false;
}

function hasStemKeyword(text: string): boolean {
  for (const kw of STEM_KEYWORDS) {
    const pattern = new RegExp(`\\b${escapeRegex(kw)}\\b`, "i");
    if (pattern.test(text)) return true;
  }
  return false;
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * detects the email pattern from existing emails and fills in missing ones.
 * requires 3+ matching emails to infer the pattern.
 */
export function inferEmails(teachers: Teacher[]): Teacher[] {
  const withEmail = teachers.filter((t) => t.email);
  if (withEmail.length < 3) return teachers;

  // extract the shared domain from existing emails
  const domains = withEmail.map((t) => t.email!.split("@")[1] ?? "");
  const domainCounts = new Map<string, number>();
  for (const d of domains) {
    if (d) domainCounts.set(d, (domainCounts.get(d) ?? 0) + 1);
  }

  // pick the most common domain
  let topDomain = "";
  let topCount = 0;
  for (const [d, c] of domainCounts) {
    if (c > topCount) { topDomain = d; topCount = c; }
  }

  if (topCount < 3) return teachers;

  // try each pattern against teachers with that domain
  type PatternFn = (first: string, last: string, domain: string) => string;
  const patterns: { name: string; fn: PatternFn }[] = [
    { name: "first.last", fn: (f, l, d) => `${f}.${l}@${d}` },
    { name: "firstlast", fn: (f, l, d) => `${f}${l}@${d}` },
    { name: "first_last", fn: (f, l, d) => `${f}_${l}@${d}` },
    { name: "flast", fn: (f, l, d) => `${f[0]}${l}@${d}` },
  ];

  const domainTeachers = withEmail.filter((t) => t.email!.endsWith(`@${topDomain}`));

  let bestPattern: PatternFn | null = null;
  let bestMatches = 0;

  for (const { fn } of patterns) {
    let matches = 0;
    for (const t of domainTeachers) {
      const first = t.firstName.toLowerCase();
      const last = t.lastName.toLowerCase();
      if (!first || !last) continue;

      const expected = fn(first, last, topDomain);
      if (expected === t.email) matches++;
    }
    if (matches >= 3 && matches > bestMatches) {
      bestPattern = fn;
      bestMatches = matches;
    }
  }

  if (!bestPattern) return teachers;

  // apply the detected pattern to teachers missing emails
  return teachers.map((t) => {
    if (t.email) return t;

    const first = t.firstName.toLowerCase();
    const last = t.lastName.toLowerCase();
    if (!first || !last) return t;

    return {
      ...t,
      email: bestPattern!(first, last, topDomain),
      sources: [...t.sources.filter((s) => s !== "inferred"), "inferred" as DataSource],
    };
  });
}

/**
 * infer the canonical email domain from the MAJORITY of teacher emails. the
 * URL-derived domain is unreliable — users can type typo aliases (e.g.
 * `cvs-dvt.org` vs the real `cvsdvt.org`), redirect aliases, or the site may
 * use a different domain than its emails (e.g. `schoolwebsite.com` but emails
 * at `@district.edu`). majority-of-emails is the ground-truth domain for
 * confidence scoring.
 */
function inferCanonicalEmailDomain(
  teachers: { email: string | null }[],
  fallback: string,
): string {
  const counts = new Map<string, number>();
  for (const t of teachers) {
    const d = t.email?.split("@")[1]?.trim().toLowerCase();
    if (!d) continue;
    counts.set(d, (counts.get(d) ?? 0) + 1);
  }
  if (counts.size === 0) return fallback;

  let topDomain = fallback;
  let topCount = 0;
  for (const [d, c] of counts) {
    if (c > topCount) {
      topDomain = d;
      topCount = c;
    }
  }
  return topDomain;
}

export interface ValidateOptions {
  /**
   * when false, skip the keyword-based STEM filter so the orchestrator can
   * do its own LLM-powered filter pass. name parsing, email inference,
   * dedup, and confidence scoring still run. default true (backwards-compat).
   */
  stemFilter?: boolean;
}

/**
 * main validation pipeline — transforms raw extracted data into clean Teacher records.
 */
export function validateTeachers(
  raw: RawTeacherData[],
  schoolDomain: string,
  options: ValidateOptions = {},
): Teacher[] {
  const urlDomain = schoolDomain.toLowerCase().replace(/^www\./, "");
  const applyStemFilter = options.stemFilter ?? true;

  // step a + b: parse names, normalize emails, scrub column confusion
  let teachers: Teacher[] = raw
    .filter((r) => r.name?.trim())
    .map((r) => {
      const { firstName, lastName } = parseName(r.name);
      const email = r.email ? normalizeEmail(r.email) : null;

      // email-domain match is computed against the canonical domain below,
      // after we know the majority email domain. placeholder here.
      const emailDomainMatch = false;

  // scrub obviously misfiled department entries
  let department: string | null = r.department?.trim() || null;

      // canonicalize dept label so "Math" and "Mathematics" don't coexist in
      // the same CSV. runs after the school-name scrub so we don't canonicalize
      // something that turned out to be a building name.
      department = canonicalizeDepartment(department);

      department = reconcileDepartment(department, r.role);

      return {
        firstName,
        lastName,
        email,
        role: r.role?.trim() ?? "",
        department,
        linkedinUrl: null,
        sources: ["school_website"] as DataSource[],
        // classification disabled: keep neutral defaults
        confidence: 1 as ConfidenceScore,
        hackerScore: 1 as HackerScore,
        _emailDomainMatch: emailDomainMatch,
      };
    });

  // do not drop any teachers; only clean role if it looks like an email address
  teachers = teachers.map((t) => {
    if (/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(t.role)) {
      return { ...t, role: "" };
    }
    return t;
  });

  // step d: infer missing emails from detected patterns
  teachers = inferEmails(teachers);

  // classification disabled: do NOT filter out any teachers

  // step f: deduplicate by first + last name
  const seen = new Map<string, Teacher>();
  for (const t of teachers) {
    const key = `${t.firstName.toLowerCase()}|${t.lastName.toLowerCase()}`;
    const existing = seen.get(key);
    if (!existing || scoreTeacher(t) > scoreTeacher(existing)) {
      seen.set(key, t);
    }
  }
  teachers = [...seen.values()];

  // step f.2: deduplicate by email. catches the maiden/married-name case where
  // a directory lists the same person twice under different surnames ("Britton
  // (aka Sanchez)" + "Sanchez (see Britton above)" → one row). name-dedup in
  // step f can't catch this because the last names differ.
  const byEmail = new Map<string, Teacher>();
  const emailless: Teacher[] = [];
  for (const t of teachers) {
    if (!t.email) {
      emailless.push(t);
      continue;
    }
    const key = t.email.toLowerCase();
    const existing = byEmail.get(key);
    if (!existing || scoreTeacher(t) > scoreTeacher(existing)) {
      byEmail.set(key, t);
    }
  }
  teachers = [...byEmail.values(), ...emailless];

  // step f.3: strip directory cross-reference annotations from role text.
  // directories sometimes include inline notes like "(see Britton, Kathryn
  // above)" or "(aka Sanchez)" alongside the actual role. these are metadata,
  // not job titles, and shouldn't ship in the csv.
  teachers = teachers.map((t) => {
    if (!t.role) return t;
    const cleaned = t.role
      .replace(/\s*\((?:see|see also|aka|a\.?k\.?a\.?|formerly|née|nee|same as|previously)[^)]*\)/gi, "")
      .replace(/\s{2,}/g, " ")
      .trim();
    return cleaned === t.role ? t : { ...t, role: cleaned };
  });

  // step g: confidence scoring disabled — just strip internal metadata
  teachers = teachers.map((t) => {
    const { _emailDomainMatch, ...clean } = t as Teacher & { _emailDomainMatch?: boolean };
    return clean;
  });
  // keep original order (no sorting/classification)

  return teachers;
}

// ── internal helpers ──

/** quick confidence proxy for dedup — prefer records with more data */
function scoreTeacher(t: Teacher): number {
  let s = 0;
  if (t.email) s += 3;
  if (t.role) s += 2;
  if (t.department) s += 1;
  // school assignment and phone extension removed from scoring
  return s;
}

// ── hacker score (disabled for classification; function retained but unused) ─
// affinity for Hack Club's project-based CS / maker / hacker ethos. scored 1-5
// off the teacher's role + department keywords. tiers are ordered from highest
// to lowest so an ambiguous teacher ("Physics & CS Teacher") gets the top tier
// that matches.

const HACKER_TIERS: { score: HackerScore; patterns: RegExp[] }[] = [
  // tier 5: dedicated hacker-adjacent teaching — CS, software, security, hackathons
  {
    score: 5,
    patterns: [
      /\bcomputer\s+science\b/i,
      /\bcomp\s*sci\b/i,
      /\bAP\s+CS\b/i,
      /\bcoding\b/i,
      /\bprogramming\b/i,
      /\bsoftware\b/i,
      /\bweb\s+(design|development|dev)\b/i,
      /\bapp\s+development\b/i,
      /\bgame\s+(design|development|dev)\b/i,
      /\bcyber\s*security\b/i,
      /\bdata\s+science\b/i,
      /\bmachine\s+learning\b/i,
      /\bhackathon\b/i,
      /\bhack\s*club\b/i,
      /\bCS\b/, // case-sensitive on purpose: "CS" acronym, not "cs" in "classroom"
    ],
  },
  // tier 4: engineering, robotics, maker, digital/tech — build-things domains
  {
    score: 4,
    patterns: [
      /\bengineering\b/i,
      /\brobotics\b/i,
      /\bmaker\s*(space|lab)?\b/i,
      /\b3d\s+printing\b/i,
      /\belectronics\b/i,
      /\bdigital\s+(electronics|design|fabrication|learning|media|technology)\b/i,
      /\bdesign\s+technology\b/i,
      /\btech(?:nology)?\s+(integration|integrator|coordinator|ed(?:ucation)?)\b/i,
      /\binformation\s+technology\b/i,
      /\bIT\s+(teacher|instructor|coordinator)\b/,
      /\bSTEM\b/,
      /\bpre-?engineering\b/i,
      /\bprinciples\s+of\s+engineering\b/i,
      /\bproject\s+lead\s+the\s+way\b/i,
      /\bPLTW\b/,
    ],
  },
  // tier 3: tinker-adjacent — physics, applied science, CAD
  {
    score: 3,
    patterns: [
      /\bphysics\b/i,
      /\bCAD\b/,
      /\bdrafting\b/i,
      /\bwoodworking\b/i,
      /\bshop\b/i,
      /\bapplied\s+science\b/i,
      /\bforensic\s+science\b/i,
      /\bastronomy\b/i,
    ],
  },
  // tier 2: life + earth sciences — STEM but lab-coat, not hacker
  {
    score: 2,
    patterns: [
      /\bchemistry\b/i,
      /\bbiology\b/i,
      /\bbio\b/i,
      /\benvironmental\s+science\b/i,
      /\bearth\s+science\b/i,
      /\banatomy\b/i,
      /\bphysiology\b/i,
      /\bgeology\b/i,
      /\bmarine\s+biology\b/i,
      /\blife\s+science\b/i,
      /\bphysical\s+science\b/i,
      /\bgeneral\s+science\b/i,
      /\bscience\b/i,
    ],
  },
];

/**
 * compute a 1-5 Hack Club affinity score from a teacher's role + department.
 * CS/coding/software = 5, engineering/robotics/maker = 4, physics/applied = 3,
 * chem/bio/env = 2, math and everything else = 1.
 */
export function computeHackerScore(
  role: string,
  department: string | null | undefined,
): HackerScore {
  const text = [role, department].filter(Boolean).join(" ");
  if (!text.trim()) return 1;

  for (const tier of HACKER_TIERS) {
    if (tier.patterns.some((p) => p.test(text))) return tier.score;
  }
  // default tier 1: math and anything else that passed the STEM filter but
  // doesn't match a more hacker-flavored keyword.
  return 1;
}

/** roles that mention stem-adjacent terms but aren't clearly a stem teaching role */
function isAmbiguousRole(role: string): boolean {
  const lower = role.toLowerCase();
  const ambiguous = [
    /\btechnology\s+coordinator\b/,
    /\btechnology\s+director\b/,
    /\bstem\s+coordinator\b/,
    /\bdepartment\s+(head|chair)\b/,
    /\bassistant\b/,
    /\baide\b/,
    /\bsubstitute\b/,
    /\btutor\b/,
  ];
  return ambiguous.some((p) => p.test(lower));
}
