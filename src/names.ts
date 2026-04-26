// ── names.ts — canonicalize NCES-supplied names for display ──
//
// NCES ships school/district names in inconsistent case (mostly all caps, some
// sentence case) and with trailing verbose descriptors (e.g. "in the county of
// El Paso and..."). we normalize before they hit the CSV so downstream judges
// and output look clean.

/**
 * title-cases an all-caps or messy string, trims descriptive tails, and
 * collapses whitespace. safe no-op for already-clean names.
 */
export function normalizeDistrictName(raw: string | null | undefined): string {
  if (!raw) return "";
  let s = raw.trim();

  // chop verbose tails — NCES occasionally appends "in the county of X..." to
  // lea_name. anything after these phrases is descriptive, not part of the
  // official name.
  s = s.replace(/\s+in\s+the\s+county\s+of\b.*$/i, "");
  s = s.replace(/\s+in\s+county\b.*$/i, "");
  s = s.replace(/\s+,\s*in\b.*$/i, "");

  s = s.replace(/\s+/g, " ").trim();

  // title case if the string is all-uppercase (heuristic: no lowercase letters).
  if (!/[a-z]/.test(s)) s = toTitleCase(s);
  s = expandSchoolAbbreviations(s);
  return s;
}

/** same treatment for school names (no county-trimming — NCES doesn't append that here). */
export function normalizeSchoolName(raw: string | null | undefined): string {
  if (!raw) return "";
  let s = raw.trim().replace(/\s+/g, " ");
  // NCES ships a trailing " (the)" on some names (e.g. "Bronx High School of
  // Science (the)") — a machine-readable hint that "The" is the canonical
  // article. drop the parenthetical and prepend "The " for display.
  const theMatch = s.match(/^(.*)\s*\(the\)\s*$/i);
  if (theMatch?.[1]) s = `The ${theMatch[1].trim()}`;
  // titlecase all-caps first so that when we expand abbreviations afterward,
  // the output is uniformly title-cased (otherwise "SHAKER HTS" + expansion
  // leaves "SHAKER Heights" — mixed).
  if (!/[a-z]/.test(s)) s = toTitleCase(s);
  s = expandSchoolAbbreviations(s);
  return s;
}

// expand NCES abbreviations so "Shaker Hts High School" displays as "Shaker
// Heights High School" in the CSV. matching is case-insensitive; the replace
// preserves original casing context (downstream title-casing handles the rest).
const SCHOOL_ABBREV: Array<[RegExp, string]> = [
  [/\bHts\b/gi, "Heights"],
  [/\bMt\b/gi, "Mount"],
  [/\bSt\.?\b/gi, "Saint"],
  [/\bFt\b/gi, "Fort"],
  [/\bSr\b/gi, "Senior"],
  [/\bJr\b/gi, "Junior"],
  [/\bElem\b/gi, "Elementary"],
  [/\bCtr\b/gi, "Center"],
  [/\bIntermed\b/gi, "Intermediate"],
  [/\bSch\b/gi, "School"],
  [/\bH S\b/gi, "High School"],
  [/\bJr\/Sr\b/gi, "Junior/Senior"],
  [/\bTwp\b/gi, "Township"],
  // district acronyms stay uppercase — "Austin Isd" reads as a typo to anyone
  // familiar with TX districts ("AISD"/"ISD"); same for CA ("USD") and
  // Washington ("HSD", "PSD", "SD"). applied AFTER toTitleCase so we re-upper
  // what the titlecaser lowered.
  [/\bIsd\b/g, "ISD"],
  [/\bUsd\b/g, "USD"],
  [/\bHsd\b/g, "HSD"],
  [/\bPsd\b/g, "PSD"],
];

function expandSchoolAbbreviations(s: string): string {
  let out = s;
  for (const [re, repl] of SCHOOL_ABBREV) out = out.replace(re, repl);
  return out;
}

/**
 * canonicalize department labels so the CSV doesn't ship "Math" in one row and
 * "Mathematics" in the next. keeps a stable enumerated set so a judge can
 * group / filter reliably. null/empty passes through unchanged.
 */
export function canonicalizeDepartment(raw: string | null | undefined): string | null {
  if (!raw) return null;
  const s = raw.trim();
  if (!s) return null;
  const lower = s.toLowerCase().replace(/[^a-z0-9/&+ -]/g, "").trim();

  // exact-alias table first — most hits land here.
  const aliases: Record<string, string> = {
    "math": "Mathematics",
    "maths": "Mathematics",
    "mathematics": "Mathematics",
    "math dept": "Mathematics",
    "math department": "Mathematics",

    "science": "Science",
    "sciences": "Science",
    "general science": "Science",
    "natural science": "Science",
    "natural sciences": "Science",

    "bio": "Biology",
    "biology": "Biology",
    "ap bio": "Biology",
    "ap biology": "Biology",

    "chem": "Chemistry",
    "chemistry": "Chemistry",
    "ap chem": "Chemistry",
    "ap chemistry": "Chemistry",

    "physics": "Physics",
    "ap physics": "Physics",

    "earth science": "Earth Science",
    "environmental science": "Environmental Science",
    "apes": "Environmental Science",

    "cs": "Computer Science",
    "comp sci": "Computer Science",
    "computer science": "Computer Science",
    "ap cs": "Computer Science",
    "ap computer science": "Computer Science",
    "coding": "Computer Science",
    "programming": "Computer Science",
    "software": "Computer Science",

    "engineering": "Engineering",
    "engineering & design": "Engineering",
    "pre-engineering": "Engineering",

    "technology": "Technology",
    "tech": "Technology",
    "tech ed": "Technology",
    "technology education": "Technology",
    "it": "Technology",
    "information technology": "Technology",
    "digital": "Technology",
    "digital learning": "Technology",

    "stem": "STEM",
    "steam": "STEAM",

    "robotics": "Robotics",
    "maker": "Maker",
    "makerspace": "Maker",
    "maker space": "Maker",
  };

  if (aliases[lower]) return aliases[lower]!;

  // fallback heuristics for the long tail (e.g. "AP Computer Science A",
  // "Mathematics & Statistics") — canonicalize by dominant keyword.
  if (/\b(comput|coding|programming|software)/.test(lower)) return "Computer Science";
  if (/\b(engineering)/.test(lower)) return "Engineering";
  if (/\b(robotic)/.test(lower)) return "Robotics";
  if (/\b(steam)\b/.test(lower)) return "STEAM";
  if (/\b(stem)\b/.test(lower)) return "STEM";
  if (/\b(physic)/.test(lower)) return "Physics";
  if (/\b(chem)/.test(lower)) return "Chemistry";
  if (/\b(biolog|anatomy|physiology|life science)/.test(lower)) return "Biology";
  if (/\b(environ)/.test(lower)) return "Environmental Science";
  if (/\b(earth science|geolog|astronomy)/.test(lower)) return "Earth Science";
  if (/\b(tech(?:nology|nical|nician)?|digital|it)\b/.test(lower)) return "Technology";
  if (/\b(math|algebra|geometry|calculus|precalc|trigonometry|trig|statistics|probability)/.test(lower)) return "Mathematics";
  if (/\b(science)\b/.test(lower)) return "Science";

  // nothing matched — return titlecased original so we at least don't ship
  // "MATHEMATICS" next to "Mathematics".
  return toTitleCase(s);
}

const SMALL_WORDS = new Set([
  "a", "an", "and", "as", "at", "but", "by", "for", "in", "nor",
  "of", "on", "or", "per", "the", "to", "vs", "via",
]);

const ACRONYM_TOKENS = new Set([
  "HS", "MS", "ES", "ELA", "IT", "CS", "PE", "AP", "IB",
  "STEM", "STEAM", "ESL", "ELL", "ROTC", "JROTC", "CTE",
  "US", "USA", "UK", "EU", "AI", "ML",
]);

function toTitleCase(s: string): string {
  const words = s.split(/(\s+|-|\/)/);
  return words
    .map((w, i) => {
      if (!w.trim()) return w;
      if (w === "-" || w === "/") return w;
      if (ACRONYM_TOKENS.has(w)) return w;
      const upper = w.toUpperCase();
      if (ACRONYM_TOKENS.has(upper)) return upper;
      const lower = w.toLowerCase();
      // preserve numbers and ordinal markers
      if (/^\d/.test(lower)) return lower;
      // short connectors stay lowercase except first/last token
      const isEdge = i === 0 || i === words.length - 1;
      if (!isEdge && SMALL_WORDS.has(lower)) return lower;
      return lower.charAt(0).toUpperCase() + lower.slice(1);
    })
    .join("");
}
