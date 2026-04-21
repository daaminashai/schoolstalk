// ── shared utility functions for schoolyank ──

const PREFIXES = /^(mr\.|mrs\.|ms\.|dr\.|prof\.)\s+/i;
const SUFFIXES = /,?\s+(jr\.|sr\.|iii|iv|ph\.d\.|m\.ed\.|ed\.d\.)$/i;

function titleCase(s: string): string {
  if (!s) return s;

  // short all-caps tokens (≤3 chars) are almost always acronym initials —
  // "JP", "JD", "DJ", "TJ", "AJ" etc. — and should be preserved as-is.
  // without this special case the all-caps branch below lowercases them
  // into "Jp", "Jd" etc.
  if (/^[A-Z]{2,3}$/.test(s)) return s;

  // if the input already looks mixed-case (has both upper and lower letters
  // AND isn't ALL CAPS), trust it — preserves "MacFadyen", "O'Brien",
  // "de la Cruz", "St. Martin" as written on the site.
  const hasLower = /[a-z]/.test(s);
  const hasUpper = /[A-Z]/.test(s);
  if (hasLower && hasUpper) return s;

  // otherwise titlecase and re-capitalize after common name prefixes that
  // naive capitalize-after-whitespace misses: "Mc", "Mac" (when followed by
  // a capital-looking stem), "O'", "D'", "St.".
  const cased = s
    .toLowerCase()
    .replace(/(?:^|\s|-)(\w)/g, (match, c: string) => match.slice(0, -1) + c.toUpperCase());
  return cased
    .replace(/\b(Mc|Mac)([a-z])/g, (_, prefix: string, c: string) => `${prefix}${c.toUpperCase()}`)
    .replace(/\b([OD])'([a-z])/g, (_, prefix: string, c: string) => `${prefix}'${c.toUpperCase()}`);
}

export function parseName(fullName: string): { firstName: string; lastName: string } {
  let name = fullName.trim();

  // strip alumni class years ANYWHERE in the string before other parsing.
  // lawrenceville.org and peer prep schools often write "Daniel Concepcion '02"
  // (or the curly-quote variant); without this the year ends up kept as the
  // last name, or flipped into the first name if a trailing comma triggers
  // the "Last, First" branch.
  name = name.replace(/\s+['\u2019]\d{2}\b/g, "");

  // strip parenthesized segments (maiden names, alternate spellings, nicknames)
  name = name.replace(/\s*\([^)]*\)\s*/g, " ").trim();

  // strip prefixes and suffixes
  name = name.replace(PREFIXES, "");
  while (SUFFIXES.test(name)) name = name.replace(SUFFIXES, "");
  name = name.trim();

  // "Last, First" format
  if (name.includes(",")) {
    const [last, ...rest] = name.split(",").map((s) => s.trim());
    const first = rest.join(" ").trim();
    return { firstName: titleCase(first || ""), lastName: titleCase(last ?? "") };
  }

  // "First Last" or "First Middle Last"
  const parts = name.split(/\s+/);
  if (parts.length === 1) return { firstName: titleCase(parts[0]!), lastName: "" };

  const firstName = parts[0]!;
  const lastName = parts[parts.length - 1]!;
  return { firstName: titleCase(firstName), lastName: titleCase(lastName) };
}

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

export function normalizeEmail(email: string): string | null {
  const cleaned = email.toLowerCase().trim();
  return EMAIL_RE.test(cleaned) ? cleaned : null;
}

export function slugify(text: string): string {
  return text
    .toLowerCase()
    .trim()
    .replace(/[^\w\s-]/g, "")
    .replace(/[\s_]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

export function fuzzyMatch(a: string, b: string): number {
  const wordsA = new Set(a.toLowerCase().trim().split(/\s+/).filter(Boolean));
  const wordsB = new Set(b.toLowerCase().trim().split(/\s+/).filter(Boolean));

  if (wordsA.size === 0 && wordsB.size === 0) return 1;
  if (wordsA.size === 0 || wordsB.size === 0) return 0;

  let intersection = 0;
  for (const w of wordsA) if (wordsB.has(w)) intersection++;

  const union = new Set([...wordsA, ...wordsB]).size;
  return intersection / union;
}

export function extractDomain(url: string): string {
  try {
    const hostname = new URL(url).hostname;
    return hostname.replace(/^www\./, "");
  } catch {
    return url;
  }
}

export function sleep(ms: number): Promise<void> {
  return Bun.sleep(ms);
}
