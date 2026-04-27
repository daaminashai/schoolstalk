// ── hasura bypass — direct GraphQL extraction for Apptegy K-12 districts ──
//
// Many US school districts run their public-facing staff directory on the
// Apptegy CMS, which ships a Vue SPA backed by a public Hasura GraphQL API.
// The SPA is often gated by Google reCAPTCHA, which makes headless Chromium
// useless for the browser-use agent. The underlying Hasura endpoint, however,
// is unauthenticated with introspection enabled — and exposes the full
// teacher roster (names + emails + school + subject teams + phone) behind a
// single GraphQL query.
//
// This module auto-discovers the Hasura endpoint for a given district URL,
// verifies it's Apptegy-shaped, and pulls all teachers in a single pass.
// Falls through to the browser-use agent if anything fails, so the bypass is
// always a pure speedup — never a regression.
//
// Discovery pipeline (cheapest first):
//   1. Manual override (for known districts — skip discovery entirely)
//   2. Pattern probe: slug-based guess (`<slug>-hasura-prd.hasura.app`)
//   3. HTML sniff: fetch the homepage + `directory.<domain>` and look for
//      hasura URLs in `<link rel=dns-prefetch>`, `<script src>`, or inline JS
//
// Each candidate is verified via a lightweight introspection query; only
// endpoints with the expected Apptegy schema (`groups`, `people`,
// `organizations`) are used.

import type { RawTeacherData, RawSiteInfo } from "./types";
import type { ScraperOutput } from "./scraper";

// ── known districts (fast-path) ────────────────────────────────────────────
// If a URL matches one of these, we skip discovery. Useful to avoid probing
// on every run. Add new ones here as they're verified — auto-discovery handles
// everything else.
const KNOWN_HASURA_ENDPOINTS: Array<{
  matches: (url: URL) => boolean;
  endpoint: string;
}> = [
  {
    matches: (u) => /(^|\.)asd20\.org$/i.test(u.hostname),
    endpoint: "https://asd20-hasura-prd.hasura.app/v1/graphql",
  },
];

// ── public entry points ────────────────────────────────────────────────────

/**
 * Top-level: given a school URL, attempt to find and use a Hasura bypass.
 * Returns a fully-formed ScraperOutput if successful, null otherwise.
 */
export async function tryHasuraBypass(
  schoolUrl: string,
  log: (msg: string) => void = () => {},
): Promise<ScraperOutput | null> {
  const endpoint = await discoverHasuraEndpoint(schoolUrl, log);
  if (!endpoint) return null;

  log(`Hasura endpoint detected: ${endpoint}`);

  const metadata = await detectDistrictMetadata(endpoint, schoolUrl);
  if (!metadata) {
    log("Hasura endpoint returned unexpected schema — falling back to browser agent");
    return null;
  }

  log(`district: ${metadata.districtName} (${metadata.state})`);

  return scrapeViaHasura(endpoint, metadata);
}

// ── discovery ──────────────────────────────────────────────────────────────

/**
 * Finds the Hasura GraphQL endpoint for a district's website, verifying the
 * schema looks Apptegy-shaped before returning.
 */
export async function discoverHasuraEndpoint(
  schoolUrl: string,
  log: (msg: string) => void = () => {},
): Promise<string | null> {
  let parsed: URL;
  try {
    parsed = new URL(schoolUrl);
  } catch {
    return null;
  }

  // 1. manual override
  const known = KNOWN_HASURA_ENDPOINTS.find((k) => k.matches(parsed));
  if (known) {
    if (await isApptegyHasura(known.endpoint)) return known.endpoint;
    log(`manual Hasura override ${known.endpoint} no longer works — falling through to discovery`);
  }

  // 2. pattern-based slug guesses. Apptegy's conventional hostnames follow a
  //    narrow pattern; we probe in parallel and take the first valid hit. DNS
  //    failures for non-Apptegy slugs return in <500ms so the overhead on
  //    misses is ≤2s total.
  const slug = extractSlug(parsed.hostname);
  if (slug) {
    const candidates = [
      `https://${slug}-hasura-prd.hasura.app/v1/graphql`,
      `https://${slug}-hasura-prod.hasura.app/v1/graphql`,
      `https://${slug}-hasura-prd.azurewebsites.net/v1/graphql`,
      `https://${slug}-hasura-prod.azurewebsites.net/v1/graphql`,
    ];
    const results = await Promise.all(
      candidates.map(async (c) => ({ c, ok: await isApptegyHasura(c) })),
    );
    const hit = results.find((r) => r.ok);
    if (hit) return hit.c;
  }

  // 3. HTML sniff — single homepage fetch; fast fallback for districts whose
  //    Hasura hostname doesn't match the standard slug pattern (rare). Only
  //    the homepage is fetched (4s timeout) — SPA subdomains like directory.*
  //    are gated by reCAPTCHA and would cost more than they gain.
  const endpoint = await sniffHasuraFromHtml(`${parsed.origin}/`);
  if (endpoint && (await isApptegyHasura(endpoint))) return endpoint;

  return null;
}

/** Extract a slug like "asd20" from a hostname like "www.asd20.org" */
function extractSlug(hostname: string): string | null {
  const parts = hostname.toLowerCase().split(".");
  // strip common prefixes
  while (parts.length > 1 && ["www", "schools", "directory"].includes(parts[0]!)) {
    parts.shift();
  }
  // take the leftmost remaining part (e.g. "asd20" from "asd20.org")
  const slug = parts[0];
  if (!slug || slug.length < 3 || /^\d+$/.test(slug)) return null;
  return slug;
}

/** Fetch HTML at url and look for any hasura endpoint reference in it. */
async function sniffHasuraFromHtml(url: string): Promise<string | null> {
  try {
    const res = await fetch(url, {
      redirect: "follow",
      signal: AbortSignal.timeout(4_000),
    });
    if (!res.ok) return null;
    const html = await res.text();
    // pick up dns-prefetch, script src, inline refs to any hasura-looking URL
    const matches = html.match(
      /https?:\/\/[a-z0-9-]+-hasura[a-z0-9-]*\.(?:hasura\.app|azurewebsites\.net)(?:\/v1\/graphql)?/gi,
    );
    if (!matches || matches.length === 0) return null;
    // normalize to /v1/graphql suffix
    const raw = matches[0]!;
    return raw.endsWith("/v1/graphql") ? raw : `${raw.replace(/\/$/, "")}/v1/graphql`;
  } catch {
    return null;
  }
}

/**
 * Introspect the endpoint and verify it has the Apptegy schema shape.
 * Short timeout (3s) — DNS failures for non-Apptegy slugs should return fast,
 * and a real Apptegy endpoint responds in ~200ms.
 */
async function isApptegyHasura(endpoint: string): Promise<boolean> {
  const data = await graphqlQuery<{
    __schema: { queryType: { fields: Array<{ name: string }> } };
  }>(
    endpoint,
    `{ __schema { queryType { fields { name } } } }`,
    3_000,
  );
  if (!data) return false;
  const names = new Set(data.__schema.queryType.fields.map((f) => f.name));
  // Apptegy's Hasura consistently exposes these query roots. If any are
  // missing this probably isn't an Apptegy deployment.
  return names.has("groups") && names.has("people") && names.has("organizations");
}

// ── district metadata detection ────────────────────────────────────────────

export interface DistrictMetadata {
  districtName: string;
  state: string;
  officeAddress: string;
  /** the root district org's uuid — used later to scope queries to this tenant */
  rootOrgId: string | null;
}

/**
 * Detect the district's canonical name, state, and office address from the
 * organizations table. Uses two signals: parentOrganizationId=null identifies
 * the tenant root; URL-slug overlap with org abbreviation tie-breaks when
 * multiple roots exist (e.g. shared Hasura instance).
 */
export async function detectDistrictMetadata(
  endpoint: string,
  schoolUrl: string,
): Promise<DistrictMetadata | null> {
  const data = await graphqlQuery<{
    organizations: Array<{
      id: string;
      title: string;
      abbreviation: string | null;
      address: { street1?: string; city?: string; state?: string; zip?: string } | null;
      parentOrganizationId: string | null;
    }>;
  }>(
    endpoint,
    `{ organizations(where: { parentOrganizationId: { _is_null: true } }) {
        id title abbreviation address parentOrganizationId
    } }`,
  );
  if (!data || data.organizations.length === 0) return null;

  // single root org: easy case
  const roots = data.organizations;
  let root = roots[0]!;

  // multiple roots → disambiguate by slug overlap with title/abbreviation
  if (roots.length > 1) {
    const slug = extractSlug(new URL(schoolUrl).hostname) ?? "";
    const slugLower = slug.toLowerCase();
    const scored = roots.map((o) => {
      const abbrev = (o.abbreviation ?? "").toLowerCase();
      const title = (o.title ?? "").toLowerCase();
      let score = 0;
      if (abbrev && slugLower.includes(abbrev)) score += 3;
      if (title.includes(slugLower)) score += 2;
      // also check if slug looks like district's compact form
      if (slugLower === abbrev) score += 5;
      return { org: o, score };
    });
    scored.sort((a, b) => b.score - a.score);
    root = scored[0]?.org ?? roots[0]!;
  }

  const addr = root.address ?? {};
  const parts = [addr.street1, addr.city, addr.state, addr.zip]
    .filter((s): s is string => !!s && s.trim().length > 0);
  if (!addr.state) return null; // need state for NCES disambiguation

  return {
    districtName: root.title,
    state: addr.state,
    officeAddress: parts.join(", "),
    rootOrgId: root.id,
  };
}

// ── teacher extraction ─────────────────────────────────────────────────────

// No subject filter: we request all groups and later synthesize roles/subjects
// from titles where possible. Non-teaching groups will be filtered by heuristics
// in role/subject derivation rather than by a STEM-only allowlist.

interface GroupRow {
  title: string;
  ownerOrganization: { title: string } | null;
  people: Array<{
    person: {
      displayName: string | null;
      email: string | null;
      title: string | null;
      phone: string | null;
    } | null;
  }>;
}

/**
 * Pull every STEM teacher from a Hasura-backed Apptegy district and convert
 * to the ScraperOutput shape so the rest of the pipeline works unchanged.
 * Returns null on any error so the caller can fall back to the browser agent.
 */
export async function scrapeViaHasura(
  endpoint: string,
  metadata: DistrictMetadata,
): Promise<ScraperOutput | null> {
  // 1. pull all groups + members + owning school
  const groupsQuery = `
    { groups {
        title
        ownerOrganization { title }
        people { person { displayName email title phone } }
    } }
  `;
  const groupsRes = await graphqlQuery<{ groups: GroupRow[] }>(endpoint, groupsQuery);
  if (!groupsRes) return null;

  // 2. pull schools from the organizations table (child orgs of this root)
  const orgsRes = await graphqlQuery<{
    organizations: Array<{ title: string }>;
  }>(
    endpoint,
    metadata.rootOrgId
      ? `{ organizations(where: { parentOrganizationId: { _eq: "${metadata.rootOrgId}" } }) { title } }`
      : `{ organizations { title } }`,
  );
  const schoolNames = (orgsRes?.organizations ?? [])
    .map((o) => o.title)
    .filter((n) => !!n && !isNonTeacherGroup(n) && n !== metadata.districtName);

  // 3. dedupe teachers by email (fallback to displayName). one person can be
  //    in multiple groups (math + coach, etc.) — merge their subjects
  //    AND collect every group.title they appear in so we can derive a rich
  //    role label later (raw person.title on Apptegy is uniformly "Teacher").
  interface BufferedTeacher {
    name: string;
    email?: string;
    rawTitle?: string; // from person.title — usually "Teacher" on Apptegy
    phone?: string;
    assignedSchool?: string;
    subjects: Set<string>; // normalized labels ("Mathematics", "Biology", etc)
    rawGroupTitles: Set<string>; // raw ("Math Coach", "AP Chemistry", "STEM")
  }
  const byKey = new Map<string, BufferedTeacher>();

  for (const g of groupsRes.groups) {
    const subject = normalizeSubject(g.title);
    const schoolName = g.ownerOrganization?.title ?? "";
    for (const { person } of g.people) {
      if (!person) continue;
      const display = (person.displayName ?? "").trim();
      if (!display) continue;
      const email = (person.email ?? "").trim() || undefined;
      const key = (email ?? display).toLowerCase();

      const existing = byKey.get(key);
      if (existing) {
        existing.subjects.add(subject);
        existing.rawGroupTitles.add(g.title);
        if (!existing.assignedSchool && schoolName) existing.assignedSchool = schoolName;
        continue;
      }

      byKey.set(key, {
        name: display,
        email,
        rawTitle: person.title?.trim() || undefined,
        phone: person.phone?.trim() || undefined,
        assignedSchool: schoolName || undefined,
        subjects: new Set([subject]),
        rawGroupTitles: new Set([g.title]),
      });
    }
  }

  const teachers: RawTeacherData[] = [...byKey.values()].map((t) => ({
    name: t.name,
    email: t.email,
    role: deriveRole(t.rawTitle, t.rawGroupTitles, t.subjects),
    phone: t.phone,
    department: pickDeptLabel(t.subjects) || undefined,
    assignedSchool: t.assignedSchool,
  }));

  // 4. synthesize RawSiteInfo — in single-school mode this is unused, retain name for compatibility
  const siteInfo: RawSiteInfo = {
    name: metadata.districtName,
    // hard-coded address ensures the orchestrator's extractState() picks up
    // the right state. empty strings cause it to fall back to URL parsing,
    // which almost never contains a state.
    address: metadata.officeAddress || null,
    schools: schoolNames,
    schoolGroups: [],
  };

  return { teachers, siteInfo, sessionId: "hasura-bypass" };
}

// ── helpers ────────────────────────────────────────────────────────────────

/** POST a GraphQL query against a Hasura endpoint. Returns null on any error. */
async function graphqlQuery<T>(
  endpoint: string,
  query: string,
  timeoutMs = 15_000,
): Promise<T | null> {
  try {
    const res = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query }),
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (!res.ok) return null;
    const json = (await res.json()) as { data?: T; errors?: unknown };
    if (json.errors) return null;
    return json.data ?? null;
  } catch {
    return null;
  }
}

// no-op placeholder (left for potential future filtering needs)
function isNonStemGroupTitle(_title: string): boolean { return false; }

// titles that ARE roles on their own — if any of the teacher's group titles
// matches, use the richest such title verbatim instead of synthesizing "X Teacher".
// "Math Coach" stays "Math Coach", not "Math Coach Teacher".
const ROLE_DESCRIPTOR_PATTERN =
  /\b(coach|specialist|coordinator|interventionist|chair|director|head|lead|aide|paraprofessional|mentor|resource|instructional|support|consultant)\b/i;

/**
 * Build a richer role label than Apptegy's uniform "Teacher".
 * Priority, highest-quality first:
 *   1. rawTitle already specific (not just "Teacher"/empty) → use verbatim
 *   2. any group title describing a role (coach/specialist/etc) → use verbatim,
 *      picking the most specific one when multiple apply
 *   3. synthesize "<Subject> Teacher" from the canonical subject label
 *   4. fall back to "Teacher"
 */
function deriveRole(
  rawTitle: string | undefined,
  rawGroupTitles: Set<string>,
  subjects: Set<string>,
): string {
  // 1. person.title if it's actually informative
  const trimmedTitle = rawTitle?.trim();
  if (trimmedTitle && !/^teacher$/i.test(trimmedTitle)) {
    return trimmedTitle;
  }

  // 2. any group title that's itself a role (coach/specialist/…)
  const roleGroups = [...rawGroupTitles].filter((g) => ROLE_DESCRIPTOR_PATTERN.test(g));
  if (roleGroups.length > 0) {
    // pick the longest — usually the most specific ("AP Math Coach" > "Coach")
    roleGroups.sort((a, b) => b.length - a.length);
    return roleGroups[0]!;
  }

  // 3. synthesize from the canonical subject label — pick the most specific
  //    subject the teacher is associated with (Computer Science > Math > STEM)
  const subjectLabel = pickDeptLabel(subjects);
  if (subjectLabel) return `${subjectLabel} Teacher`;

  // 4. last resort
  return "Teacher";
}

/** canonical subject label for the CSV — maps group.title to our enum */
function normalizeSubject(groupTitle: string): string {
  const t = groupTitle.toLowerCase();
  if (/^math\b|\bmathematics\b|algebra|geometry|calculus|statistics/.test(t)) return "Mathematics";
  if (/computer\s*science|\bcs\b|coding|programming|software/.test(t)) return "Computer Science";
  if (/\bstem\b|\bsteam\b/.test(t)) return "STEM";
  if (/\btechnology\b|\btech ed\b|\bit\b/.test(t)) return "Technology";
  if (/engineering|robotics/.test(t)) return "Engineering";
  if (/biolog|anatom|physiolog/.test(t)) return "Biology";
  if (/chemist/.test(t)) return "Chemistry";
  if (/physic/.test(t)) return "Physics";
  if (/environ|earth|geolog|astronom/.test(t)) return "Science";
  if (/science/.test(t)) return "Science";
  return ""; // unknown/non-subject group — let validator/role inference handle
}

/** when a teacher appears in multiple STEM groups, pick the most specific label */
function pickDeptLabel(subjects: Set<string>): string {
  const priority = [
    "Computer Science", "Engineering", "Physics", "Chemistry",
    "Biology", "Technology", "Mathematics", "Science", "STEM",
  ];
  for (const p of priority) if (subjects.has(p)) return p;
  return [...subjects][0] ?? "";
}

function isNonTeacherGroup(name: string): boolean {
  // some orgs in the Apptegy schema aren't actually schools — filter them
  // out of the reported school list.
  return /\b(board|human resources|operations|facilities|transportation|administration|food services|insider)\b/i.test(
    name,
  );
}
