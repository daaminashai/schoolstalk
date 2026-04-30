// -- Thrillshare/Apptegy staff-directory bypass --
//
// Apptegy sites expose their public staff directory through Thrillshare JSON
// endpoints embedded in the page boot state. Reading those endpoints directly
// avoids launching Chromium/browser-use for a common K-12 CMS family.

import type { ScraperOutput } from "./scraper";
import type { RawTeacherData, RawSiteInfo } from "./types";

const DISCOVERY_TIMEOUT_MS = 4_000;
const API_TIMEOUT_MS = 8_000;
const MAX_DIRECTORY_PAGES = 25;

interface DirectoryRow {
  full_name?: string | null;
  first?: string | null;
  last?: string | null;
  title?: string | null;
  department?: string | null;
  email?: string | null;
}

interface DirectoryResponse {
  directories?: DirectoryRow[];
  data?: DirectoryRow[];
  meta?: {
    links?: {
      next?: string | null;
      last?: string | null;
    };
  };
}

export async function tryThrillshareBypass(
  schoolUrl: string,
  candidateUrls: string[] = [],
  log: (msg: string) => void = () => {},
): Promise<ScraperOutput | null> {
  const pageUrls = candidatePageUrls(schoolUrl, candidateUrls);
  const endpoints = new Set<string>();

  await Promise.all(
    pageUrls.map(async (url) => {
      if (isThrillshareDirectoryApi(url)) endpoints.add(url);

      const html = await fetchText(url);
      if (!html) return;
      for (const endpoint of extractDirectoryEndpoints(html))
        endpoints.add(endpoint);
    }),
  );

  const teachers = dedupeTeachers(
    (
      await Promise.all(
        [...endpoints].map((endpoint) => scrapeDirectoryEndpoint(endpoint)),
      )
    ).flat(),
  );
  if (teachers.length > 0) {
    log(
      `Thrillshare/Apptegy staff API matched ${teachers.length} teacher-like records across ${endpoints.size} endpoint${endpoints.size === 1 ? "" : "s"}`,
    );
    const siteInfo: RawSiteInfo = { name: null };
    return { teachers, siteInfo, sessionId: "thrillshare-bypass" };
  }

  return null;
}

function candidatePageUrls(
  schoolUrl: string,
  candidateUrls: string[],
): string[] {
  let origin = "";
  try {
    origin = new URL(schoolUrl).origin;
  } catch {}

  const urls = [
    ...candidateUrls.filter(
      (url) => isThrillshareDirectoryApi(url) || scoreLikelyStaffUrl(url) > 0,
    ),
    schoolUrl,
  ]
    .map((url) => normalizeUrl(url, origin))
    .filter((url): url is string => !!url);

  return dedupe(urls)
    .sort((a, b) => scoreLikelyStaffUrl(b) - scoreLikelyStaffUrl(a))
    .slice(0, 5);
}

function normalizeUrl(url: string, origin: string): string | null {
  try {
    return new URL(url, origin || undefined).href;
  } catch {
    return null;
  }
}

function scoreLikelyStaffUrl(url: string): number {
  if (isThrillshareDirectoryApi(url)) return 100;
  if (/\b(staff|faculty|teacher|directory|people)\b/i.test(url)) return 10;
  return 0;
}

function isThrillshareDirectoryApi(url: string): boolean {
  return /thrillshare-cmsv2\.services\.thrillshare\.com\/api\/v\d+\/.*\/directories/i.test(
    url,
  );
}

function extractDirectoryEndpoints(html: string): string[] {
  const endpoints = new Set<string>();
  const state = parseClientWorkState(html);
  collectStaffLinks(state, endpoints);

  const normalized = html.replace(/\\\//g, "/").replace(/&amp;/g, "&");
  const matches = normalized.matchAll(
    /https:\/\/thrillshare-cmsv2\.services\.thrillshare\.com\/api\/v\d+\/[^"'\\\s<>]+\/directories[^"'\\\s<>]*/gi,
  );
  for (const match of matches) endpoints.add(cleanEndpoint(match[0]!));

  return [...endpoints];
}

function parseClientWorkState(html: string): unknown {
  const jsonParseMatch = html.match(
    /window\.clientWorkStateTemp\s*=\s*JSON\.parse\(("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')\)/s,
  );
  if (jsonParseMatch) {
    try {
      const decoded = JSON.parse(jsonParseMatch[1]!) as string;
      return JSON.parse(decoded) as unknown;
    } catch {}
  }

  const objectMatch = html.match(
    /window\.clientWorkStateTemp\s*=\s*(\{.*?\})\s*;\s*<\/script>/s,
  );
  if (objectMatch) {
    try {
      return JSON.parse(objectMatch[1]!) as unknown;
    } catch {}
  }

  return null;
}

function collectStaffLinks(value: unknown, endpoints: Set<string>): void {
  if (!value || typeof value !== "object") return;
  const state = value as Record<string, unknown>;
  const links = state.links as Record<string, unknown> | undefined;
  if (!links || typeof links !== "object") return;

  addEndpoint(links.staff, endpoints);

  const v4 = links.v4 as Record<string, unknown> | undefined;
  const staff = v4?.staff as Record<string, unknown> | undefined;
  addEndpoint(staff?.main, endpoints);
  const sections = staff?.sections as Record<string, unknown> | undefined;
  if (sections) {
    for (const endpoint of Object.values(sections))
      addEndpoint(endpoint, endpoints);
  }
}

function addEndpoint(value: unknown, endpoints: Set<string>): void {
  if (typeof value !== "string" || !isThrillshareDirectoryApi(value)) return;
  endpoints.add(cleanEndpoint(value));
}

function cleanEndpoint(endpoint: string): string {
  return endpoint.replace(/&amp;/g, "&");
}

async function scrapeDirectoryEndpoint(
  endpoint: string,
): Promise<RawTeacherData[]> {
  const rows: DirectoryRow[] = [];
  let nextUrl: string | null = endpoint;
  const seenPages = new Set<string>();

  for (let page = 0; nextUrl && page < MAX_DIRECTORY_PAGES; page++) {
    if (seenPages.has(nextUrl)) break;
    seenPages.add(nextUrl);

    const data: DirectoryResponse | null =
      await fetchJson<DirectoryResponse>(nextUrl);
    if (!data) break;

    const pageRows = data.directories ?? data.data ?? [];
    rows.push(...pageRows);

    nextUrl = data.meta?.links?.next
      ? cleanEndpoint(data.meta.links.next)
      : null;
  }

  return dedupeTeachers(
    rows.map(rowToTeacher).filter((t): t is RawTeacherData => !!t),
  );
}

function rowToTeacher(row: DirectoryRow): RawTeacherData | null {
  const name =
    row.full_name?.trim() ||
    [row.first, row.last].filter(Boolean).join(" ").trim();
  if (!name) return null;

  const role = row.title?.trim() || "";
  const department = row.department?.trim() || "";
  if (!isLikelyTeacher(role, department)) return null;

  return {
    name,
    ...(row.email?.trim() ? { email: row.email.trim() } : {}),
    ...(role ? { role } : {}),
    ...(isSubjectDepartment(department) ? { department } : {}),
  };
}

function isLikelyTeacher(role: string, department: string): boolean {
  const text = `${role} ${department}`.toLowerCase();
  if (!text.trim()) return false;

  const subjectSignal =
    /\b(math|mathematics|algebra|geometry|calculus|science|biology|chemistry|physics|english|language arts|social studies|history|art|music|band|choir|spanish|french|computer|technology|engineering|facs|physical education|pe)\b/.test(
      text,
    );
  const teaches =
    /\b(teacher|instructor|faculty|educator|interventionist)\b/.test(text) ||
    (/\b(coach|specialist|coordinator|chair)\b/.test(text) && subjectSignal) ||
    /\b(pre-?k|kindergarten|\d+(?:st|nd|rd|th)?\s+grade)\b/.test(text) ||
    subjectSignal;
  if (!teaches) return false;

  const supportOnly =
    /\b(superintendent|principal|assistant principal|vice principal|secretary|receptionist|clerk|bookkeeper|treasurer|payroll|custodian|maintenance|cafeteria|food service|transportation|bus driver|nurse|registrar|security|resource officer|technology director|athletic director|counselor|social worker|psychologist|therapist|librarian|media specialist|communications|board member)\b/.test(
      text,
    );
  const explicitTeaching =
    /\b(teacher|instructor|faculty|educator|coach|classroom|special education|nursing instructor)\b/.test(
      text,
    );
  return !supportOnly || explicitTeaching;
}

function isSubjectDepartment(department: string): boolean {
  return (
    /\b(math|mathematics|science|biology|chemistry|physics|english|language arts|social studies|history|art|music|computer|technology|engineering|stem|facs|physical education|pe)\b/i.test(
      department,
    ) &&
    !/\b(certified|classified|staff|administration|preschool)\b/i.test(
      department,
    )
  );
}

async function fetchText(url: string): Promise<string | null> {
  try {
    const res = await fetch(url, {
      redirect: "follow",
      signal: AbortSignal.timeout(DISCOVERY_TIMEOUT_MS),
    });
    if (!res.ok) return null;
    return await res.text();
  } catch {
    return null;
  }
}

async function fetchJson<T>(url: string): Promise<T | null> {
  try {
    const res = await fetch(url, {
      redirect: "follow",
      signal: AbortSignal.timeout(API_TIMEOUT_MS),
    });
    if (!res.ok) return null;
    return (await res.json()) as T;
  } catch {
    return null;
  }
}

function dedupeTeachers(teachers: RawTeacherData[]): RawTeacherData[] {
  const seen = new Map<string, RawTeacherData>();
  for (const teacher of teachers) {
    const key = (teacher.email || teacher.name).toLowerCase();
    if (!seen.has(key)) seen.set(key, teacher);
  }
  return [...seen.values()];
}

function dedupe<T>(values: T[]): T[] {
  return [...new Set(values)];
}
