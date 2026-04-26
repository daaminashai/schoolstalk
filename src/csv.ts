import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import type { ScrapeResult, Teacher } from "./types";
import { canonicalizeDepartment } from "./names";

const HEADERS = [
  "first_name",
  "last_name",
  "email",
  "role",
  "department",
  "source_url",
  "data_sources",
] as const;

function escapeField(value: string): string {
  if (value.includes(",") || value.includes('"') || value.includes("\n")) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

// Merge department into role for output. Example:
// role: "2nd Grade Teacher", department: "Mathematics" → "2nd Grade mathematics Teacher".
// If the role already mentions the subject (e.g. "Math Teacher"), don't duplicate.
function mergeRoleAndDepartment(role: string, department: string | null): string {
  if (!department) return role;
  const canon = canonicalizeDepartment(department);
  if (!canon) return role;

  const roleLower = role.toLowerCase();
  const subjLower = canon.toLowerCase();

  // basic duplicate checks and common aliases so we don't add twice
  const aliasChecks: Record<string, RegExp[]> = {
    "Mathematics": [/\bmathematics\b/i, /\bmath\b/i],
    "Science": [/\bscience\b/i],
    "Biology": [/\bbiology\b/i, /\bbio\b/i],
    "Chemistry": [/\bchemistry\b/i, /\bchem\b/i],
    "Physics": [/\bphysics\b/i, /\bphysic\w*/i],
    "Environmental Science": [/\benvironmental\b/i],
    "Earth Science": [/\bearth\b/i, /\bgeolog\w*/i, /\bastronom\w*/i],
    "Computer Science": [/\bcomputer\s+science\b/i, /\bcomp\s*sci\b/i, /\bcs\b/, /\bcoding\b/i, /\bprogramming\b/i, /\bsoftware\b/i],
    "Engineering": [/\bengineering\b/i],
    "Technology": [/\btechnology\b/i, /\btech\b/i, /\btech\s*ed\b/i, /\bIT\b/],
    "Robotics": [/\brobot\w*/i],
    "Maker": [/\bmaker\w*/i],
    "STEM": [/\bSTEM\b/],
  };

  for (const re of aliasChecks[canon] ?? [new RegExp(`\\b${subjLower}\\b`, "i")]) {
    if (re.test(role)) return role;
  }

  // Insert subject before the first occurrence of "teacher" or "instructor"
  const m = role.match(/\b(teacher|instructor)\b/i);
  if (m && m.index != null) {
    const idx = m.index;
    const before = role.slice(0, idx).replace(/\s+$/, " ");
    const after = role.slice(idx);
    const subjectText = subjLower; // use lowercase for readability (e.g., "math teacher")
    return `${before}${subjectText} ${after}`.replace(/\s{2,}/g, " ").trim();
  }

  // If no obvious insertion point, leave role as-is to avoid mangling titles like "Department Chair"
  return role;
}

function teacherToRow(teacher: Teacher, result: ScrapeResult): string {
  // source_url: where the teacher's data was scraped from.
  const sourceUrl = result.sourceUrl;

  const mergedRole = mergeRoleAndDepartment(teacher.role, teacher.department);

  const fields: string[] = [
    teacher.firstName,
    teacher.lastName,
    teacher.email ?? "",
    mergedRole,
    "", // department column intentionally left blank in output
    sourceUrl,
    teacher.sources.join(";"),
  ];

  return fields.map(escapeField).join(",");
}

export function generateCsv(result: ScrapeResult): string {
  const header = HEADERS.join(",");
  const rows = result.teachers.map((t) => teacherToRow(t, result));
  return [header, ...rows].join("\n") + "\n";
}

/**
 * concatenate multiple ScrapeResults into a single CSV — one header, then all
 * teacher rows from all results in order. the schema is unchanged (source_url
 * already identifies which site each teacher came from), so consumers don't
 * need to handle a new column. used by batch mode's --merged-output.
 */
export function generateMergedCsv(results: ScrapeResult[]): string {
  const header = HEADERS.join(",");
  const rows: string[] = [];
  for (const r of results) {
    for (const t of r.teachers) rows.push(teacherToRow(t, r));
  }
  return [header, ...rows].join("\n") + "\n";
}

export async function writeCsv(filePath: string, content: string): Promise<void> {
  await mkdir(dirname(filePath), { recursive: true });
  await Bun.write(filePath, content);
}
