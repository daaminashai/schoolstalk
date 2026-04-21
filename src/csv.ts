import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import type { ScrapeResult, SchoolInfo, Teacher } from "./types";

const HEADERS = [
  "first_name",
  "last_name",
  "email",
  "role",
  "department",
  "school_name",
  "school_address",
  "school_city",
  "school_state",
  "school_zip",
  "school_phone",
  "school_district",
  "source_url",
  "data_sources",
] as const;

function escapeField(value: string): string {
  if (value.includes(",") || value.includes('"') || value.includes("\n")) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

/** resolve the SchoolInfo for a given teacher with layered fallbacks */
function schoolForTeacher(teacher: Teacher, result: ScrapeResult): SchoolInfo | null {
  // 1. composite match (name + ncesId) — most specific. handles the case where
  //    multiple synthetic schools share one umbrella ncesId (e.g. both
  //    "Allen Brook School" and "Williston Central School" inherit the federal
  //    "Williston Schools" ncesId).
  if (teacher.schoolName && teacher.schoolNcesId) {
    const lower = teacher.schoolName.toLowerCase();
    const composite = result.schools.find(
      (s) => s.ncesId === teacher.schoolNcesId && s.name.toLowerCase() === lower,
    );
    if (composite) return composite;
  }
  // 2. name match — keeps the teacher's site-level display name as authoritative
  if (teacher.schoolName) {
    const lower = teacher.schoolName.toLowerCase();
    const byName = result.schools.find((s) => s.name.toLowerCase() === lower);
    if (byName) return byName;
  }
  // 3. ncesId match — last resort, may pick an arbitrary synthetic sibling
  if (teacher.schoolNcesId) {
    const byId = result.schools.find((s) => s.ncesId === teacher.schoolNcesId);
    if (byId) return byId;
  }
  // no match: in single-school mode, return the only school; in district mode
  // return null so the caller can substitute district-office data.
  if (!result.district && result.schools.length === 1) return result.schools[0]!;
  return null;
}

function teacherToRow(teacher: Teacher, result: ScrapeResult): string {
  const school = schoolForTeacher(teacher, result);
  const district = result.district;

  // pick address: teacher's resolved school > district office > empty
  const addr = school?.address ?? district?.officeAddress ?? null;

  // school_name column: the teacher's specific school when known, otherwise
  // whatever label makes sense in the current mode.
  const schoolName =
    school?.name ?? teacher.schoolName ?? (district ? "" : result.schools[0]?.name ?? "");

  // school_phone: teacher's school phone > district office phone > empty
  const schoolPhone = school?.phone ?? district?.officePhone ?? "";

  // school_district column: prefer the top-level district, else the school's
  // nces-provided lea_name.
  const districtName = district?.name ?? school?.district ?? "";

  // source_url: where the teacher's data was scraped from. in district mode
  // that's the district site (everyone shares the same directory root); in
  // single-school mode it's the school's site. both are tracked as schoolUrl
  // in the scrape config and stored on district/school records.
  const sourceUrl = district?.url ?? school?.url ?? result.schools[0]?.url ?? "";

  const fields: string[] = [
    teacher.firstName,
    teacher.lastName,
    teacher.email ?? "",
    teacher.role,
    teacher.department ?? "",
    schoolName,
    addr?.street ?? "",
    addr?.city ?? "",
    addr?.state ?? "",
    addr?.zip ?? "",
    schoolPhone,
    districtName,
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
