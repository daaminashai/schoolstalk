import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import type { ScrapeResult, Teacher } from "./types";

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

function teacherToRow(teacher: Teacher, result: ScrapeResult): string {
  // source_url: where the teacher's data was scraped from.
  const sourceUrl = result.sourceUrl;

  const fields: string[] = [
    teacher.firstName,
    teacher.lastName,
    teacher.email ?? "",
    teacher.role,
    teacher.department ?? "",
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
