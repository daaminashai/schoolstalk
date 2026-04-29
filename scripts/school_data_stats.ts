#!/usr/bin/env bun

import { mkdir, readdir } from "node:fs/promises";
import { dirname, extname, join, relative, resolve } from "node:path";

const DEFAULT_ROOT = "schools";
const DEFAULT_OUTPUT = "output/all_teachers.csv";
const TEACHER_HEADERS = [
  "first_name",
  "last_name",
  "email",
  "role",
  "department",
  "source_url",
  "data_sources",
] as const;

type Args = {
  root: string;
  output: string;
};

function parseArgs(argv: string[]): Args {
  let root = DEFAULT_ROOT;
  let output = DEFAULT_OUTPUT;

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--root") {
      const value = argv[i + 1];
      if (!value) throw new Error("Missing value for --root");
      root = value;
      i += 1;
      continue;
    }
    if (arg === "--output" || arg === "-o") {
      const value = argv[i + 1];
      if (!value) throw new Error(`Missing value for ${arg}`);
      output = value;
      i += 1;
      continue;
    }
    if (arg === "--help" || arg === "-h") {
      printUsage();
      process.exit(0);
    }
    throw new Error(`Unknown argument: ${arg}`);
  }

  return { root, output };
}

function printUsage(): void {
  console.log(`Usage: bun scripts/school_data_stats.ts [--root schools] [--output output/all_teachers.csv]

Reports:
  total_csvs
  empty_csvs
  total_teachers
  teachers_without_email

Also writes one combined teacher CSV to --output.`);
}

async function listCsvFiles(root: string): Promise<string[]> {
  const paths: string[] = [];

  async function walk(dir: string): Promise<void> {
    const entries = await readdir(dir, { withFileTypes: true });
    for (const entry of entries) {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) {
        await walk(path);
      } else if (entry.isFile() && extname(entry.name).toLowerCase() === ".csv") {
        paths.push(path);
      }
    }
  }

  await walk(root);
  return paths.sort((a, b) => a.localeCompare(b));
}

function parseCsv(content: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQuotes = false;

  for (let i = 0; i < content.length; i += 1) {
    const char = content[i];
    const next = content[i + 1];

    if (inQuotes) {
      if (char === '"' && next === '"') {
        field += '"';
        i += 1;
      } else if (char === '"') {
        inQuotes = false;
      } else {
        field += char;
      }
      continue;
    }

    if (char === '"') {
      inQuotes = true;
    } else if (char === ",") {
      row.push(field);
      field = "";
    } else if (char === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
    } else if (char !== "\r") {
      field += char;
    }
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }

  return rows;
}

function isBlankRow(row: string[]): boolean {
  return row.every((cell) => cell.trim() === "");
}

function normalizeHeader(value: string): string {
  return value.trim().replace(/^\uFEFF/, "").toLowerCase();
}

function getColumnIndexes(header: string[]): Map<string, number> {
  const indexes = new Map<string, number>();
  header.forEach((column, index) => {
    indexes.set(normalizeHeader(column), index);
  });
  return indexes;
}

function getCell(row: string[], indexes: Map<string, number>, column: string): string {
  const index = indexes.get(column);
  if (index === undefined) return "";
  return row[index] ?? "";
}

function toTeacherRow(row: string[], indexes: Map<string, number>): string[] {
  return TEACHER_HEADERS.map((header) => getCell(row, indexes, header));
}

function escapeCsvField(value: string): string {
  if (/[",\n\r]/.test(value)) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

function serializeCsv(rows: string[][]): string {
  return rows.map((row) => row.map(escapeCsvField).join(",")).join("\n") + "\n";
}

async function main(): Promise<number> {
  const args = parseArgs(Bun.argv.slice(2));
  const root = resolve(args.root);
  const output = resolve(args.output);
  const csvPaths = await listCsvFiles(root);

  let emptyCsvs = 0;
  let totalTeachers = 0;
  let teachersWithoutEmail = 0;
  const teacherRows: string[][] = [];

  for (const csvPath of csvPaths) {
    const content = await Bun.file(csvPath).text();
    const rows = parseCsv(content).filter((row) => !isBlankRow(row));

    if (rows.length <= 1) {
      emptyCsvs += 1;
      continue;
    }

    const header = rows[0];
    if (!header) {
      emptyCsvs += 1;
      continue;
    }

    const indexes = getColumnIndexes(header);
    const dataRows = rows.slice(1);

    for (const row of dataRows) {
      const teacherRow = toTeacherRow(row, indexes);
      teacherRows.push(teacherRow);
      totalTeachers += 1;

      const email = getCell(row, indexes, "email").trim();
      if (email === "") teachersWithoutEmail += 1;
    }
  }

  await mkdir(dirname(output), { recursive: true });
  await Bun.write(output, serializeCsv([Array.from(TEACHER_HEADERS), ...teacherRows]));

  console.log(`root: ${root}`);
  console.log(`total_csvs: ${csvPaths.length}`);
  console.log(`empty_csvs: ${emptyCsvs}`);
  console.log(`total_teachers: ${totalTeachers}`);
  console.log(`teachers_without_email: ${teachersWithoutEmail}`);
  console.log(`combined_csv: ${relative(process.cwd(), output)}`);

  return 0;
}

main().then(
  (code) => process.exit(code),
  (error: unknown) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  },
);
