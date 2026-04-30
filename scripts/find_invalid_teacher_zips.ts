#!/usr/bin/env bun

import { resolve } from "node:path";

const DEFAULT_INPUT = "teachers-Grid view.csv";
const DEFAULT_OUTPUT = "invalid_teacher_zips.csv";
const VALID_ZIP_PATTERN = /^\d{5}(?:-\d{4})?$/;

type Args = {
  input: string;
  output: string;
};

type CsvRow = {
  fields: string[];
  line: number;
};

type InvalidZipRow = {
  line: number;
  zip: string;
  reason: string;
  id: string;
  schoolName: string;
  city: string;
  state: string;
  teacherFirstName: string;
  teacherLastName: string;
  teacherEmail: string;
  fields: string[];
};

function parseArgs(argv: string[]): Args {
  let input = DEFAULT_INPUT;
  let output = DEFAULT_OUTPUT;

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];

    if (arg === "--input" || arg === "-i") {
      const value = argv[i + 1];
      if (!value) throw new Error(`Missing value for ${arg}`);
      input = value;
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

  return { input, output };
}

function printUsage(): void {
  console.log(`Usage: bun scripts/find_invalid_teacher_zips.ts [--input "teachers-Grid view.csv"] [--output invalid_teacher_zips.csv]

Writes a new CSV containing rows where the Zip column is not a strict 5-digit ZIP or ZIP+4 value.`);
}

function parseCsv(content: string): CsvRow[] {
  const rows: CsvRow[] = [];
  let row: string[] = [];
  let field = "";
  let inQuotes = false;
  let line = 1;
  let rowStartLine = 1;

  for (let i = 0; i < content.length; i += 1) {
    const char = content[i];
    const next = content[i + 1];

    if (inQuotes) {
      if (char === '"' && next === '"') {
        field += '"';
        i += 1;
      } else if (char === '"') {
        inQuotes = false;
      } else if (char !== undefined) {
        field += char;
        if (char === "\n") line += 1;
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
      rows.push({ fields: row, line: rowStartLine });
      row = [];
      field = "";
      line += 1;
      rowStartLine = line;
    } else if (char !== "\r" && char !== undefined) {
      field += char;
    }
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push({ fields: row, line: rowStartLine });
  }

  return rows;
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

function normalizeHeader(value: string): string {
  return value.trim().replace(/^\uFEFF/, "").toLowerCase();
}

function getColumnIndex(header: string[], column: string): number {
  const index = header.findIndex((value) => normalizeHeader(value) === normalizeHeader(column));
  if (index === -1) throw new Error(`No ${column} column found`);
  return index;
}

function getOptionalColumnIndex(header: string[], column: string): number | null {
  const index = header.findIndex((value) => normalizeHeader(value) === normalizeHeader(column));
  return index === -1 ? null : index;
}

function field(row: string[], index: number | null): string {
  if (index === null) return "";
  return row[index] ?? "";
}

function getInvalidZipReason(value: string): string | null {
  const trimmed = value.trim();

  if (trimmed === "") return "blank";
  if (VALID_ZIP_PATTERN.test(trimmed)) return null;

  if (/^\d+$/.test(trimmed)) {
    return `${trimmed.length}-digit zip`;
  }

  if (/^\d{5}-\d+$/.test(trimmed)) {
    return "ZIP+4 suffix is not 4 digits";
  }

  if (/^\d{5}\s*-\s*\d{4}$/.test(trimmed)) {
    return "ZIP+4 contains spaces";
  }

  return "contains non-ZIP characters";
}

function toReportRows(header: string[], invalidRows: InvalidZipRow[]): string[][] {
  return [
    ["invalid_zip_line", "invalid_zip_reason", ...header],
    ...invalidRows.map((row) => [
      String(row.line),
      row.reason,
      ...row.fields,
    ]),
  ];
}

async function main(): Promise<number> {
  const args = parseArgs(Bun.argv.slice(2));
  const input = resolve(args.input);
  const content = await Bun.file(input).text();
  const rows = parseCsv(content);
  const headerRow = rows[0];

  if (!headerRow) throw new Error(`No rows found in ${args.input}`);

  const header = headerRow.fields;
  const zipIndex = getColumnIndex(header, "Zip");
  const idIndex = getOptionalColumnIndex(header, "ID");
  const schoolNameIndex = getOptionalColumnIndex(header, "School Name");
  const cityIndex = getOptionalColumnIndex(header, "City");
  const stateIndex = getOptionalColumnIndex(header, "State");
  const teacherFirstNameIndex = getOptionalColumnIndex(header, "teacher_first_name");
  const teacherLastNameIndex = getOptionalColumnIndex(header, "teacher_last_name");
  const teacherEmailIndex = getOptionalColumnIndex(header, "teacher_email");
  const invalidRows: InvalidZipRow[] = [];
  const reasonCounts = new Map<string, number>();

  for (const csvRow of rows.slice(1)) {
    const row = csvRow.fields;
    const zip = row[zipIndex] ?? "";
    const reason = getInvalidZipReason(zip);

    if (reason === null) continue;

    invalidRows.push({
      line: csvRow.line,
      zip,
      reason,
      id: field(row, idIndex),
      schoolName: field(row, schoolNameIndex),
      city: field(row, cityIndex),
      state: field(row, stateIndex),
      teacherFirstName: field(row, teacherFirstNameIndex),
      teacherLastName: field(row, teacherLastNameIndex),
      teacherEmail: field(row, teacherEmailIndex),
      fields: row,
    });
    reasonCounts.set(reason, (reasonCounts.get(reason) ?? 0) + 1);
  }

  console.log(`file: ${args.input}`);
  console.log(`total_rows: ${Math.max(rows.length - 1, 0)}`);
  console.log(`invalid_zip_rows: ${invalidRows.length}`);
  console.log("reasons:");
  for (const [reason, count] of Array.from(reasonCounts).sort(([a], [b]) => a.localeCompare(b))) {
    console.log(`${reason}: ${count}`);
  }

  await Bun.write(resolve(args.output), serializeCsv(toReportRows(header, invalidRows)));
  console.log(`output: ${args.output}`);

  return 0;
}

main().then(
  (code) => process.exit(code),
  (error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  },
);
