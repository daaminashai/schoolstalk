#!/usr/bin/env bun

import { mkdir, readdir } from "node:fs/promises";
import { basename, dirname, extname, join, relative, resolve } from "node:path";

const DEFAULT_ROOT = "schools";
const DEFAULT_STAFF_URLS_CSV = "schools_with_staff_urls.csv";
const DEFAULT_SCHOOLS_COMPLETE_CSV = "schools_complete.csv";
const DEFAULT_OUTPUT = "output/cs_teachers_enriched.csv";
const DEFAULT_LIMIT = 200;

const TEACHER_HEADERS = [
  "first_name",
  "last_name",
  "email",
  "role",
  "department",
  "source_url",
  "data_sources",
] as const;

const OUTPUT_HEADERS = [
  ...TEACHER_HEADERS,
  "school_id",
  "school_name",
  "school_city",
  "school_state",
  "school_website",
  "school_staff_url",
  "school_mailing_address",
  "school_mailing_city",
  "school_mailing_state",
  "school_mailing_zip",
  "school_file",
] as const;

const CS_PATTERN = /\b(computer\s*science|comp\s*sci|computer\s*programming|coding|programming|software|web\s*development|app\s*development|\bCS\b)\b/i;

type Args = {
  root: string;
  staffUrlsCsv: string;
  schoolsCompleteCsv: string;
  output: string;
  limit: number;
};

type SchoolRecord = {
  id: string;
  name: string;
  city: string;
  state: string;
  website: string;
  streetAddress: string;
  mailingAddress: string;
  mailingCity: string;
  mailingState: string;
  mailingZip: string;
};

type StaffUrlRecord = {
  id: string;
  name: string;
  city: string;
  state: string;
  website: string;
  staffUrl: string;
};

type Candidate = {
  row: string[];
  indexes: Map<string, number>;
  schoolId: string;
  school: SchoolRecord | undefined;
  staffUrlRecord: StaffUrlRecord | undefined;
  fallback: { state: string; city: string; file: string };
};

function parseArgs(argv: string[]): Args {
  let root = DEFAULT_ROOT;
  let staffUrlsCsv = DEFAULT_STAFF_URLS_CSV;
  let schoolsCompleteCsv = DEFAULT_SCHOOLS_COMPLETE_CSV;
  let output = DEFAULT_OUTPUT;
  let limit = DEFAULT_LIMIT;

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];

    if (arg === "--root") {
      const value = argv[i + 1];
      if (!value) throw new Error("Missing value for --root");
      root = value;
      i += 1;
      continue;
    }

    if (arg === "--staff-urls-csv" || arg === "--schools-csv") {
      const value = argv[i + 1];
      if (!value) throw new Error(`Missing value for ${arg}`);
      staffUrlsCsv = value;
      i += 1;
      continue;
    }

    if (arg === "--schools-complete-csv") {
      const value = argv[i + 1];
      if (!value) throw new Error("Missing value for --schools-complete-csv");
      schoolsCompleteCsv = value;
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

    if (arg === "--limit") {
      const value = argv[i + 1];
      if (!value) throw new Error("Missing value for --limit");
      limit = Number.parseInt(value, 10);
      if (!Number.isFinite(limit) || limit <= 0) {
        throw new Error("--limit must be a positive integer");
      }
      i += 1;
      continue;
    }

    if (arg === "--help" || arg === "-h") {
      printUsage();
      process.exit(0);
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  return { root, staffUrlsCsv, schoolsCompleteCsv, output, limit };
}

function printUsage(): void {
  console.log(`Usage: bun scripts/export_cs_teachers.ts [options]

Options:
  --root <dir>                  School teacher CSV root. Default: ${DEFAULT_ROOT}
  --staff-urls-csv <file>       Staff URL CSV. Default: ${DEFAULT_STAFF_URLS_CSV}
  --schools-complete-csv <file> School metadata CSV. Default: ${DEFAULT_SCHOOLS_COMPLETE_CSV}
  --output, -o <file>           Output CSV. Default: ${DEFAULT_OUTPUT}
  --limit <n>                   Number of CS teachers to export. Default: ${DEFAULT_LIMIT}

The script joins teacher CSVs by the school id in the filename, e.g.
schools/AL/fairfield/7350.csv -> ID 7350 in the school metadata CSV.`);
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
      } else if (char !== undefined) {
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
    } else if (char !== "\r" && char !== undefined) {
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
  return value.trim().replace(/^\uFEFF/, "").toLowerCase().replace(/[\s_-]+/g, " ");
}

function getColumnIndexes(header: string[]): Map<string, number> {
  const indexes = new Map<string, number>();
  header.forEach((column, index) => {
    indexes.set(normalizeHeader(column), index);
  });
  return indexes;
}

function getCell(row: string[], indexes: Map<string, number>, column: string): string {
  const index = indexes.get(normalizeHeader(column));
  if (index === undefined) return "";
  return row[index] ?? "";
}

function getAnyCell(row: string[], indexes: Map<string, number>, columns: string[]): string {
  for (const column of columns) {
    const value = getCell(row, indexes, column).trim();
    if (value !== "") return value;
  }
  return "";
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

function compactJoin(parts: string[], separator: string): string {
  return parts.map((part) => part.trim()).filter(Boolean).join(separator);
}

function formatMailingAddress(row: string[], indexes: Map<string, number>): string {
  const directAddress = getAnyCell(row, indexes, [
    "entire mailing address",
    "mailing address",
    "school mailing address",
    "mailing_address",
  ]);
  if (directAddress) return directAddress;

  const street = getAnyCell(row, indexes, [
    "mailing street",
    "street mailing",
    "street_mailing",
    "mailing street 1",
    "street address",
    "address",
  ]);
  if (!street) return "";

  const city = getAnyCell(row, indexes, ["mailing city", "city mailing", "city_mailing", "city"]);
  const state = getAnyCell(row, indexes, ["mailing state", "state mailing", "state_mailing", "state"]);
  const zip = getAnyCell(row, indexes, ["mailing zip", "zip mailing", "zip_mailing", "zip", "zipcode"]);
  const stateZip = compactJoin([state, zip], " ");
  return compactJoin([street, city, stateZip], ", ");
}

async function loadSchoolsComplete(schoolsCompleteCsv: string): Promise<Map<string, SchoolRecord>> {
  const content = await Bun.file(schoolsCompleteCsv).text();
  const rows = parseCsv(content).filter((row) => !isBlankRow(row));
  const header = rows[0];
  if (!header) throw new Error(`No header found in ${schoolsCompleteCsv}`);

  const indexes = getColumnIndexes(header);
  const schools = new Map<string, SchoolRecord>();

  for (const row of rows.slice(1)) {
    const id = getAnyCell(row, indexes, ["hs id", "id", "school id"]);
    if (!id) continue;

    schools.set(id, {
      id,
      name: getAnyCell(row, indexes, ["name", "school name"]),
      city: getAnyCell(row, indexes, ["city"]),
      state: getAnyCell(row, indexes, ["state"]),
      website: getAnyCell(row, indexes, ["school homepage", "website", "school website"]),
      streetAddress: getAnyCell(row, indexes, ["street address", "street", "address"]),
      mailingAddress: formatMailingAddress(row, indexes),
      mailingCity: getAnyCell(row, indexes, ["mailing city", "city mailing", "city_mailing", "city"]),
      mailingState: getAnyCell(row, indexes, ["mailing state", "state mailing", "state_mailing", "state"]),
      mailingZip: getAnyCell(row, indexes, ["mailing zip", "zip mailing", "zip_mailing", "zip", "zipcode"]),
    });
  }

  return schools;
}

async function loadStaffUrls(staffUrlsCsv: string): Promise<Map<string, StaffUrlRecord>> {
  const content = await Bun.file(staffUrlsCsv).text();
  const rows = parseCsv(content).filter((row) => !isBlankRow(row));
  const header = rows[0];
  if (!header) throw new Error(`No header found in ${staffUrlsCsv}`);

  const indexes = getColumnIndexes(header);
  const staffUrls = new Map<string, StaffUrlRecord>();

  for (const row of rows.slice(1)) {
    const id = getAnyCell(row, indexes, ["hs id", "id", "school id"]);
    if (!id) continue;
    staffUrls.set(id, {
      id,
      name: getAnyCell(row, indexes, ["name", "school name"]),
      city: getAnyCell(row, indexes, ["city"]),
      state: getAnyCell(row, indexes, ["state"]),
      website: getAnyCell(row, indexes, ["school homepage", "website", "school website"]),
      staffUrl: getAnyCell(row, indexes, ["verified url", "primary url", "staff url"]),
    });
  }

  return staffUrls;
}

function isCsTeacher(row: string[], indexes: Map<string, number>): boolean {
  const role = getCell(row, indexes, "role");
  const department = getCell(row, indexes, "department");
  return CS_PATTERN.test(`${role} ${department}`);
}

function teacherKey(row: string[], indexes: Map<string, number>): string {
  const email = getCell(row, indexes, "email").trim().toLowerCase();
  if (email) return `email:${email}`;

  const firstName = getCell(row, indexes, "first_name").trim().toLowerCase();
  const lastName = getCell(row, indexes, "last_name").trim().toLowerCase();
  const role = getCell(row, indexes, "role").trim().toLowerCase();
  const department = getCell(row, indexes, "department").trim().toLowerCase();
  return `name-role:${firstName}:${lastName}:${role}:${department}`;
}

function fallbackPathParts(root: string, csvPath: string): { state: string; city: string; file: string } {
  const file = relative(root, csvPath);
  const parts = file.split(/[\\/]/);
  return {
    state: parts[0] ?? "",
    city: parts[1] ?? "",
    file,
  };
}

function getCandidateState(candidate: Candidate): string {
  return candidate.school?.state || candidate.staffUrlRecord?.state || candidate.fallback.state || "";
}

function candidateToOutputRow(candidate: Candidate): string[] {
  const { row, indexes, schoolId, school, staffUrlRecord, fallback } = candidate;
  return [
    ...TEACHER_HEADERS.map((headerName) => getCell(row, indexes, headerName)),
    schoolId,
    school?.name || staffUrlRecord?.name || "",
    school?.city || staffUrlRecord?.city || fallback.city,
    school?.state || staffUrlRecord?.state || fallback.state,
    school?.website || staffUrlRecord?.website || "",
    staffUrlRecord?.staffUrl ?? "",
    school?.mailingAddress ?? "",
    school?.mailingCity ?? "",
    school?.mailingState ?? "",
    school?.mailingZip ?? "",
    fallback.file,
  ];
}

function selectDiverseCandidates(candidatesBySchool: Map<string, Candidate>, limit: number): Candidate[] {
  const candidatesByState = new Map<string, Candidate[]>();

  for (const candidate of candidatesBySchool.values()) {
    const state = getCandidateState(candidate);
    const stateCandidates = candidatesByState.get(state) ?? [];
    stateCandidates.push(candidate);
    candidatesByState.set(state, stateCandidates);
  }

  const states = Array.from(candidatesByState.keys()).sort((a, b) => a.localeCompare(b));
  const selected: Candidate[] = [];

  while (selected.length < limit && states.some((state) => (candidatesByState.get(state)?.length ?? 0) > 0)) {
    for (const state of states) {
      if (selected.length >= limit) break;
      const stateCandidates = candidatesByState.get(state);
      const candidate = stateCandidates?.shift();
      if (candidate) selected.push(candidate);
    }
  }

  return selected;
}

async function main(): Promise<number> {
  const args = parseArgs(Bun.argv.slice(2));
  const root = resolve(args.root);
  const staffUrlsCsv = resolve(args.staffUrlsCsv);
  const schoolsCompleteCsv = resolve(args.schoolsCompleteCsv);
  const output = resolve(args.output);
  const schools = await loadSchoolsComplete(schoolsCompleteCsv);
  const staffUrls = await loadStaffUrls(staffUrlsCsv);
  const csvPaths = await listCsvFiles(root);
  const seenTeachers = new Set<string>();
  const candidatesBySchool = new Map<string, Candidate>();
  const outputRows: string[][] = [Array.from(OUTPUT_HEADERS)];

  let matchedRows = 0;
  let duplicateRows = 0;

  for (const csvPath of csvPaths) {
    const content = await Bun.file(csvPath).text();
    const rows = parseCsv(content).filter((row) => !isBlankRow(row));
    const header = rows[0];
    if (!header || rows.length <= 1) continue;

    const indexes = getColumnIndexes(header);
    const schoolId = basename(csvPath, ".csv");
    const school = schools.get(schoolId);
    const staffUrlRecord = staffUrls.get(schoolId);
    const fallback = fallbackPathParts(root, csvPath);

    for (const row of rows.slice(1)) {
      if (!isCsTeacher(row, indexes)) continue;
      matchedRows += 1;

      const key = teacherKey(row, indexes);
      if (seenTeachers.has(key)) {
        duplicateRows += 1;
        continue;
      }
      seenTeachers.add(key);

      if (candidatesBySchool.has(schoolId)) continue;
      candidatesBySchool.set(schoolId, {
        row,
        indexes,
        schoolId,
        school,
        staffUrlRecord,
        fallback,
      });
    }
  }

  const selectedCandidates = selectDiverseCandidates(candidatesBySchool, args.limit);
  for (const candidate of selectedCandidates) {
    outputRows.push(candidateToOutputRow(candidate));
  }

  const selectedStates = new Set(selectedCandidates.map(getCandidateState));
  const missingSchoolRows = selectedCandidates.filter((candidate) => !candidate.school).length;

  await mkdir(dirname(output), { recursive: true });
  await Bun.write(output, serializeCsv(outputRows));

  console.log(`root: ${root}`);
  console.log(`staff_urls_csv: ${staffUrlsCsv}`);
  console.log(`schools_complete_csv: ${schoolsCompleteCsv}`);
  console.log(`csv_files: ${csvPaths.length}`);
  console.log(`cs_rows_matched_before_dedupe: ${matchedRows}`);
  console.log(`duplicate_rows_skipped: ${duplicateRows}`);
  console.log(`unique_schools_with_cs_teacher: ${candidatesBySchool.size}`);
  console.log(`states_represented: ${selectedStates.size}`);
  console.log(`rows_missing_school_metadata: ${missingSchoolRows}`);
  console.log(`teachers_written: ${outputRows.length - 1}`);
  console.log(`repeated_schools: 0`);
  console.log(`output: ${relative(process.cwd(), output)}`);

  return 0;
}

main().then(
  (code) => process.exit(code),
  (error: unknown) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  },
);
