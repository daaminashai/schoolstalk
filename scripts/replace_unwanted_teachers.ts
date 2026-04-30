#!/usr/bin/env bun

import { mkdir, readdir } from "node:fs/promises";
import { basename, dirname, extname, join, relative, resolve } from "node:path";
import { findRoleSignals } from "./find_unwanted_teacher_roles.ts";

const DEFAULT_INPUT = "teachers_all_replaced.csv";
const DEFAULT_REMOVED_INPUT = "unwanted_teacher_roles.csv";
const DEFAULT_TEACHERS_ROOT = "teachers";
const DEFAULT_OUTPUT = "teachers_all_replaced_final.csv";
const DEFAULT_REPORT_OUTPUT = "teacher_replacements.csv";

type Args = {
  input: string;
  removedInput: string;
  teachersRoot: string;
  output: string;
  reportOutput: string;
  inPlace: boolean;
  dryRun: boolean;
  strict: boolean;
  limit: number | null;
};

type CsvRow = {
  fields: string[];
  line: number;
};

type SchoolMetadata = {
  id: string;
  values: Map<string, string>;
};

type Candidate = {
  schoolId: string;
  firstName: string;
  lastName: string;
  email: string;
  role: string;
  sourceUrl: string;
  dataSources: string;
  subject: string;
  reasons: string;
  key: string;
  sourceFile: string;
};

type CandidateLoadResult = {
  bySchool: Map<string, Candidate[]>;
  all: Candidate[];
  csvFiles: number;
  totalRows: number;
  acceptedRows: number;
  duplicateRows: number;
  blankRoleRows: number;
  unwantedRoleRows: number;
};

type Replacement = {
  strategy: "same_school" | "fallback_school";
  removedRow: string[];
  replacementRow: string[];
  candidate: Candidate;
};

const TEACHER_OUTPUT_COLUMNS = new Set(
  [
    "teacher_first_name",
    "teacher_last_name",
    "teacher_role",
    "teacher_email",
    "teacher_source_url",
    "teacher_data_sources",
    "teacher_subject",
    "teacher_reasons",
  ].map(normalizeHeader),
);

function parseArgs(argv: string[]): Args {
  let input = DEFAULT_INPUT;
  let removedInput = DEFAULT_REMOVED_INPUT;
  let teachersRoot = DEFAULT_TEACHERS_ROOT;
  let output = DEFAULT_OUTPUT;
  let reportOutput = DEFAULT_REPORT_OUTPUT;
  let inPlace = false;
  let dryRun = false;
  let strict = false;
  let limit: number | null = null;

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];

    if (arg === "--input" || arg === "-i") {
      const value = argv[i + 1];
      if (!value) throw new Error(`Missing value for ${arg}`);
      input = value;
      i += 1;
      continue;
    }

    if (arg === "--removed" || arg === "--removed-input") {
      const value = argv[i + 1];
      if (!value) throw new Error(`Missing value for ${arg}`);
      removedInput = value;
      i += 1;
      continue;
    }

    if (arg === "--teachers-root") {
      const value = argv[i + 1];
      if (!value) throw new Error(`Missing value for ${arg}`);
      teachersRoot = value;
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

    if (arg === "--report-output") {
      const value = argv[i + 1];
      if (!value) throw new Error(`Missing value for ${arg}`);
      reportOutput = value;
      i += 1;
      continue;
    }

    if (arg === "--limit") {
      const value = argv[i + 1];
      if (!value) throw new Error(`Missing value for ${arg}`);
      limit = Number.parseInt(value, 10);
      if (!Number.isFinite(limit) || limit < 0)
        throw new Error("--limit must be a non-negative integer");
      i += 1;
      continue;
    }

    if (arg === "--in-place") {
      inPlace = true;
      continue;
    }

    if (arg === "--dry-run") {
      dryRun = true;
      continue;
    }

    if (arg === "--strict") {
      strict = true;
      continue;
    }

    if (arg === "--help" || arg === "-h") {
      printUsage();
      process.exit(0);
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  return {
    input,
    removedInput,
    teachersRoot,
    output,
    reportOutput,
    inPlace,
    dryRun,
    strict,
    limit,
  };
}

function printUsage(): void {
  console.log(`Usage: bun scripts/replace_unwanted_teachers.ts [options]

Options:
  --input, -i <file>          Cleaned teachers CSV. Default: ${DEFAULT_INPUT}
  --removed <file>            CSV from find_unwanted_teacher_roles.ts. Default: ${DEFAULT_REMOVED_INPUT}
  --teachers-root <dir>       Teacher candidate CSV root. Default: ${DEFAULT_TEACHERS_ROOT}
  --output, -o <file>         Output CSV. Default: ${DEFAULT_OUTPUT}
  --report-output <file>      Replacement audit CSV. Default: ${DEFAULT_REPORT_OUTPUT}
  --limit <n>                 Replace at most n removed records. Default: all removed records
  --in-place                  Overwrite --input instead of writing --output
  --dry-run                   Print counts without writing files
  --strict                    Exit non-zero if fewer replacements are found than requested

For each removed row, the script chooses the first safe unused teacher from the same school.
If none exists, it chooses a safe unused teacher from another school with known metadata.
Safety uses the same unwanted-role rules as scripts/find_unwanted_teacher_roles.ts.`);
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

function isBlankRow(row: string[]): boolean {
  return row.every((cell) => cell.trim() === "");
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
  return value
    .trim()
    .replace(/^\uFEFF/, "")
    .toLowerCase()
    .replace(/[\s_-]+/g, " ");
}

function getColumnIndexes(header: string[]): Map<string, number> {
  const indexes = new Map<string, number>();
  header.forEach((column, index) =>
    indexes.set(normalizeHeader(column), index),
  );
  return indexes;
}

function getCell(
  row: string[],
  indexes: Map<string, number>,
  column: string,
): string {
  const index = indexes.get(normalizeHeader(column));
  if (index === undefined) return "";
  return row[index] ?? "";
}

async function listCsvFiles(root: string): Promise<string[]> {
  const paths: string[] = [];

  async function walk(dir: string): Promise<void> {
    const entries = await readdir(dir, { withFileTypes: true });
    for (const entry of entries) {
      if (entry.name.startsWith(".")) continue;
      const path = join(dir, entry.name);
      if (entry.isDirectory()) {
        await walk(path);
      } else if (
        entry.isFile() &&
        extname(entry.name).toLowerCase() === ".csv"
      ) {
        paths.push(path);
      }
    }
  }

  await walk(root);
  return paths.sort((a, b) => a.localeCompare(b));
}

function schoolIdFromPath(csvPath: string): string {
  const csvName = basename(csvPath, ".csv");
  return csvName.endsWith(".top") ? csvName.slice(0, -4) : csvName;
}

function teacherKey(
  schoolId: string,
  firstName: string,
  lastName: string,
  email: string,
  role: string,
): string {
  const normalizedEmail = email.trim().toLowerCase();
  if (normalizedEmail) return `email:${normalizedEmail}`;

  return ["school-name-role", schoolId, firstName, lastName, role]
    .map((part) => part.trim().toLowerCase().replace(/\s+/g, " "))
    .join(":");
}

function outputTeacherKey(row: string[], indexes: Map<string, number>): string {
  return teacherKey(
    getCell(row, indexes, "ID"),
    getCell(row, indexes, "teacher_first_name"),
    getCell(row, indexes, "teacher_last_name"),
    getCell(row, indexes, "teacher_email"),
    getCell(row, indexes, "teacher_role"),
  );
}

function metadataFromTargetRow(
  header: string[],
  row: string[],
  indexes: Map<string, number>,
): SchoolMetadata | null {
  const id = getCell(row, indexes, "ID").trim();
  if (!id) return null;

  const values = new Map<string, string>();
  header.forEach((column, index) => {
    const normalized = normalizeHeader(column);
    if (TEACHER_OUTPUT_COLUMNS.has(normalized)) return;
    values.set(normalized, row[index] ?? "");
  });
  values.set(normalizeHeader("ID"), id);

  return { id, values };
}

function mergeMetadata(
  target: Map<string, SchoolMetadata>,
  metadata: SchoolMetadata | null,
): void {
  if (!metadata) return;

  const existing = target.get(metadata.id);
  if (!existing) {
    target.set(metadata.id, metadata);
    return;
  }

  for (const [column, value] of metadata.values) {
    if (
      (existing.values.get(column) ?? "").trim() === "" &&
      value.trim() !== ""
    ) {
      existing.values.set(column, value);
    }
  }
}

function projectRemovedRows(
  removedRows: CsvRow[],
  removedIndexes: Map<string, number>,
  targetHeader: string[],
): string[][] {
  return removedRows
    .map((csvRow) =>
      targetHeader.map((column) =>
        getCell(csvRow.fields, removedIndexes, column),
      ),
    )
    .filter((row) => !isBlankRow(row));
}

function candidateFromRow(
  row: string[],
  indexes: Map<string, number>,
  schoolId: string,
  sourceFile: string,
): Candidate | null {
  const firstName = getCell(row, indexes, "first_name").trim();
  const lastName = getCell(row, indexes, "last_name").trim();
  const email = getCell(row, indexes, "email").trim();
  const role = getCell(row, indexes, "role").trim();

  if (!role || (!firstName && !lastName)) return null;
  if (findRoleSignals(role).length > 0) return null;

  return {
    schoolId,
    firstName,
    lastName,
    email,
    role,
    sourceUrl: getCell(row, indexes, "source_url"),
    dataSources: getCell(row, indexes, "data_sources"),
    subject: getCell(row, indexes, "subject"),
    reasons: getCell(row, indexes, "reasons"),
    key: teacherKey(schoolId, firstName, lastName, email, role),
    sourceFile,
  };
}

async function loadCandidates(
  root: string,
  blockedKeys: Set<string>,
): Promise<CandidateLoadResult> {
  const csvPaths = await listCsvFiles(root);
  const bySchool = new Map<string, Candidate[]>();
  const all: Candidate[] = [];
  const seen = new Set(blockedKeys);
  let totalRows = 0;
  let acceptedRows = 0;
  let duplicateRows = 0;
  let blankRoleRows = 0;
  let unwantedRoleRows = 0;

  for (const csvPath of csvPaths) {
    const content = await Bun.file(csvPath).text();
    const rows = parseCsv(content).filter(
      (csvRow) => !isBlankRow(csvRow.fields),
    );
    const headerRow = rows[0];
    if (!headerRow || rows.length <= 1) continue;

    const indexes = getColumnIndexes(headerRow.fields);
    const schoolId = schoolIdFromPath(csvPath);

    for (const csvRow of rows.slice(1)) {
      totalRows += 1;
      const role = getCell(csvRow.fields, indexes, "role").trim();
      if (!role) {
        blankRoleRows += 1;
        continue;
      }
      if (findRoleSignals(role).length > 0) {
        unwantedRoleRows += 1;
        continue;
      }

      const candidate = candidateFromRow(
        csvRow.fields,
        indexes,
        schoolId,
        csvPath,
      );
      if (!candidate) {
        blankRoleRows += 1;
        continue;
      }
      if (seen.has(candidate.key)) {
        duplicateRows += 1;
        continue;
      }

      seen.add(candidate.key);
      acceptedRows += 1;
      all.push(candidate);

      const schoolCandidates = bySchool.get(schoolId) ?? [];
      schoolCandidates.push(candidate);
      bySchool.set(schoolId, schoolCandidates);
    }
  }

  return {
    bySchool,
    all,
    csvFiles: csvPaths.length,
    totalRows,
    acceptedRows,
    duplicateRows,
    blankRoleRows,
    unwantedRoleRows,
  };
}

function candidateToOutputRow(
  candidate: Candidate,
  targetHeader: string[],
  metadata: SchoolMetadata,
): string[] {
  return targetHeader.map((column) => {
    const normalized = normalizeHeader(column);

    if (normalized === normalizeHeader("ID")) return candidate.schoolId;
    if (normalized === normalizeHeader("teacher_first_name"))
      return candidate.firstName;
    if (normalized === normalizeHeader("teacher_last_name"))
      return candidate.lastName;
    if (normalized === normalizeHeader("teacher_role")) return candidate.role;
    if (normalized === normalizeHeader("teacher_email")) return candidate.email;
    if (normalized === normalizeHeader("teacher_source_url"))
      return candidate.sourceUrl;
    if (normalized === normalizeHeader("teacher_data_sources"))
      return candidate.dataSources;
    if (normalized === normalizeHeader("teacher_subject"))
      return candidate.subject;
    if (normalized === normalizeHeader("teacher_reasons"))
      return candidate.reasons;

    return metadata.values.get(normalized) ?? "";
  });
}

function takeSameSchoolCandidate(
  schoolId: string,
  candidatesBySchool: Map<string, Candidate[]>,
  positionsBySchool: Map<string, number>,
  usedKeys: Set<string>,
): Candidate | null {
  const candidates = candidatesBySchool.get(schoolId) ?? [];
  let position = positionsBySchool.get(schoolId) ?? 0;

  while (position < candidates.length) {
    const candidate = candidates[position];
    position += 1;
    if (candidate && !usedKeys.has(candidate.key)) {
      positionsBySchool.set(schoolId, position);
      return candidate;
    }
  }

  positionsBySchool.set(schoolId, position);
  return null;
}

function takeFallbackCandidate(
  removedSchoolId: string,
  candidates: Candidate[],
  usedKeys: Set<string>,
  metadataBySchool: Map<string, SchoolMetadata>,
): Candidate | null {
  for (const candidate of candidates) {
    if (candidate.schoolId === removedSchoolId) continue;
    if (usedKeys.has(candidate.key)) continue;
    if (!metadataBySchool.has(candidate.schoolId)) continue;
    return candidate;
  }

  return null;
}

function buildReportRows(
  replacements: Replacement[],
  targetIndexes: Map<string, number>,
): string[][] {
  const rows: string[][] = [
    [
      "replacement_number",
      "strategy",
      "removed_id",
      "removed_school_name",
      "removed_teacher_first_name",
      "removed_teacher_last_name",
      "removed_teacher_role",
      "replacement_id",
      "replacement_school_name",
      "replacement_teacher_first_name",
      "replacement_teacher_last_name",
      "replacement_teacher_role",
      "replacement_teacher_email",
      "replacement_source_file",
    ],
  ];

  replacements.forEach((replacement, index) => {
    const removed = replacement.removedRow;
    const added = replacement.replacementRow;
    rows.push([
      String(index + 1),
      replacement.strategy,
      getCell(removed, targetIndexes, "ID"),
      getCell(removed, targetIndexes, "School Name"),
      getCell(removed, targetIndexes, "teacher_first_name"),
      getCell(removed, targetIndexes, "teacher_last_name"),
      getCell(removed, targetIndexes, "teacher_role"),
      getCell(added, targetIndexes, "ID"),
      getCell(added, targetIndexes, "School Name"),
      getCell(added, targetIndexes, "teacher_first_name"),
      getCell(added, targetIndexes, "teacher_last_name"),
      getCell(added, targetIndexes, "teacher_role"),
      getCell(added, targetIndexes, "teacher_email"),
      relative(process.cwd(), replacement.candidate.sourceFile),
    ]);
  });

  return rows;
}

async function main(): Promise<number> {
  const args = parseArgs(Bun.argv.slice(2));
  const input = resolve(args.input);
  const removedInput = resolve(args.removedInput);
  const teachersRoot = resolve(args.teachersRoot);
  const output = args.inPlace ? input : resolve(args.output);
  const reportOutput = resolve(args.reportOutput);

  const inputRows = parseCsv(await Bun.file(input).text()).filter(
    (row) => !isBlankRow(row.fields),
  );
  const targetHeaderRow = inputRows[0];
  if (!targetHeaderRow) throw new Error(`No rows found in ${args.input}`);

  const targetHeader = targetHeaderRow.fields;
  const targetIndexes = getColumnIndexes(targetHeader);
  const currentRows = inputRows.slice(1).map((row) => row.fields);

  const removedRows = parseCsv(await Bun.file(removedInput).text()).filter(
    (row) => !isBlankRow(row.fields),
  );
  const removedHeaderRow = removedRows[0];
  if (!removedHeaderRow)
    throw new Error(`No rows found in ${args.removedInput}`);

  const removedIndexes = getColumnIndexes(removedHeaderRow.fields);
  const removedTargetRows = projectRemovedRows(
    removedRows.slice(1),
    removedIndexes,
    targetHeader,
  );
  const requestedReplacements = args.limit ?? removedTargetRows.length;

  const metadataBySchool = new Map<string, SchoolMetadata>();
  const blockedKeys = new Set<string>();

  for (const row of currentRows) {
    mergeMetadata(
      metadataBySchool,
      metadataFromTargetRow(targetHeader, row, targetIndexes),
    );
    blockedKeys.add(outputTeacherKey(row, targetIndexes));
  }

  for (const row of removedTargetRows) {
    mergeMetadata(
      metadataBySchool,
      metadataFromTargetRow(targetHeader, row, targetIndexes),
    );
    blockedKeys.add(outputTeacherKey(row, targetIndexes));
  }

  const candidates = await loadCandidates(teachersRoot, blockedKeys);
  const positionsBySchool = new Map<string, number>();
  const usedKeys = new Set<string>();
  const replacements: Replacement[] = [];
  let sameSchoolReplacements = 0;
  let fallbackReplacements = 0;
  let missedReplacements = 0;

  for (const removedRow of removedTargetRows.slice(0, requestedReplacements)) {
    const removedSchoolId = getCell(removedRow, targetIndexes, "ID").trim();
    let strategy: Replacement["strategy"] = "same_school";
    let candidate = takeSameSchoolCandidate(
      removedSchoolId,
      candidates.bySchool,
      positionsBySchool,
      usedKeys,
    );

    if (!candidate) {
      candidate = takeFallbackCandidate(
        removedSchoolId,
        candidates.all,
        usedKeys,
        metadataBySchool,
      );
      strategy = "fallback_school";
    }

    if (!candidate) {
      missedReplacements += 1;
      continue;
    }

    const metadata = metadataBySchool.get(candidate.schoolId);
    if (!metadata) {
      missedReplacements += 1;
      continue;
    }

    usedKeys.add(candidate.key);
    const replacementRow = candidateToOutputRow(
      candidate,
      targetHeader,
      metadata,
    );
    replacements.push({ strategy, removedRow, replacementRow, candidate });

    if (strategy === "same_school") sameSchoolReplacements += 1;
    else fallbackReplacements += 1;
  }

  console.log(`input: ${args.input}`);
  console.log(`removed_input: ${args.removedInput}`);
  console.log(`teachers_root: ${args.teachersRoot}`);
  console.log(`current_rows: ${currentRows.length}`);
  console.log(`removed_rows: ${removedTargetRows.length}`);
  console.log(`requested_replacements: ${requestedReplacements}`);
  console.log(`teacher_csv_files: ${candidates.csvFiles}`);
  console.log(`candidate_rows_scanned: ${candidates.totalRows}`);
  console.log(`candidate_rows_safe_available: ${candidates.acceptedRows}`);
  console.log(
    `candidate_rows_skipped_unwanted_role: ${candidates.unwantedRoleRows}`,
  );
  console.log(`candidate_rows_skipped_duplicate: ${candidates.duplicateRows}`);
  console.log(
    `candidate_rows_skipped_blank_role_or_name: ${candidates.blankRoleRows}`,
  );
  console.log(`same_school_replacements: ${sameSchoolReplacements}`);
  console.log(`fallback_school_replacements: ${fallbackReplacements}`);
  console.log(`missed_replacements: ${missedReplacements}`);
  console.log(
    `rows_after_replacement: ${currentRows.length + replacements.length}`,
  );

  if (args.strict && replacements.length < requestedReplacements) {
    throw new Error(
      `Only found ${replacements.length} replacements for ${requestedReplacements} requested rows`,
    );
  }

  if (args.dryRun) {
    console.log("dry_run: true");
    return 0;
  }

  await mkdir(dirname(output), { recursive: true });
  await mkdir(dirname(reportOutput), { recursive: true });
  await Bun.write(
    output,
    serializeCsv([
      targetHeader,
      ...currentRows,
      ...replacements.map((replacement) => replacement.replacementRow),
    ]),
  );
  await Bun.write(
    reportOutput,
    serializeCsv(buildReportRows(replacements, targetIndexes)),
  );

  console.log(`output: ${relative(process.cwd(), output)}`);
  console.log(`report_output: ${relative(process.cwd(), reportOutput)}`);

  return 0;
}

if (import.meta.main) {
  main().then(
    (code) => process.exit(code),
    (error: unknown) => {
      console.error(error instanceof Error ? error.message : error);
      process.exit(1);
    },
  );
}
