#!/usr/bin/env bun

import { readdir } from "node:fs/promises";
import { basename, extname, join, relative, resolve } from "node:path";

const DEFAULT_INPUT = "final_teachers_all.csv";
const DEFAULT_DIST_TEACHERS = "dist/teachers.csv";
const DEFAULT_TEACHERS_ROOT = "teachers";

type Args = {
  input: string;
  output: string;
  distTeachers: string;
  teachersRoot: string;
  dryRun: boolean;
};

type CsvRow = {
  fields: string[];
  line: number;
};

type SourceRecord = {
  labels: string;
  links: string;
  reasons: string;
  source: string;
  priority: number;
};

const ENRICHMENT_COLUMNS = ["teacher_labels", "teacher_links", "teacher_reasons"] as const;

function parseArgs(argv: string[]): Args {
  let input = DEFAULT_INPUT;
  let output = DEFAULT_INPUT;
  let distTeachers = DEFAULT_DIST_TEACHERS;
  let teachersRoot = DEFAULT_TEACHERS_ROOT;
  let dryRun = false;

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];

    if (arg === "--input" || arg === "-i") {
      const value = argv[i + 1];
      if (!value) throw new Error(`Missing value for ${arg}`);
      input = value;
      if (output === DEFAULT_INPUT) output = value;
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

    if (arg === "--dist-teachers") {
      const value = argv[i + 1];
      if (!value) throw new Error(`Missing value for ${arg}`);
      distTeachers = value;
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

    if (arg === "--dry-run") {
      dryRun = true;
      continue;
    }

    if (arg === "--help" || arg === "-h") {
      printUsage();
      process.exit(0);
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  return { input, output, distTeachers, teachersRoot, dryRun };
}

function printUsage(): void {
  console.log(`Usage: bun scripts/enrich_final_teacher_sources.ts [options]

Options:
  --input, -i <file>       Final teachers CSV. Default: ${DEFAULT_INPUT}
  --output, -o <file>      Output CSV. Default: overwrites --input
  --dist-teachers <file>   dist teacher CSV fallback. Default: ${DEFAULT_DIST_TEACHERS}
  --teachers-root <dir>    Canonical teacher source root. Default: ${DEFAULT_TEACHERS_ROOT}
  --dry-run                Print coverage without writing

Adds teacher_labels and teacher_links, and preserves/fills teacher_reasons from source rows.
Canonical source rows are teachers/{state}/{city}/{id}.top.csv; dist/teachers.csv is used as a fallback.`);
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
  return value.trim().replace(/^\uFEFF/, "").toLowerCase().replace(/[\s_-]+/g, " ");
}

function normalizeValue(value: string): string {
  return value.trim().toLowerCase().replace(/\s+/g, " ");
}

function getColumnIndexes(header: string[]): Map<string, number> {
  const indexes = new Map<string, number>();
  header.forEach((column, index) => indexes.set(normalizeHeader(column), index));
  return indexes;
}

function getCell(row: string[], indexes: Map<string, number>, column: string): string {
  const index = indexes.get(normalizeHeader(column));
  if (index === undefined) return "";
  return row[index] ?? "";
}

function getAnyCell(row: string[], indexes: Map<string, number>, columns: string[]): string {
  for (const column of columns) {
    const value = getCell(row, indexes, column);
    if (value.trim() !== "") return value;
  }
  return "";
}

function buildOutputHeader(header: string[]): string[] {
  const result: string[] = [];
  let inserted = false;

  for (const column of header) {
    const normalized = normalizeHeader(column);
    if (normalized === normalizeHeader("teacher_labels") || normalized === normalizeHeader("teacher_links")) continue;

    if (normalized === normalizeHeader("teacher_reasons")) {
      result.push("teacher_labels", "teacher_links", "teacher_reasons");
      inserted = true;
      continue;
    }

    result.push(column);
  }

  if (!inserted) result.push(...ENRICHMENT_COLUMNS);
  return result;
}

function rowToRecord(row: string[], header: string[]): Map<string, string> {
  const record = new Map<string, string>();
  header.forEach((column, index) => record.set(normalizeHeader(column), row[index] ?? ""));
  return record;
}

function recordToRow(record: Map<string, string>, header: string[]): string[] {
  return header.map((column) => record.get(normalizeHeader(column)) ?? "");
}

function sourceKeys(schoolId: string, firstName: string, lastName: string, email: string, role: string): string[] {
  const keys: string[] = [];
  const id = normalizeValue(schoolId);
  const first = normalizeValue(firstName);
  const last = normalizeValue(lastName);
  const normalizedEmail = normalizeValue(email);
  const normalizedRole = normalizeValue(role);

  if (!id) return keys;
  if (normalizedEmail) keys.push(`id-email:${id}:${normalizedEmail}`);
  if (first || last || normalizedRole) keys.push(`id-name-role:${id}:${first}:${last}:${normalizedRole}`);
  if (first || last) keys.push(`id-name:${id}:${first}:${last}`);
  return keys;
}

function mergeSource(existing: SourceRecord | undefined, incoming: SourceRecord): SourceRecord {
  if (!existing) return incoming;

  const preferIncoming = incoming.priority >= existing.priority;
  const primary = preferIncoming ? incoming : existing;
  const fallback = preferIncoming ? existing : incoming;

  return {
    labels: primary.labels || fallback.labels,
    links: primary.links || fallback.links,
    reasons: primary.reasons || fallback.reasons,
    source: primary.source,
    priority: primary.priority,
  };
}

function addSourceRecord(index: Map<string, SourceRecord>, keys: string[], record: SourceRecord): void {
  for (const key of keys) {
    index.set(key, mergeSource(index.get(key), record));
  }
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
      } else if (entry.isFile() && extname(entry.name).toLowerCase() === ".csv") {
        paths.push(path);
      }
    }
  }

  await walk(root);
  return paths.sort((a, b) => a.localeCompare(b));
}

function schoolIdFromTopPath(csvPath: string): string {
  const csvName = basename(csvPath, ".csv");
  return csvName.endsWith(".top") ? csvName.slice(0, -4) : csvName;
}

async function addDistSources(index: Map<string, SourceRecord>, distTeachers: string): Promise<number> {
  const rows = parseCsv(await Bun.file(distTeachers).text()).filter((row) => !isBlankRow(row.fields));
  const header = rows[0];
  if (!header) return 0;

  const indexes = getColumnIndexes(header.fields);
  let added = 0;

  for (const csvRow of rows.slice(1)) {
    const record: SourceRecord = {
      labels: getCell(csvRow.fields, indexes, "teacher_labels"),
      links: getCell(csvRow.fields, indexes, "teacher_links"),
      reasons: getAnyCell(csvRow.fields, indexes, ["teacher_reasons", "teache_reasons"]),
      source: distTeachers,
      priority: 1,
    };
    if (!record.labels && !record.links && !record.reasons) continue;

    const keys = sourceKeys(
      getCell(csvRow.fields, indexes, "ID"),
      getCell(csvRow.fields, indexes, "teacher_first_name"),
      getCell(csvRow.fields, indexes, "teacher_last_name"),
      getCell(csvRow.fields, indexes, "teacher_email"),
      getCell(csvRow.fields, indexes, "teacher_role"),
    );

    addSourceRecord(index, keys, record);
    added += 1;
  }

  return added;
}

async function addTopSources(index: Map<string, SourceRecord>, teachersRoot: string): Promise<{ files: number; rows: number }> {
  const csvPaths = await listCsvFiles(teachersRoot);
  let rowsAdded = 0;

  for (const csvPath of csvPaths) {
    if (!csvPath.endsWith(".top.csv")) continue;

    const rows = parseCsv(await Bun.file(csvPath).text()).filter((row) => !isBlankRow(row.fields));
    const header = rows[0];
    if (!header || rows.length <= 1) continue;

    const indexes = getColumnIndexes(header.fields);
    const schoolId = schoolIdFromTopPath(csvPath);

    for (const csvRow of rows.slice(1)) {
      const record: SourceRecord = {
        labels: getCell(csvRow.fields, indexes, "labels"),
        links: getCell(csvRow.fields, indexes, "links"),
        reasons: getCell(csvRow.fields, indexes, "reasons"),
        source: csvPath,
        priority: 2,
      };
      if (!record.labels && !record.links && !record.reasons) continue;

      const keys = sourceKeys(
        schoolId,
        getCell(csvRow.fields, indexes, "first_name"),
        getCell(csvRow.fields, indexes, "last_name"),
        getCell(csvRow.fields, indexes, "email"),
        getCell(csvRow.fields, indexes, "role"),
      );

      addSourceRecord(index, keys, record);
      rowsAdded += 1;
    }
  }

  return { files: csvPaths.filter((path) => path.endsWith(".top.csv")).length, rows: rowsAdded };
}

function findSource(index: Map<string, SourceRecord>, row: string[], indexes: Map<string, number>): SourceRecord | null {
  const keys = sourceKeys(
    getCell(row, indexes, "ID"),
    getCell(row, indexes, "teacher_first_name"),
    getCell(row, indexes, "teacher_last_name"),
    getCell(row, indexes, "teacher_email"),
    getCell(row, indexes, "teacher_role"),
  );

  for (const key of keys) {
    const source = index.get(key);
    if (source) return source;
  }

  return null;
}

async function main(): Promise<number> {
  const args = parseArgs(Bun.argv.slice(2));
  const input = resolve(args.input);
  const output = resolve(args.output);
  const distTeachers = resolve(args.distTeachers);
  const teachersRoot = resolve(args.teachersRoot);

  const sourceIndex = new Map<string, SourceRecord>();
  const distSourceRows = await addDistSources(sourceIndex, distTeachers);
  const topSources = await addTopSources(sourceIndex, teachersRoot);

  const inputRows = parseCsv(await Bun.file(input).text()).filter((row) => !isBlankRow(row.fields));
  const inputHeaderRow = inputRows[0];
  if (!inputHeaderRow) throw new Error(`No rows found in ${args.input}`);

  const inputHeader = inputHeaderRow.fields;
  const inputIndexes = getColumnIndexes(inputHeader);
  const outputHeader = buildOutputHeader(inputHeader);
  const outputRows: string[][] = [outputHeader];

  let matchedRows = 0;
  let matchedTopRows = 0;
  let labelsFilled = 0;
  let linksFilled = 0;
  let reasonsFilled = 0;
  let finalLabelsNonEmpty = 0;
  let finalLinksNonEmpty = 0;
  let finalReasonsNonEmpty = 0;

  for (const csvRow of inputRows.slice(1)) {
    const record = rowToRecord(csvRow.fields, inputHeader);
    const source = findSource(sourceIndex, csvRow.fields, inputIndexes);

    if (source) {
      matchedRows += 1;
      if (source.priority === 2) matchedTopRows += 1;

      if (!record.get(normalizeHeader("teacher_labels")) && source.labels) {
        record.set(normalizeHeader("teacher_labels"), source.labels);
        labelsFilled += 1;
      }

      if (!record.get(normalizeHeader("teacher_links")) && source.links) {
        record.set(normalizeHeader("teacher_links"), source.links);
        linksFilled += 1;
      }

      if (!record.get(normalizeHeader("teacher_reasons")) && source.reasons) {
        record.set(normalizeHeader("teacher_reasons"), source.reasons);
        reasonsFilled += 1;
      }
    }

    if ((record.get(normalizeHeader("teacher_labels")) ?? "").trim() !== "") finalLabelsNonEmpty += 1;
    if ((record.get(normalizeHeader("teacher_links")) ?? "").trim() !== "") finalLinksNonEmpty += 1;
    if ((record.get(normalizeHeader("teacher_reasons")) ?? "").trim() !== "") finalReasonsNonEmpty += 1;

    outputRows.push(recordToRow(record, outputHeader));
  }

  console.log(`input: ${relative(process.cwd(), input)}`);
  console.log(`output: ${relative(process.cwd(), output)}`);
  console.log(`dist_source_rows_loaded: ${distSourceRows}`);
  console.log(`top_csv_files_scanned: ${topSources.files}`);
  console.log(`top_source_rows_loaded: ${topSources.rows}`);
  console.log(`source_keys: ${sourceIndex.size}`);
  console.log(`input_rows: ${inputRows.length - 1}`);
  console.log(`matched_rows: ${matchedRows}`);
  console.log(`matched_rows_from_top_source: ${matchedTopRows}`);
  console.log(`teacher_labels_filled: ${labelsFilled}`);
  console.log(`teacher_links_filled: ${linksFilled}`);
  console.log(`teacher_reasons_filled: ${reasonsFilled}`);
  console.log(`teacher_labels_non_empty: ${finalLabelsNonEmpty}`);
  console.log(`teacher_links_non_empty: ${finalLinksNonEmpty}`);
  console.log(`teacher_reasons_non_empty: ${finalReasonsNonEmpty}`);

  if (args.dryRun) {
    console.log("dry_run: true");
    return 0;
  }

  await Bun.write(output, serializeCsv(outputRows));
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
