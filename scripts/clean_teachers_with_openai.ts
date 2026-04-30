#!/usr/bin/env bun

import { appendFile, mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";

const DEFAULT_INPUT = "dist/teachers.csv";
const DEFAULT_OUTPUT = "dist/teachers.cleaned.csv";
const DEFAULT_MODEL = "gpt-4.1-mini";
const DEFAULT_BATCH_SIZE = 3;
const DEFAULT_CONCURRENCY = 8;
const DEFAULT_MAX_RETRIES = 4;

const AUDIT_COLUMNS = [
  "cleaning_status",
  "cleaning_confidence",
  "cleaning_sources",
  "cleaning_changed_fields",
  "cleaning_notes",
  "cleaned_at",
] as const;

const CLEANABLE_COLUMNS = [
  "Name",
  "Street Address",
  "City",
  "State",
  "Zip",
  "Phone",
  "Website",
  "Website Final URL",
  "teacher_first_name",
  "teacher_last_name",
  "teacher_email",
  "teacher_role",
  "teacher_department",
  "teacher_source_url",
  "teacher_subject",
  "teacher_labels",
  "teacher_links",
  "teacher_reasons",
] as const;

let openAiPauseUntil = 0;

type Args = {
  input: string;
  output: string;
  model: string;
  batchSize: number;
  concurrency: number;
  startRow: number;
  limit: number | null;
  maxRetries: number;
  sleepMs: number;
  force: boolean;
  dryRun: boolean;
};

type CandidateRecord = {
  input_row_number: number;
  fields: Record<string, string>;
};

type CleanedRecord = {
  input_row_number: number;
  status: "verified" | "corrected" | "unverified" | "not_found" | "ambiguous";
  confidence: number;
  corrected_fields: { column: string; value: string }[];
  changed_fields: { column: string; old_value: string; new_value: string }[];
  sources: string[];
  notes: string;
};

type CleaningResponse = {
  records: CleanedRecord[];
};

type AppliedChange = {
  rowNumber: number;
  column: string;
  oldValue: string;
  newValue: string;
};

type BatchJob = {
  index: number;
  rowIndexes: number[];
};

type BatchResult = {
  index: number;
  rowIndexes: number[];
  cleanedRows: string[][];
  logLines: string[];
  processedRows: number;
  changedFields: number;
};

class OpenAiRequestError extends Error {
  constructor(message: string, readonly status: number, readonly retryAfterMs: number | null) {
    super(message);
  }
}

function parseArgs(argv: string[]): Args {
  let input = DEFAULT_INPUT;
  let output = DEFAULT_OUTPUT;
  let model = process.env.OPENAI_MODEL?.trim() || DEFAULT_MODEL;
  let batchSize = DEFAULT_BATCH_SIZE;
  let concurrency = parseConcurrency(process.env.OPENAI_CONCURRENCY?.trim() || String(DEFAULT_CONCURRENCY), "OPENAI_CONCURRENCY");
  let startRow = 1;
  let limit: number | null = null;
  let maxRetries = DEFAULT_MAX_RETRIES;
  let sleepMs = 0;
  let force = false;
  let dryRun = false;

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];

    if (arg === "--input" || arg === "-i") {
      input = readValue(argv, i, arg);
      i += 1;
      continue;
    }

    if (arg === "--output" || arg === "-o") {
      output = readValue(argv, i, arg);
      i += 1;
      continue;
    }

    if (arg === "--model") {
      model = readValue(argv, i, arg);
      i += 1;
      continue;
    }

    if (arg === "--batch-size") {
      batchSize = parsePositiveInteger(readValue(argv, i, arg), arg);
      i += 1;
      continue;
    }

    if (arg === "--concurrency" || arg === "-j") {
      concurrency = parseConcurrency(readValue(argv, i, arg), arg);
      i += 1;
      continue;
    }

    if (arg === "--start-row") {
      startRow = parsePositiveInteger(readValue(argv, i, arg), arg);
      i += 1;
      continue;
    }

    if (arg === "--limit") {
      limit = parsePositiveInteger(readValue(argv, i, arg), arg);
      i += 1;
      continue;
    }

    if (arg === "--max-retries") {
      maxRetries = parsePositiveInteger(readValue(argv, i, arg), arg);
      i += 1;
      continue;
    }

    if (arg === "--sleep-ms") {
      sleepMs = parseNonNegativeInteger(readValue(argv, i, arg), arg);
      i += 1;
      continue;
    }

    if (arg === "--force") {
      force = true;
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

  if (batchSize !== DEFAULT_BATCH_SIZE) {
    console.warn(`warning: --batch-size is ${batchSize}; requested production default is ${DEFAULT_BATCH_SIZE} records per OpenAI request.`);
  }

  if (resolve(input) === resolve(output)) {
    throw new Error("--output must be different from --input because cleaned rows are appended incrementally");
  }

  return { input, output, model, batchSize, concurrency, startRow, limit, maxRetries, sleepMs, force, dryRun };
}

function readValue(argv: string[], index: number, arg: string): string {
  const value = argv[index + 1];
  if (!value) throw new Error(`Missing value for ${arg}`);
  return value;
}

function parsePositiveInteger(value: string, arg: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) throw new Error(`${arg} must be a positive integer`);
  return parsed;
}

function parseConcurrency(value: string, arg: string): number {
  if (value.toLowerCase() === "max") return Number.MAX_SAFE_INTEGER;
  return parsePositiveInteger(value, arg);
}

function parseNonNegativeInteger(value: string, arg: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed < 0) throw new Error(`${arg} must be a non-negative integer`);
  return parsed;
}

function printUsage(): void {
  console.log(`Usage: bun scripts/clean_teachers_with_openai.ts [options]

Uses OpenAI Responses API web search to validate and correct ${DEFAULT_INPUT} in batches of 3 records.

Required environment:
  OPENAI_API_KEY                 OpenAI API key

Options:
  --input, -i <file>             Input CSV. Default: ${DEFAULT_INPUT}
  --output, -o <file>            Output CSV. Default: ${DEFAULT_OUTPUT}
  --model <model>                OpenAI model. Default: OPENAI_MODEL or ${DEFAULT_MODEL}
  --batch-size <n>               Records per OpenAI request. Default: ${DEFAULT_BATCH_SIZE}
  --concurrency, -j <n|max>      Parallel OpenAI requests. Default: OPENAI_CONCURRENCY or ${DEFAULT_CONCURRENCY}
  --start-row <n>                1-based data row to start at. Default: 1
  --limit <n>                    Maximum data rows to process
  --sleep-ms <n>                 Delay between requests
  --max-retries <n>              Retries per failed request. Default: ${DEFAULT_MAX_RETRIES}
  --force                        Recreate output instead of resuming it
  --dry-run                      Call OpenAI and print proposed results without writing changes

The output keeps the original columns, applies supported corrections, and adds cleaning_* audit columns.
If the output already exists, the script resumes after the rows already written unless --force is used.`);
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

function escapeCsvField(value: string): string {
  if (/[",\n\r]/.test(value)) return `"${value.replace(/"/g, '""')}"`;
  return value;
}

function serializeCsv(rows: string[][]): string {
  return rows.map((row) => row.map((field) => escapeCsvField(field ?? "")).join(",")).join("\n") + "\n";
}

function normalizeHeader(value: string): string {
  return value.trim().replace(/^\uFEFF/, "").toLowerCase();
}

function buildIndexes(header: string[]): Map<string, number> {
  const indexes = new Map<string, number>();
  header.forEach((column, index) => indexes.set(normalizeHeader(column), index));
  return indexes;
}

function getRequiredIndex(indexes: Map<string, number>, column: string): number {
  const index = indexes.get(normalizeHeader(column));
  if (index === undefined) throw new Error(`No ${column} column found`);
  return index;
}

function ensureAuditColumns(rows: string[][]): void {
  const header = rows[0];
  if (!header) throw new Error("CSV has no header row");

  const existing = new Set(header.map(normalizeHeader));
  for (const column of AUDIT_COLUMNS) {
    if (existing.has(normalizeHeader(column))) continue;
    header.push(column);
    for (const row of rows.slice(1)) row.push("");
  }
}

function ensureRowWidth(rows: string[][]): void {
  const headerLength = rows[0]?.length ?? 0;
  for (const row of rows.slice(1)) {
    while (row.length < headerLength) row.push("");
  }
}

function rowToObject(header: string[], row: string[]): Record<string, string> {
  const object: Record<string, string> = {};
  for (let i = 0; i < header.length; i += 1) {
    const column = header[i];
    if (!column || AUDIT_COLUMNS.includes(column as (typeof AUDIT_COLUMNS)[number])) continue;
    object[column] = row[i] ?? "";
  }
  return object;
}

function candidateRows(rows: string[][], args: Args, alreadyWrittenRows: number): number[] {
  const header = rows[0];
  if (!header) return [];

  const indexes = buildIndexes(header);
  const cleanedAtIndex = getRequiredIndex(indexes, "cleaned_at");
  const remainingLimit = args.limit === null ? Number.POSITIVE_INFINITY : args.limit - alreadyWrittenRows;
  if (remainingLimit <= 0) return [];

  const startIndex = args.startRow + alreadyWrittenRows;
  const endExclusive = Math.min(rows.length, startIndex + remainingLimit);
  const candidates: number[] = [];

  for (let rowIndex = startIndex; rowIndex < endExclusive; rowIndex += 1) {
    const row = rows[rowIndex];
    if (!row || row.every((cell) => cell.trim() === "")) continue;
    if ((row[cleanedAtIndex] ?? "").trim() !== "") continue;
    candidates.push(rowIndex);
  }

  return candidates;
}

function buildBatch(header: string[], rows: string[][], rowIndexes: number[]): CandidateRecord[] {
  return rowIndexes.map((rowIndex) => ({
    input_row_number: rowIndex,
    fields: rowToObject(header, rows[rowIndex] ?? []),
  }));
}

function buildInstructions(): string {
  return `You clean a CSV of US school teacher records.

For each record, use web search. Search the teacher name with the school name, city, state, staff directory, role, email, and school website. Verify whether the teacher exists and whether the school data is plausible.

Rules:
- Return one result for every input record using the same input_row_number.
- Correct clear mistakes in teacher name, school name, school website, role, subject, department, email, address, phone, city/state/zip, and source/link fields when reliable evidence supports the correction.
- Prefer official school, district, staff directory, board, PDF, or teacher page sources. Use third-party pages only as secondary evidence.
- Do not invent missing data. Leave a field unchanged if evidence is weak or conflicting.
- Do not remove a teacher just because web search is inconclusive. Use status unverified or ambiguous.
- Use status not_found only when searches strongly suggest the person is not associated with the school or the record appears to identify the wrong person.
- Keep corrected_fields limited to the provided CSV column names.
- Keep changed_fields limited to fields you changed, with old_value and new_value.
- Put source URLs used for the decision in sources.
- Notes should be concise and factual.`;
}

function buildPrompt(records: CandidateRecord[]): string {
  const cleanable = CLEANABLE_COLUMNS.join(", ");
  return `Clean these ${records.length} teacher CSV records.

Only these columns may be changed: ${cleanable}

Records:
${JSON.stringify(records, null, 2)}`;
}

function responseSchema(): Record<string, unknown> {
  return {
    type: "object",
    additionalProperties: false,
    required: ["records"],
    properties: {
      records: {
        type: "array",
        minItems: 1,
        items: {
          type: "object",
          additionalProperties: false,
          required: [
            "input_row_number",
            "status",
            "confidence",
            "corrected_fields",
            "changed_fields",
            "sources",
            "notes",
          ],
          properties: {
            input_row_number: { type: "integer" },
            status: { type: "string", enum: ["verified", "corrected", "unverified", "not_found", "ambiguous"] },
            confidence: { type: "number", minimum: 0, maximum: 1 },
            corrected_fields: {
              type: "array",
              items: {
                type: "object",
                additionalProperties: false,
                required: ["column", "value"],
                properties: {
                  column: { type: "string" },
                  value: { type: "string" },
                },
              },
            },
            changed_fields: {
              type: "array",
              items: {
                type: "object",
                additionalProperties: false,
                required: ["column", "old_value", "new_value"],
                properties: {
                  column: { type: "string" },
                  old_value: { type: "string" },
                  new_value: { type: "string" },
                },
              },
            },
            sources: {
              type: "array",
              items: { type: "string" },
            },
            notes: { type: "string" },
          },
        },
      },
    },
  };
}

async function callOpenAi(args: Args, records: CandidateRecord[]): Promise<CleaningResponse> {
  const apiKey = process.env.OPENAI_API_KEY?.trim();
  if (!apiKey) throw new Error("Missing OPENAI_API_KEY in environment");

  const body = {
    model: args.model,
    instructions: buildInstructions(),
    input: buildPrompt(records),
    tools: [{ type: "web_search_preview" }],
    text: {
      format: {
        type: "json_schema",
        name: "teacher_cleaning_batch",
        strict: true,
        schema: responseSchema(),
      },
    },
  };

  const response = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  const text = await response.text();
  if (!response.ok) {
    throw new OpenAiRequestError(
      `OpenAI request failed (${response.status}): ${text.slice(0, 1000)}`,
      response.status,
      retryDelayFromHeaders(response.headers),
    );
  }

  const payload = JSON.parse(text) as Record<string, unknown>;
  return parseCleaningResponse(payload);
}

function parseCleaningResponse(payload: Record<string, unknown>): CleaningResponse {
  const directText = typeof payload.output_text === "string" ? payload.output_text : "";
  const extractedText = directText || extractResponseText(payload);
  if (!extractedText) throw new Error("OpenAI response did not include output text");
  return parseLooseJson<CleaningResponse>(extractedText);
}

function extractResponseText(payload: Record<string, unknown>): string {
  const output = payload.output;
  if (!Array.isArray(output)) return "";

  const parts: string[] = [];
  for (const item of output) {
    if (!item || typeof item !== "object") continue;
    const content = (item as { content?: unknown }).content;
    if (!Array.isArray(content)) continue;

    for (const contentItem of content) {
      if (!contentItem || typeof contentItem !== "object") continue;
      const typed = contentItem as { text?: unknown; type?: unknown };
      if (typeof typed.text === "string") parts.push(typed.text);
    }
  }

  return parts.join("\n").trim();
}

function parseLooseJson<T>(raw: string): T {
  const stripped = raw
    .replace(/^\s*```(?:json)?\s*\n?/i, "")
    .replace(/\n?```\s*$/i, "")
    .trim();

  try {
    return JSON.parse(stripped) as T;
  } catch {}

  const firstObject = stripped.indexOf("{");
  const firstArray = stripped.indexOf("[");
  const firstStart = firstObject === -1 ? firstArray : firstArray === -1 ? firstObject : Math.min(firstObject, firstArray);
  if (firstStart !== -1) {
    const opener = stripped[firstStart];
    const closer = opener === "{" ? "}" : "]";
    const lastEnd = stripped.lastIndexOf(closer);
    if (lastEnd > firstStart) return JSON.parse(stripped.slice(firstStart, lastEnd + 1)) as T;
  }

  throw new Error(`Failed to parse JSON from OpenAI output: ${stripped.slice(0, 500)}`);
}

async function callOpenAiWithRetries(args: Args, records: CandidateRecord[]): Promise<CleaningResponse> {
  if (!process.env.OPENAI_API_KEY?.trim()) throw new Error("Missing OPENAI_API_KEY in environment");

  let attempt = 0;
  let delayMs = 2_000;

  while (true) {
    try {
      await waitForOpenAiPause();
      return await callOpenAi(args, records);
    } catch (error) {
      if (error instanceof OpenAiRequestError && error.status !== 429 && error.status < 500) throw error;

      attempt += 1;
      if (attempt > args.maxRetries) throw error;
      const message = error instanceof Error ? error.message : String(error);
      const retryDelayMs = error instanceof OpenAiRequestError && error.retryAfterMs !== null ? error.retryAfterMs : delayMs;
      if (error instanceof OpenAiRequestError && error.status === 429) pauseOpenAiRequests(retryDelayMs);
      console.warn(`OpenAI request failed; retrying in ${retryDelayMs}ms (${attempt}/${args.maxRetries}): ${message}`);
      await sleep(retryDelayMs);
      delayMs = Math.min(delayMs * 2, 60_000);
    }
  }
}

async function waitForOpenAiPause(): Promise<void> {
  const delayMs = openAiPauseUntil - Date.now();
  if (delayMs > 0) await sleep(delayMs);
}

function pauseOpenAiRequests(delayMs: number): void {
  openAiPauseUntil = Math.max(openAiPauseUntil, Date.now() + delayMs);
}

function retryDelayFromHeaders(headers: Headers): number | null {
  return parseRetryAfter(headers.get("retry-after"))
    ?? parseRateLimitReset(headers.get("x-ratelimit-reset-requests"))
    ?? parseRateLimitReset(headers.get("x-ratelimit-reset-tokens"));
}

function parseRetryAfter(value: string | null): number | null {
  if (!value) return null;

  const seconds = Number(value);
  if (Number.isFinite(seconds)) return Math.max(0, Math.ceil(seconds * 1000));

  const dateMs = Date.parse(value);
  if (Number.isFinite(dateMs)) return Math.max(0, dateMs - Date.now());

  return null;
}

function parseRateLimitReset(value: string | null): number | null {
  if (!value) return null;

  const trimmed = value.trim().toLowerCase();
  const match = trimmed.match(/^(\d+(?:\.\d+)?)(ms|s|m)?$/);
  if (!match) return null;

  const amount = Number(match[1]);
  if (!Number.isFinite(amount)) return null;

  const unit = match[2] ?? "s";
  if (unit === "ms") return Math.ceil(amount);
  if (unit === "m") return Math.ceil(amount * 60_000);
  return Math.ceil(amount * 1000);
}

function assertOpenAiApiKey(): void {
  if (!process.env.OPENAI_API_KEY?.trim()) throw new Error("Missing OPENAI_API_KEY in environment");
}

function applyCleaningResult(header: string[], row: string[], result: CleanedRecord, cleanedAt: string): AppliedChange[] {
  const indexes = buildIndexes(header);
  const cleanable = new Set(CLEANABLE_COLUMNS.map(normalizeHeader));
  const changes: AppliedChange[] = [];

  for (const correction of result.corrected_fields ?? []) {
    const column = correction.column;
    const value = correction.value;
    const normalized = normalizeHeader(column);
    const index = indexes.get(normalized);
    if (index === undefined || !cleanable.has(normalized)) continue;

    const oldValue = row[index] ?? "";
    const nextValue = String(value ?? "").trim();
    if (oldValue === nextValue) continue;
    row[index] = nextValue;
    changes.push({
      rowNumber: result.input_row_number,
      column: header[index] ?? column,
      oldValue,
      newValue: nextValue,
    });
  }

  row[getRequiredIndex(indexes, "cleaning_status")] = result.status;
  row[getRequiredIndex(indexes, "cleaning_confidence")] = clampConfidence(result.confidence).toFixed(2);
  row[getRequiredIndex(indexes, "cleaning_sources")] = uniqueStrings(result.sources ?? []).join(";");
  row[getRequiredIndex(indexes, "cleaning_changed_fields")] = JSON.stringify(changes.map((change) => ({
    column: change.column,
    old_value: change.oldValue,
    new_value: change.newValue,
  })));
  row[getRequiredIndex(indexes, "cleaning_notes")] = result.notes ?? "";
  row[getRequiredIndex(indexes, "cleaned_at")] = cleanedAt;

  return changes;
}

function appliedChangeLines(header: string[], row: string[], result: CleanedRecord, changes: AppliedChange[]): string[] {
  if (changes.length === 0) return [];

  const label = describeRow(header, row);
  const lines = [`changes for row ${result.input_row_number}${label ? ` (${label})` : ""}: status=${result.status} confidence=${clampConfidence(result.confidence).toFixed(2)}`];

  for (const change of changes) {
    lines.push(`  ${change.column}: ${formatValue(change.oldValue)} -> ${formatValue(change.newValue)}`);
  }

  if (result.notes.trim() !== "") lines.push(`  notes: ${result.notes.trim()}`);
  return lines;
}

function describeRow(header: string[], row: string[]): string {
  const school = getOptionalCell(header, row, "Name");
  const firstName = getOptionalCell(header, row, "teacher_first_name");
  const lastName = getOptionalCell(header, row, "teacher_last_name");
  const teacher = [firstName, lastName].filter(Boolean).join(" ");
  return [teacher, school].filter(Boolean).join(" at ");
}

function getOptionalCell(header: string[], row: string[], column: string): string {
  const index = buildIndexes(header).get(normalizeHeader(column));
  if (index === undefined) return "";
  return (row[index] ?? "").trim();
}

function formatValue(value: string): string {
  const formatted = value === "" ? "<blank>" : value;
  return JSON.stringify(formatted.length > 180 ? `${formatted.slice(0, 177)}...` : formatted);
}

function clampConfidence(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(1, value));
}

function uniqueStrings(values: string[]): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const value of values) {
    const trimmed = String(value ?? "").trim();
    if (!trimmed || seen.has(trimmed)) continue;
    seen.add(trimmed);
    result.push(trimmed);
  }
  return result;
}

function validateCleaningResponse(response: CleaningResponse, rowIndexes: number[]): void {
  if (!Array.isArray(response.records)) throw new Error("OpenAI response is missing records array");

  const expected = new Set(rowIndexes);
  const seen = new Set<number>();
  for (const record of response.records) {
    if (!expected.has(record.input_row_number)) {
      throw new Error(`OpenAI returned unexpected input_row_number ${record.input_row_number}`);
    }
    if (seen.has(record.input_row_number)) {
      throw new Error(`OpenAI returned duplicate input_row_number ${record.input_row_number}`);
    }
    seen.add(record.input_row_number);
  }

  for (const rowIndex of expected) {
    if (!seen.has(rowIndex)) throw new Error(`OpenAI response omitted input_row_number ${rowIndex}`);
  }
}

async function prepareRows(args: Args): Promise<string[][]> {
  const input = resolve(args.input);
  const content = await Bun.file(input).text();
  const rows = parseCsv(content);

  if (!rows[0]) throw new Error(`No rows found in ${input}`);
  ensureAuditColumns(rows);
  ensureRowWidth(rows);

  return rows;
}

async function existingOutputRows(args: Args, expectedHeader: string[]): Promise<number> {
  if (args.force || args.dryRun) return 0;

  const output = resolve(args.output);
  if (!await Bun.file(output).exists()) return 0;

  const rows = parseCsv(await Bun.file(output).text());
  const header = rows[0];
  if (!header) return 0;

  if (!headersMatch(header, expectedHeader)) {
    throw new Error(`Existing output header does not match ${args.input}. Use --force to recreate ${args.output}.`);
  }

  return rows.slice(1).filter((row) => row.some((cell) => cell.trim() !== "")).length;
}

function headersMatch(left: string[], right: string[]): boolean {
  if (left.length !== right.length) return false;
  return left.every((column, index) => normalizeHeader(column) === normalizeHeader(right[index] ?? ""));
}

async function initializeOutput(args: Args, header: string[]): Promise<void> {
  if (args.dryRun) return;

  const output = resolve(args.output);
  if (!args.force && await Bun.file(output).exists()) return;

  await mkdir(dirname(output), { recursive: true });
  await Bun.write(output, serializeCsv([header]));
}

async function appendCleanedRows(path: string, rows: string[][]): Promise<void> {
  if (rows.length === 0) return;
  await appendFile(resolve(path), serializeCsv(rows));
}

function buildBatchJobs(pendingRows: number[], batchSize: number): BatchJob[] {
  const jobs: BatchJob[] = [];
  for (let offset = 0; offset < pendingRows.length; offset += batchSize) {
    jobs.push({
      index: jobs.length,
      rowIndexes: pendingRows.slice(offset, offset + batchSize),
    });
  }
  return jobs;
}

async function processBatchJob(args: Args, header: string[], rows: string[][], job: BatchJob): Promise<BatchResult> {
  const batch = buildBatch(header, rows, job.rowIndexes);
  const logLines = [`cleaning rows ${job.rowIndexes.join(", ")} (batch ${job.index + 1})`];
  const response = await callOpenAiWithRetries(args, batch);
  validateCleaningResponse(response, job.rowIndexes);

  if (args.dryRun) {
    logLines.push(JSON.stringify(response, null, 2));
    return {
      index: job.index,
      rowIndexes: job.rowIndexes,
      cleanedRows: [],
      logLines,
      processedRows: job.rowIndexes.length,
      changedFields: 0,
    };
  }

  const cleanedAt = new Date().toISOString();
  const byRow = new Map(response.records.map((record) => [record.input_row_number, record]));
  const cleanedRows: string[][] = [];
  let changedFields = 0;

  for (const rowIndex of job.rowIndexes) {
    const row = rows[rowIndex] ?? [];
    const result = byRow.get(rowIndex);
    if (!result) continue;

    const appliedChanges = applyCleaningResult(header, row, result, cleanedAt);
    changedFields += appliedChanges.length;
    logLines.push(...appliedChangeLines(header, row, result, appliedChanges));
    cleanedRows.push(row);
  }

  if (changedFields === 0) logLines.push("changes: none");

  return {
    index: job.index,
    rowIndexes: job.rowIndexes,
    cleanedRows,
    logLines,
    processedRows: job.rowIndexes.length,
    changedFields,
  };
}

async function runConcurrentBatchJobs(
  args: Args,
  header: string[],
  rows: string[][],
  jobs: BatchJob[],
  onResult: (result: BatchResult) => Promise<void>,
): Promise<void> {
  let nextJobIndex = 0;
  let firstError: unknown = null;
  const workerCount = Math.min(args.concurrency, jobs.length);

  async function worker(workerIndex: number): Promise<void> {
    while (true) {
      if (firstError) return;

      const job = jobs[nextJobIndex];
      nextJobIndex += 1;
      if (!job) return;

      try {
        const result = await processBatchJob(args, header, rows, job);
        await onResult(result);
      } catch (error) {
        firstError = error;
        const label = error instanceof Error ? error.message : String(error);
        console.error(`worker ${workerIndex + 1} failed on batch ${job.index + 1}: ${label}`);
        return;
      }

      await sleep(args.sleepMs);
    }
  }

  await Promise.all(Array.from({ length: workerCount }, (_, index) => worker(index)));
  if (firstError) throw firstError;
}

async function sleep(ms: number): Promise<void> {
  if (ms <= 0) return;
  await new Promise((resolveSleep) => setTimeout(resolveSleep, ms));
}

async function main(): Promise<number> {
  const args = parseArgs(Bun.argv.slice(2));
  const rows = await prepareRows(args);
  const header = rows[0];
  if (!header) throw new Error("CSV has no header row");

  const alreadyWrittenRows = await existingOutputRows(args, header);
  const pendingRows = candidateRows(rows, args, alreadyWrittenRows);
  if (pendingRows.length === 0) {
    console.log("No pending rows to clean.");
    return 0;
  }

  console.log(`input: ${args.input}`);
  console.log(`output: ${args.output}`);
  console.log(`model: ${args.model}`);
  console.log(`already_written_rows: ${alreadyWrittenRows}`);
  console.log(`pending_rows: ${pendingRows.length}`);
  console.log(`batch_size: ${args.batchSize}`);
  console.log(`concurrency: ${args.concurrency === Number.MAX_SAFE_INTEGER ? "max" : args.concurrency}`);

  assertOpenAiApiKey();
  await initializeOutput(args, header);

  const jobs = buildBatchJobs(pendingRows, args.batchSize);
  const completed = new Map<number, BatchResult>();
  let nextFlushIndex = 0;
  let processed = 0;
  let changed = 0;
  let flushQueue = Promise.resolve();

  await runConcurrentBatchJobs(args, header, rows, jobs, async (result) => {
    flushQueue = flushQueue.then(async () => {
      completed.set(result.index, result);

      while (completed.has(nextFlushIndex)) {
        const next = completed.get(nextFlushIndex);
        if (!next) break;

        completed.delete(nextFlushIndex);
        for (const line of next.logLines) console.log(line);

        if (!args.dryRun) await appendCleanedRows(args.output, next.cleanedRows);

        processed += next.processedRows;
        changed += next.changedFields;
        console.log(`progress: ${processed}/${pendingRows.length} rows, changed_fields=${changed}`);
        nextFlushIndex += 1;
      }
    });

    await flushQueue;
  });

  await flushQueue;

  console.log(`processed_rows: ${processed}`);
  console.log(`changed_fields: ${changed}`);
  if (args.dryRun) console.log("dry_run: true");

  return 0;
}

main().then(
  (code) => process.exit(code),
  (error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  },
);
