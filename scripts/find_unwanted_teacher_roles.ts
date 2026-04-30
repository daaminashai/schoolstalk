#!/usr/bin/env bun

import { resolve } from "node:path";

const DEFAULT_INPUT = "teachers_zach.csv";
const DEFAULT_OUTPUT = "unwanted_teacher_roles.csv";
const DEFAULT_SUMMARY_OUTPUT = "unwanted_teacher_role_signals.csv";

type Args = {
  input: string;
  output: string;
  summaryOutput: string;
  removeFromInput: boolean;
};

type CsvRow = {
  fields: string[];
  line: number;
};

type SignalRule = {
  id: string;
  label: string;
  pattern: RegExp;
};

type RoleSignal = {
  id: string;
  label: string;
  matches: string[];
};

type RejectedRoleRow = {
  line: number;
  role: string;
  signals: RoleSignal[];
  fields: string[];
};

type SignalSummary = {
  id: string;
  label: string;
  count: number;
  matchedTerms: Set<string>;
  exampleRoles: Set<string>;
};

const SIGNAL_RULES: SignalRule[] = [
  {
    id: "non_teaching_technology_support",
    label: "non-teaching technology/support title",
    pattern:
      /\b(?:information\s+systems?|computer\s+network|network\s+technician|technician|technology\s+aides?|tech\s+aides?|(?:information\s+)?technology\s+support\s+specialists?|computer\s+support\s+specialists?|computer\s+systems?\s+specialists?|software(?:\/hr)?\s+specialists?|technology\s+speci(?:al|la)ists?|tech\s+specialists?|it\s+specialists?|instructional\s+tech\s+specialists?|digital\s+(?:learning|teaching\s+&\s+learning)\s+specialists?|technology\s+department|tech(?:nology)?\s+department|technology\s+integration\s+specialist|tech\s+integration\s+specialist|webmaster|social\s+media|fixed\s+assets?|one\s+to\s+one\s+plus|help\s*desk|it\s+support)\b/gi,
  },
  {
    id: "administrative_leadership",
    label: "administrative/leadership title",
    pattern:
      /\b(?:directors?|principals?|principles?|deans?|headmasters?|superintendents?|chief\s+(?:technology|information|operations|financial|academic|innovation|strategy|data)\s+officers?|cto|administrators?|administration|formula\s+grants?|supervisors?|managers?)\b/gi,
  },
  {
    id: "coordinator_facilitator_or_coach",
    label: "coordinator/facilitator/coach title",
    pattern:
      /\b(?:coordinators?|facilitators?|curriculum\s+specialists?|curriculum\s+coordinators?|instructional\s+coach(?:es)?|math\s+coach(?:es)?|science\s+coach(?:es)?|stem\s+coach(?:es)?|academic\s+coach(?:es)?)\b/gi,
  },
  {
    id: "support_staff_title",
    label: "support staff title",
    pattern:
      /\b(?:(?:computer|technology|instructional|teacher|classroom|office|library|lab)\s+aides?|assistant\s+(?:directors?|principals?|deans?|technology\s+coordinators?|coordinators?|administrators?|managers?)|paraprofessionals?|paraeducators?|secretar(?:y|ies)|clerks?|registrars?|office\s+staff|bookkeepers?)\b/gi,
  },
  {
    id: "student_services_or_library",
    label: "student services/library title, not classroom teacher",
    pattern:
      /\b(?:counselors?|librarians?|library\s+media|media\s+specialists?|social\s+workers?|psychologists?|nurses?|therapists?|speech\s+language\s+pathologists?)\b/gi,
  },
];

const CLASSROOM_TEACHING_PATTERN = /\b(?:teacher|instructor|educator|faculty)\b/i;

const GRADE_TOKEN_PATTERN =
  "(?:pre[-\\s]?k|prek|pk|tk|kindergarten|kinder|k|first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|eleventh|twelfth|1[0-2]|[1-9])(?:st|nd|rd|th)?";
const EIGHTH_AND_BELOW_GRADE_TOKEN_PATTERN =
  "(?:pre[-\\s]?k|prek|pk|tk|kindergarten|kinder|k|first|second|third|fourth|fifth|sixth|seventh|eighth|[1-8](?:st|nd|rd|th)?)";
const EIGHTH_AND_BELOW_STANDALONE_GRADE_TOKEN_PATTERN =
  "(?:pre[-\\s]?k|prek|pk|tk|kindergarten|kinder|k|first|second|third|fourth|fifth|sixth|seventh|eighth|[1-8](?:st|nd|rd|th))";
const GRADE_ROLE_CONTEXT_PATTERN =
  "(?:math(?:ematics)?|science|stem|steam|ela|english|language\\s+arts|algebra(?:\\s+i{1,3})?|computing|computer(?:\\s+(?:literacy|science|lab))?|lab|technology|engineering|physical\\s+science|physical\\s+ed(?:ucation)?|health|project\\s+lead\\s+the\\s+way|social\\s+studies|study\\s+skills|teacher|instructor|educator)";
const GRADE_LIST_SEPARATOR_PATTERN = "(?:\\s*(?:-|–|—|/|,|&)\\s*|\\s+and\\s+|\\s*-\\s*and\\s*)";
const EIGHTH_AND_BELOW_GRADE_PATTERNS = [
  new RegExp(`\\b(${EIGHTH_AND_BELOW_GRADE_TOKEN_PATTERN})\\s*(?:-|–|—)?\\s*(?:grade|grades|gr\\.?)\\b`, "gi"),
  new RegExp(`\\b(?:grade|grades|gr\\.?)\\s*(?:-|–|—)?\\s*(${EIGHTH_AND_BELOW_GRADE_TOKEN_PATTERN})\\b`, "gi"),
  new RegExp(
    `\\b(${EIGHTH_AND_BELOW_STANDALONE_GRADE_TOKEN_PATTERN}(?:${GRADE_LIST_SEPARATOR_PATTERN}${EIGHTH_AND_BELOW_STANDALONE_GRADE_TOKEN_PATTERN})+)\\s*(?:-|–|—)?\\s*(?:grade|grades|gr\\.?|${GRADE_ROLE_CONTEXT_PATTERN})\\b`,
    "gi",
  ),
  new RegExp(
    `\\b(${EIGHTH_AND_BELOW_STANDALONE_GRADE_TOKEN_PATTERN})\\s*(?:-|–|—|/)?\\s*(?:${GRADE_ROLE_CONTEXT_PATTERN})\\b`,
    "gi",
  ),
  new RegExp(
    `\\b(?:${GRADE_ROLE_CONTEXT_PATTERN})\\s*(?:-|–|—|/|,)?\\s*(${EIGHTH_AND_BELOW_STANDALONE_GRADE_TOKEN_PATTERN})\\b`,
    "gi",
  ),
  new RegExp(`\\b(${EIGHTH_AND_BELOW_STANDALONE_GRADE_TOKEN_PATTERN})\\s*(?:/|&)\\s*(?:hs|high\\s+school)\\b`, "gi"),
  /\b(?:elementary(?:\s+schools?)?|primary|middle\s+schools?)\b/gi,
];
const GRADE_RANGE_PATTERN = new RegExp(
  `\\b(${GRADE_TOKEN_PATTERN})\\s*(?:-|–|—|to|through|thru|/)\\s*(${GRADE_TOKEN_PATTERN})\\s*(?:grade|grades|gr\\.?)?\\b`,
  "gi",
);

const GRADE_WORD_VALUES = new Map<string, number>([
  ["pre-k", 0],
  ["pre k", 0],
  ["prek", 0],
  ["pk", 0],
  ["tk", 0],
  ["kindergarten", 0],
  ["kinder", 0],
  ["k", 0],
  ["first", 1],
  ["second", 2],
  ["third", 3],
  ["fourth", 4],
  ["fifth", 5],
  ["sixth", 6],
  ["seventh", 7],
  ["eighth", 8],
  ["ninth", 9],
  ["tenth", 10],
  ["eleventh", 11],
  ["twelfth", 12],
]);

function parseArgs(argv: string[]): Args {
  let input = DEFAULT_INPUT;
  let output = DEFAULT_OUTPUT;
  let summaryOutput = DEFAULT_SUMMARY_OUTPUT;
  let removeFromInput = false;

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

    if (arg === "--summary-output") {
      const value = argv[i + 1];
      if (!value) throw new Error(`Missing value for ${arg}`);
      summaryOutput = value;
      i += 1;
      continue;
    }

    if (arg === "--remove-from-input") {
      removeFromInput = true;
      continue;
    }

    if (arg === "--help" || arg === "-h") {
      printUsage();
      process.exit(0);
    }

    throw new Error(`Unknown argument: ${arg}`);
  }

  return { input, output, summaryOutput, removeFromInput };
}

function printUsage(): void {
  console.log(`Usage: bun scripts/find_unwanted_teacher_roles.ts [--input teachers_zach.csv] [--output unwanted_teacher_roles.csv] [--summary-output unwanted_teacher_role_signals.csv] [--remove-from-input]

Reads teacher_role locally and writes:
- a detailed CSV containing rows with unwanted role signals
- a summary CSV showing the most common rejection signals

Use --remove-from-input to overwrite the input CSV with flagged records removed.

No API calls or network access are used.`);
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

function collectPatternMatches(value: string, pattern: RegExp): string[] {
  const flags = pattern.flags.includes("g") ? pattern.flags : `${pattern.flags}g`;
  const regex = new RegExp(pattern.source, flags);
  const matches = Array.from(value.matchAll(regex), (match) => match[0].trim()).filter(Boolean);
  return Array.from(new Set(matches));
}

function normalizeGradeToken(value: string): string {
  return value
    .toLowerCase()
    .replace(/\./g, "")
    .replace(/\s+/g, " ")
    .replace(/^(\d+)(?:st|nd|rd|th)$/, "$1")
    .trim();
}

function gradeTokenToNumber(value: string): number | null {
  const normalized = normalizeGradeToken(value);
  const wordValue = GRADE_WORD_VALUES.get(normalized);
  if (wordValue !== undefined) return wordValue;

  const numeric = Number.parseInt(normalized, 10);
  return Number.isNaN(numeric) ? null : numeric;
}

function matchEighthAndBelowGrade(role: string): string[] {
  const matches: string[] = [];

  for (const pattern of EIGHTH_AND_BELOW_GRADE_PATTERNS) {
    matches.push(...collectPatternMatches(role, pattern));
  }

  const rangeRegex = new RegExp(GRADE_RANGE_PATTERN.source, GRADE_RANGE_PATTERN.flags);
  for (const match of role.matchAll(rangeRegex)) {
    const lowGrade = gradeTokenToNumber(match[1] ?? "");
    const highGrade = gradeTokenToNumber(match[2] ?? "");

    if ((lowGrade !== null && lowGrade <= 8) || (highGrade !== null && highGrade <= 8)) {
      matches.push(match[0].trim());
    }
  }

  return Array.from(new Set(matches));
}

export function findRoleSignals(role: string): RoleSignal[] {
  const trimmedRole = role.trim();
  const signals: RoleSignal[] = [];
  const gradeMatches = matchEighthAndBelowGrade(trimmedRole);

  if (gradeMatches.length > 0) {
    signals.push({
      id: "eighth_and_below_grade",
      label: "role includes 8th grade or below/middle school",
      matches: gradeMatches,
    });
  }

  for (const rule of SIGNAL_RULES) {
    if (rule.id === "student_services_or_library" && CLASSROOM_TEACHING_PATTERN.test(trimmedRole)) {
      continue;
    }

    const matches = collectPatternMatches(trimmedRole, rule.pattern);
    if (matches.length === 0) continue;

    signals.push({ id: rule.id, label: rule.label, matches });
  }

  return signals;
}

function toDetailRows(header: string[], rejectedRows: RejectedRoleRow[]): string[][] {
  return [
    [
      "rejection_line",
      "rejection_signal_count",
      "rejection_signals",
      "rejection_matched_terms",
      "teacher_role_normalized",
      ...header,
    ],
    ...rejectedRows.map((row) => [
      String(row.line),
      String(row.signals.length),
      row.signals.map((signal) => signal.label).join("; "),
      row.signals.map((signal) => `${signal.id}: ${signal.matches.join(" | ")}`).join("; "),
      row.role.trim().toLowerCase().replace(/\s+/g, " "),
      ...row.fields,
    ]),
  ];
}

function toSummaryRows(summaries: SignalSummary[]): string[][] {
  const sortedSummaries = [...summaries].sort((a, b) => b.count - a.count || a.label.localeCompare(b.label));

  return [
    ["signal_id", "signal", "flagged_rows", "matched_terms", "example_roles"],
    ...sortedSummaries.map((summary) => [
      summary.id,
      summary.label,
      String(summary.count),
      Array.from(summary.matchedTerms).slice(0, 40).join("; "),
      Array.from(summary.exampleRoles).slice(0, 10).join("; "),
    ]),
  ];
}

function updateSummary(summaries: Map<string, SignalSummary>, signal: RoleSignal, role: string): void {
  const existing = summaries.get(signal.id) ?? {
    id: signal.id,
    label: signal.label,
    count: 0,
    matchedTerms: new Set<string>(),
    exampleRoles: new Set<string>(),
  };

  existing.count += 1;
  for (const match of signal.matches) existing.matchedTerms.add(match.toLowerCase());
  if (role.trim() !== "") existing.exampleRoles.add(role.trim());
  summaries.set(signal.id, existing);
}

async function main(): Promise<number> {
  const args = parseArgs(Bun.argv.slice(2));
  const input = resolve(args.input);
  const content = await Bun.file(input).text();
  const rows = parseCsv(content);
  const headerRow = rows[0];

  if (!headerRow) throw new Error(`No rows found in ${args.input}`);

  const header = headerRow.fields;
  const roleIndex = getColumnIndex(header, "teacher_role");
  const rejectedRows: RejectedRoleRow[] = [];
  const keptRows: string[][] = [header];
  const summaries = new Map<string, SignalSummary>();

  for (const csvRow of rows.slice(1)) {
    const row = csvRow.fields;
    const role = row[roleIndex] ?? "";
    const signals = findRoleSignals(role);

    if (signals.length === 0) {
      keptRows.push(row);
      continue;
    }

    rejectedRows.push({ line: csvRow.line, role, signals, fields: row });
    for (const signal of signals) updateSummary(summaries, signal, role);
  }

  const summaryRows = toSummaryRows(Array.from(summaries.values()));

  console.log(`file: ${args.input}`);
  console.log(`total_rows: ${Math.max(rows.length - 1, 0)}`);
  console.log(`flagged_rows: ${rejectedRows.length}`);
  console.log(`kept_rows: ${Math.max(keptRows.length - 1, 0)}`);
  console.log("top_signals:");
  for (const row of summaryRows.slice(1, 11)) {
    console.log(`${row[1]}: ${row[2]}`);
  }

  await Bun.write(resolve(args.output), serializeCsv(toDetailRows(header, rejectedRows)));
  await Bun.write(resolve(args.summaryOutput), serializeCsv(summaryRows));

  if (args.removeFromInput) {
    await Bun.write(input, serializeCsv(keptRows));
    console.log(`removed_from_input: ${rejectedRows.length}`);
  }

  console.log(`output: ${args.output}`);
  console.log(`summary_output: ${args.summaryOutput}`);

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
