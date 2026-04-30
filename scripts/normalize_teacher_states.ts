#!/usr/bin/env bun

import { resolve } from "node:path";

const DEFAULT_INPUT = "dist/teachers.csv";

type Args = {
  input: string;
  dryRun: boolean;
};

const STATE_ABBREVIATIONS: Record<string, string> = {
  alabama: "AL",
  alaska: "AK",
  arizona: "AZ",
  arkansas: "AR",
  california: "CA",
  colorado: "CO",
  connecticut: "CT",
  delaware: "DE",
  "district of columbia": "DC",
  florida: "FL",
  georgia: "GA",
  hawaii: "HI",
  idaho: "ID",
  illinois: "IL",
  indiana: "IN",
  iowa: "IA",
  kansas: "KS",
  kentucky: "KY",
  louisiana: "LA",
  maine: "ME",
  maryland: "MD",
  massachusetts: "MA",
  massachussets: "MA",
  massachussetts: "MA",
  michigan: "MI",
  minnesota: "MN",
  mississippi: "MS",
  missouri: "MO",
  montana: "MT",
  nebraska: "NE",
  nevada: "NV",
  "new hampshire": "NH",
  "new jersey": "NJ",
  "new mexico": "NM",
  "new york": "NY",
  "north carolina": "NC",
  "north dakota": "ND",
  ohio: "OH",
  oklahoma: "OK",
  oregon: "OR",
  pennsylvania: "PA",
  "rhode island": "RI",
  "south carolina": "SC",
  "south dakota": "SD",
  tennessee: "TN",
  texas: "TX",
  utah: "UT",
  vermont: "VT",
  virginia: "VA",
  washington: "WA",
  "west virginia": "WV",
  wisconsin: "WI",
  wyoming: "WY",
};

const VALID_ABBREVIATIONS = new Set(Object.values(STATE_ABBREVIATIONS));
const STATE_NAME_MATCHES = Object.entries(STATE_ABBREVIATIONS)
  .sort(([left], [right]) => right.length - left.length)
  .map(([name, abbreviation]) => {
    const escaped = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&").replace(/ /g, "\\s+");
    return { abbreviation, pattern: new RegExp(`(^|[^a-z])${escaped}([^a-z]|$)`, "i") };
  });

function parseArgs(argv: string[]): Args {
  let input = DEFAULT_INPUT;
  let dryRun = false;

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];

    if (arg === "--input" || arg === "-i") {
      const value = argv[i + 1];
      if (!value) throw new Error(`Missing value for ${arg}`);
      input = value;
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

  return { input, dryRun };
}

function printUsage(): void {
  console.log(`Usage: bun scripts/normalize_teacher_states.ts [--input dist/teachers.csv] [--dry-run]

Normalizes the State column to two-letter postal abbreviations.`);
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

function normalizeState(value: string): string {
  const trimmed = value.trim();
  if (trimmed === "") return value;

  const upper = trimmed.toUpperCase();
  if (VALID_ABBREVIATIONS.has(upper)) return upper;

  const key = trimmed.toLowerCase().replace(/\s+/g, " ");
  const exactMatch = STATE_ABBREVIATIONS[key];
  if (exactMatch) return exactMatch;

  const embeddedMatch = STATE_NAME_MATCHES.find(({ pattern }) => pattern.test(trimmed));
  return embeddedMatch?.abbreviation ?? value;
}

async function main(): Promise<number> {
  const args = parseArgs(Bun.argv.slice(2));
  const input = resolve(args.input);
  const content = await Bun.file(input).text();
  const rows = parseCsv(content);
  const header = rows[0];

  if (!header) throw new Error(`No rows found in ${args.input}`);

  const stateIndex = header.findIndex((column) => normalizeHeader(column) === "state");
  if (stateIndex === -1) throw new Error(`No State column found in ${args.input}`);

  const changes = new Map<string, number>();
  const invalidStates = new Map<string, number>();
  const stateCounts = new Map<string, number>();
  let changedRows = 0;
  let invalidRows = 0;
  let blankStateRows = 0;

  for (let rowIndex = 1; rowIndex < rows.length; rowIndex += 1) {
    const row = rows[rowIndex];
    if (!row) continue;

    const current = row[stateIndex];
    if (current === undefined) continue;

    const normalized = normalizeState(current);
    if (normalized !== current) {
      row[stateIndex] = normalized;
      changedRows += 1;
      const label = `${current || "<blank>"} -> ${normalized || "<blank>"}`;
      changes.set(label, (changes.get(label) ?? 0) + 1);
    }

    const finalState = normalized.trim();
    if (finalState === "") {
      blankStateRows += 1;
      continue;
    }

    if (finalState !== "" && !/^[A-Z]{2}$/.test(finalState)) {
      invalidRows += 1;
      invalidStates.set(finalState, (invalidStates.get(finalState) ?? 0) + 1);
      continue;
    }

    stateCounts.set(finalState, (stateCounts.get(finalState) ?? 0) + 1);
  }

  if (invalidRows > 0) {
    const examples = Array.from(invalidStates, ([state, count]) => `${state}: ${count}`).join(", ");
    throw new Error(`Found ${invalidRows} rows with non-two-letter State values after normalization: ${examples}`);
  }

  if (!args.dryRun && changedRows > 0) {
    await Bun.write(input, serializeCsv(rows));
  }

  console.log(`file: ${args.input}`);
  console.log(`changed_rows: ${changedRows}`);
  for (const [label, count] of changes) {
    console.log(`${label}: ${count}`);
  }
  console.log("state_counts:");
  for (const [state, count] of Array.from(stateCounts).sort(([a], [b]) => a.localeCompare(b))) {
    console.log(`${state}: ${count}`);
  }
  console.log(`blank_state_rows: ${blankStateRows}`);
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
