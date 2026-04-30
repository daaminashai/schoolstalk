#!/usr/bin/env bun

import { resolve } from "node:path";

const DEFAULT_INPUT = "dist/teachers.csv";
const DEFAULT_ZIP_SOURCE = "uszips.csv";

type Args = {
  input: string;
  zipSource: string;
  dryRun: boolean;
};

type ZipMapping = {
  city: string;
  state: string;
};

function parseArgs(argv: string[]): Args {
  let input = DEFAULT_INPUT;
  let zipSource = DEFAULT_ZIP_SOURCE;
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

    if (arg === "--zip-source") {
      const value = argv[i + 1];
      if (!value) throw new Error(`Missing value for ${arg}`);
      zipSource = value;
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

  return { input, zipSource, dryRun };
}

function printUsage(): void {
  console.log(`Usage: bun scripts/fill_teacher_cities_from_zip.ts [--input dist/teachers.csv] [--zip-source uszips.csv] [--dry-run]

Fills blank State and City values using ZIP mappings from uszips.csv.`);
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

function getColumnIndex(header: string[], column: string): number {
  const index = header.findIndex((value) => normalizeHeader(value) === normalizeHeader(column));
  if (index === -1) throw new Error(`No ${column} column found`);
  return index;
}

function normalizeZip(value: string): string {
  const match = value.match(/\d{5}/);
  return match?.[0] ?? "";
}

function loadZipMappings(rows: string[][], source: string): Map<string, ZipMapping> {
  const header = rows[0];
  if (!header) throw new Error(`No rows found in ${source}`);

  const zipIndex = getColumnIndex(header, "zip");
  const cityIndex = getColumnIndex(header, "city");
  const stateIndex = getColumnIndex(header, "state_id");
  const mappings = new Map<string, ZipMapping>();

  for (const row of rows.slice(1)) {
    const zip = normalizeZip(row[zipIndex] ?? "");
    const city = (row[cityIndex] ?? "").trim();
    const state = (row[stateIndex] ?? "").trim().toUpperCase();

    if (zip === "" || city === "" || state === "") continue;
    mappings.set(zip, { city, state });
  }

  return mappings;
}

async function main(): Promise<number> {
  const args = parseArgs(Bun.argv.slice(2));
  const input = resolve(args.input);
  const zipSource = resolve(args.zipSource);
  const content = await Bun.file(input).text();
  const zipSourceContent = await Bun.file(zipSource).text();
  const rows = parseCsv(content);
  const zipSourceRows = parseCsv(zipSourceContent);
  const header = rows[0];

  if (!header) throw new Error(`No rows found in ${args.input}`);

  const cityIndex = getColumnIndex(header, "City");
  const stateIndex = getColumnIndex(header, "State");
  const zipIndex = getColumnIndex(header, "Zip");
  const zipMappings = loadZipMappings(zipSourceRows, args.zipSource);

  let missingCityRows = 0;
  let missingStateRows = 0;
  let filledCityRows = 0;
  let filledStateRows = 0;
  let skippedNoZip = 0;
  let missingZipRows = 0;
  const missingZips = new Set<string>();

  for (const row of rows.slice(1)) {
    const city = (row[cityIndex] ?? "").trim();
    const state = (row[stateIndex] ?? "").trim();
    const zip = normalizeZip(row[zipIndex] ?? "");

    if (city === "") missingCityRows += 1;
    if (state === "") missingStateRows += 1;
    if (city !== "" && state !== "") continue;

    if (zip === "") {
      skippedNoZip += 1;
      continue;
    }

    const mapping = zipMappings.get(zip);
    if (!mapping) {
      missingZipRows += 1;
      missingZips.add(zip);
      continue;
    }

    if (city === "") {
      row[cityIndex] = mapping.city;
      filledCityRows += 1;
    }

    if (state === "") {
      row[stateIndex] = mapping.state;
      filledStateRows += 1;
    }
  }

  if (!args.dryRun && (filledCityRows > 0 || filledStateRows > 0)) {
    await Bun.write(input, serializeCsv(rows));
  }

  console.log(`file: ${args.input}`);
  console.log(`zip_source: ${args.zipSource}`);
  console.log(`zip_mappings: ${zipMappings.size}`);
  console.log(`missing_city_rows: ${missingCityRows}`);
  console.log(`missing_state_rows: ${missingStateRows}`);
  console.log(`filled_city_rows: ${filledCityRows}`);
  console.log(`filled_state_rows: ${filledStateRows}`);
  console.log(`skipped_no_zip: ${skippedNoZip}`);
  console.log(`missing_zip_rows: ${missingZipRows}`);
  console.log(`missing_zip_values: ${Array.from(missingZips).sort().join(", ") || "<none>"}`);
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
