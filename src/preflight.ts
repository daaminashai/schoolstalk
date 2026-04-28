import { existsSync } from "node:fs";
import { mkdir, unlink } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import color from "picocolors";
import { getOpenRouterApiKey, getOpenRouterModel } from "./aiConfig";
import { SQL } from "bun";

interface CheckResult {
  name: string;
  ok: boolean;
  fatal: boolean;
  detail?: string;
}

const t = {
  ok: color.green,
  warn: color.yellow,
  bad: color.red,
  muted: color.dim,
  bold: color.bold,
};

export async function runPreflight(argv: string[] = []): Promise<void> {
  const results: CheckResult[] = [];
  const add = (name: string, ok: boolean, detail?: string, fatal = true) => {
    results.push({ name, ok, detail, fatal });
  };

  const hasOpenRouterKey = !!getOpenRouterApiKey();
  add("OpenRouter API key", hasOpenRouterKey, hasOpenRouterKey ? "set" : "OPENROUTER_API_KEY is required");
  add("AI model configured", !!getOpenRouterModel(), `using ${getOpenRouterModel()}`);
  add("entrypoint exists", existsSync(resolve("index.ts")), existsSync(resolve("index.ts")) ? "index.ts" : "index.ts missing");
  add(
    "browser bridge exists",
    existsSync(resolve("scripts/local_browser_use_agent.py")),
    existsSync(resolve("scripts/local_browser_use_agent.py")) ? "scripts/local_browser_use_agent.py" : "scripts/local_browser_use_agent.py missing",
  );
  add("requirements file exists", existsSync(resolve("requirements.txt")), existsSync(resolve("requirements.txt")) ? "requirements.txt" : "requirements.txt missing");

  const outputCheck = await checkWritable(resolve("output", ".preflight-write-test"));
  add("output directory writable", outputCheck.ok, outputCheck.detail);

  const logFile = process.env.LOG_FILE?.trim();
  if (logFile) {
    const logCheck = await checkWritable(resolve(dirname(logFile), ".preflight-log-write-test"));
    add("log file writable", logCheck.ok, logCheck.detail);
  }

  const statusPath = process.env.STATUS_CSV_PATH?.trim() || "output/status.csv";
  const statusCheck = await checkWritable(resolve(dirname(statusPath), ".preflight-status-write-test"));
  add("status csv writable", statusCheck.ok, statusCheck.detail);

  const schoolsCsv = argValue(argv, "--schools-csv") ?? atFile(argv) ?? process.env.SCHOOLYANK_INPUT_CSV?.trim();
  if (schoolsCsv) {
    const csvPath = resolve(schoolsCsv);
    add("schools csv exists", existsSync(csvPath), existsSync(csvPath) ? csvPath : `${csvPath} missing`);
    if (existsSync(csvPath)) {
      const headerCheck = await checkSchoolsCsvHeader(csvPath);
      add("schools csv header", headerCheck.ok, headerCheck.detail);

      const schoolsDirCheck = await checkWritable(resolve("schools", ".preflight-write-test"));
      add("schools directory writable", schoolsDirCheck.ok, schoolsDirCheck.detail);
    }
  }

  const python = pythonExecutable();
  const pythonVersion = await runCommand(python, ["--version"], 5_000);
  add("python available", pythonVersion.ok, pythonVersion.detail);

  if (pythonVersion.ok) {
    const deps = await runCommand(
      python,
      [
        "-c",
        "import browser_use, pydantic; from browser_use.llm.openrouter.chat import ChatOpenRouter; print('ok')",
      ],
      15_000,
    );
    add(
      "browser-use python deps",
      deps.ok,
      deps.ok ? "imports succeeded" : `${deps.detail}; run python3 -m pip install -r requirements.txt && browser-use install`,
    );
  }

  const requireDatabase = envBool("REQUIRE_DATABASE");
  if (process.env.DATABASE_URL?.trim()) {
    const db = await checkDatabase();
    add("database reachable", db.ok, db.detail);
  } else {
    add(
      "database configured",
      !requireDatabase,
      requireDatabase ? "DATABASE_URL is required because REQUIRE_DATABASE=true" : "DATABASE_URL unset; CSV-only mode",
      requireDatabase,
    );
  }

  const headless = process.env.BROWSER_USE_HEADLESS ?? process.env.BROWSER_USE_LOCAL_HEADLESS;
  if (process.env.NODE_ENV === "production" && !headless) {
    add("headless browser setting", true, "BROWSER_USE_HEADLESS is unset; set true for container deploys", false);
  }

  printResults(results);
  if (results.some((r) => !r.ok && r.fatal)) process.exit(1);
}

function printResults(results: CheckResult[]): void {
  console.log(t.bold("preflight checks"));
  for (const r of results) {
    const marker = r.ok ? t.ok("[ok]") : r.fatal ? t.bad("[fail]") : t.warn("[warn]");
    const detail = r.detail ? ` ${t.muted(r.detail)}` : "";
    console.log(`${marker} ${r.name}${detail}`);
  }
}

function argValue(argv: string[], flag: string): string | null {
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    if (a === flag) return argv[i + 1] ?? null;
    if (a.startsWith(`${flag}=`)) return a.slice(flag.length + 1);
  }
  return null;
}

function atFile(argv: string[]): string | null {
  const arg = argv.find((a) => a.startsWith("@") && a.length > 1);
  return arg ? arg.slice(1) : null;
}

function envBool(name: string): boolean {
  return /^(1|true|yes|on)$/i.test(process.env[name]?.trim() ?? "");
}

async function checkWritable(path: string): Promise<{ ok: boolean; detail: string }> {
  try {
    await mkdir(dirname(path), { recursive: true });
    await Bun.write(path, "ok\n");
    await unlink(path);
    return { ok: true, detail: dirname(path) };
  } catch (err) {
    return { ok: false, detail: err instanceof Error ? err.message : String(err) };
  }
}

async function checkSchoolsCsvHeader(path: string): Promise<{ ok: boolean; detail: string }> {
  try {
    const text = await Bun.file(path).text();
    const headerLine = text.split(/\r?\n/, 1)[0] ?? "";
    const headers = parseCsvRow(headerLine).map((h) => h.trim());
    const required = ["Hs ID", "Name", "State", "City", "School Homepage"];
    const missing = required.filter((h) => !headers.includes(h));
    if (missing.length > 0) return { ok: false, detail: `missing: ${missing.join(", ")}` };
    return { ok: true, detail: `${headers.length} columns` };
  } catch (err) {
    return { ok: false, detail: err instanceof Error ? err.message : String(err) };
  }
}

function parseCsvRow(line: string): string[] {
  const fields: string[] = [];
  let field = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i]!;
    if (inQuotes) {
      if (c === '"' && line[i + 1] === '"') {
        field += '"';
        i++;
      } else if (c === '"') {
        inQuotes = false;
      } else {
        field += c;
      }
    } else if (c === '"') {
      inQuotes = true;
    } else if (c === ",") {
      fields.push(field);
      field = "";
    } else {
      field += c;
    }
  }
  fields.push(field);
  return fields;
}

function pythonExecutable(): string {
  if (process.env.BROWSER_USE_PYTHON?.trim()) return process.env.BROWSER_USE_PYTHON.trim();

  const venvPython = process.env.VIRTUAL_ENV
    ? resolve(process.env.VIRTUAL_ENV, "bin", "python")
    : "";
  if (venvPython && existsSync(venvPython)) return venvPython;

  const projectVenvPython = resolve(".venv", "bin", "python");
  if (existsSync(projectVenvPython)) return projectVenvPython;

  return "python3";
}

async function runCommand(
  command: string,
  args: string[],
  timeoutMs: number,
): Promise<{ ok: boolean; detail: string }> {
  try {
    const proc = Bun.spawn([command, ...args], {
      stdout: "pipe",
      stderr: "pipe",
      env: process.env,
    });
    const timeout = setTimeout(() => proc.kill(), timeoutMs);
    const [exitCode, stdout, stderr] = await Promise.all([
      proc.exited,
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
    ]);
    clearTimeout(timeout);
    const output = (stdout || stderr).trim();
    return {
      ok: exitCode === 0,
      detail: output || `exit ${exitCode}`,
    };
  } catch (err) {
    return { ok: false, detail: err instanceof Error ? err.message : String(err) };
  }
}

async function checkDatabase(): Promise<{ ok: boolean; detail: string }> {
  try {
    const sql = new SQL(process.env.DATABASE_URL!);
    await Promise.race([
      sql`select 1`,
      new Promise((_, reject) => setTimeout(() => reject(new Error("connection timed out")), 10_000)),
    ]);
    await sql.close({ timeout: 0 });
    return { ok: true, detail: "select 1 succeeded" };
  } catch (err) {
    return { ok: false, detail: err instanceof Error ? err.message : String(err) };
  }
}

if (import.meta.main) {
  await runPreflight(Bun.argv.slice(2));
}
