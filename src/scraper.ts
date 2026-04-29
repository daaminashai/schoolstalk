// ── school website scraping via browser-use ──

import { z } from "zod";
import { debug } from "./debug";
import type { RawTeacherData, RawSiteInfo } from "./types";
import { tryThrillshareBypass } from "./thrillshareBypass";
import {
  createClient,
  createSession,
  runTaskStructured,
  stopSession,
} from "./browser";

// ── zod schemas for structured agent output ────────────────────────────────

const TeacherSchema = z.object({
  name: z.string(),
  email: z.string().nullable(),
  role: z.string().nullable(),
  department: z.string().nullable(),
});

const TeachersSchema = z.object({
  teachers: z.array(TeacherSchema),
});

// ── browser-use task prompt ────────────────────────────────────────────────

function promptExtractTeachers(candidates?: string[]): string {
  const preface = candidates && candidates.length > 0
    ? `Start with these candidate staff URLs in order. Visit every candidate that loads, merge results across all of them, and if a candidate is admin-only still check nearby staff/faculty links on the same site before moving on. After the candidates, do one quick site-navigation pass for any missed directory pages.\n\nCandidate staff URLs:\n${candidates.map((u, i) => ` ${i + 1}. ${u}`).join("\n")}\n\n`
    : "";

  return `${preface}Extract every teacher you can find across all subjects.

If no candidate URLs were provided, first find staff/faculty/teacher/directory pages from nav, footer, search, or department pages.

Include:
- teachers in any subject
- department chairs, coordinators, coaches, specialists, and interventionists only when they actively teach students
- long-term substitutes

Exclude:
- non-teaching support staff
- administrators with no current teaching role
- librarians unless the page explicitly shows they teach classes

Field rules:
- name: full first + last name; strip titles/postnominals
- email: exact address only; decode obvious obfuscation; if missing set null; never guess; keeping email=null is better than dropping the teacher
- role: exact job title as written
- department: explicit subject only; if the page does not explicitly label a subject/department, set null; never infer from role; never put a school name or grade level here

When a page looks empty, do not assume there are no teachers. If the HTML is mostly an app shell, root div, bundle scripts, or a noscript warning, use the browser and wait for JS-rendered content before deciding.

Finalsite shortcut: if you see \`fsConstituentItem\`, do not trust the generic listing title. Use \`?const_search_keyword=<term>\` sweeps and merge by constituent ID. Sweep: math, mathematics, algebra, geometry, calculus, statistics, science, biology, chemistry, physics, environmental, earth, anatomy, astronomy, forensic, computer, computer science, CS, coding, programming, software, tech ed, technology, IT, digital, stem, engineering, pre-engineering, design technology, maker, robotics, CAD, drafting, woodworking. Decode emails from \`FS.util.insertEmail\` by reversing both username and domain. Paginate by clicking the widget controls; some sites ignore direct \`?const_page=N\` loads. If the directory has 500+ entries, prefer targeted keyword sweeps over brute-force pagination.

Coverage:
- merge results across candidate pages, profile pages, filters, tabs, departments, and pagination
- click into profiles when email or role is not shown on the listing page
- if a district has many school subsites, prioritize breadth across schools before exhausting one school

Return structured output only: { "teachers": [ ... ] }. Do not save files. Do not call save_output_json. Do not summarize in prose.`;
}

// ── main export ──

export interface ScraperOutput {
  teachers: RawTeacherData[];
  siteInfo: RawSiteInfo;
  sessionId: string;
}

const SCRAPER_MODEL_EXTRACT = "extract" as const;

export interface ScrapeSchoolOptions {
  onStatus?: (msg: string) => void;
  onMilestone?: (msg: string, level?: "info" | "warn") => void;
  onLiveUrl?: (url: string) => void;
  schoolName?: string;
  /**
   * If provided, the scraper will try these candidate directory URLs first.
   * If they produce no teachers, extraction retries once without pinned
   * candidates so the agent can do a fresh discovery pass.
   */
  preferredDirectoryUrls?: string[];
  /**
   * Fires at the extract-task boundary so the orchestrator can advance its
   * phase indicator deterministically without substring-matching agent text.
   */
  onScraperPhase?: (phase: "extract") => void;
}

export async function scrapeSchool(
  schoolUrl: string,
  options: ScrapeSchoolOptions = {},
): Promise<ScraperOutput> {
  const status = options.onStatus ?? (() => {});
  const milestone = options.onMilestone ?? (() => {});
  const onLiveUrl = options.onLiveUrl;
  const preferred = (options.preferredDirectoryUrls ?? []).filter(Boolean);

  debug("SCRAPER", `scrapeSchool → ${schoolUrl}`);

  options.onScraperPhase?.("extract");

  const direct = await tryThrillshareBypass(schoolUrl, preferred, status);
  if (direct) {
    direct.siteInfo.name = direct.siteInfo.name ?? options.schoolName ?? null;
    milestone(
      `used Thrillshare/Apptegy API bypass (${direct.teachers.length} teacher candidate${direct.teachers.length === 1 ? "" : "s"})`,
    );
    return direct;
  }

  const client = createClient();
  const session = await createSession(client);
  const { id: sessionId, liveUrl } = session;
  debug("SCRAPER", `created session · id=${sessionId} liveUrl=${liveUrl || "(none)"}`);

  if (liveUrl) onLiveUrl?.(liveUrl);

  try {
    const siteInfo: RawSiteInfo = {
      name: options.schoolName ?? null,
    };

    if (preferred.length > 0) {
      status("trying provided directory candidates...");
      milestone(`using ${preferred.length} provided directory candidate${preferred.length === 1 ? "" : "s"}`);
    } else {
      status("finding staff pages and extracting teachers...");
    }

    status("extracting teachers...");

    const candidates = (() => {
      const list = preferred.length > 0 ? [...preferred, schoolUrl] : [];
      const seen = new Set<string>();
      const out: string[] = [];
      for (const u of list) {
        const k = u.trim();
        if (!k) continue;
        const low = k.toLowerCase();
        if (seen.has(low)) continue;
        seen.add(low);
        out.push(k);
      }
      return out;
    })();

    let extraction = await runTaskStructured(
      client,
      sessionId,
      promptExtractTeachers(candidates),
      TeachersSchema,
      { onMessage: options.onStatus, model: SCRAPER_MODEL_EXTRACT, emptyOnNull: { teachers: [] } },
    );
    debug("SCRAPER", `extract raw result · ${extraction.teachers.length} teachers`, extraction);

    if (extraction.teachers.length === 0 && preferred.length > 0) {
      milestone("candidate URLs yielded 0 teachers — retrying with fresh site discovery", "warn");
      status("retrying extraction without pinned candidates...");
      extraction = await runTaskStructured(
        client,
        sessionId,
        promptExtractTeachers(),
        TeachersSchema,
        { onMessage: options.onStatus, model: SCRAPER_MODEL_EXTRACT, emptyOnNull: { teachers: [] } },
      );
      debug("SCRAPER", `extract (after fallback) · ${extraction.teachers.length} teachers`, extraction);
    }

    const teachers: RawTeacherData[] = extraction.teachers.map((t) => ({
      name: t.name,
      ...(t.email != null && { email: t.email }),
      ...(t.role != null && { role: t.role }),
      ...(t.department != null && { department: t.department }),
    }));

    milestone(
      `extracted ${teachers.length} teacher candidate${teachers.length === 1 ? "" : "s"} from the site`,
    );

    return { teachers, siteInfo, sessionId };
  } finally {
    await stopSession(client, sessionId);
  }
}
