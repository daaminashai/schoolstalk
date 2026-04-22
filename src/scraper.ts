// ── phase 1: school website scraping via browser-use ──

import { z } from "zod";
import { debug } from "./debug";
import type { RawTeacherData, RawSiteInfo } from "./types";
import {
  createClient,
  createSession,
  runTask,
  runTaskStructured,
  stopSession,
} from "./browser";

// comprehensive stem subject keywords fed into the browser-use prompts
const STEM_SUBJECTS = [
  "algebra", "geometry", "calculus", "precalculus", "pre-calculus",
  "trigonometry", "statistics", "probability", "ap calculus",
  "ap statistics", "integrated math", "math", "mathematics",
  "finite math", "discrete math", "multivariable calculus",
  "linear algebra", "math analysis",
  "physics", "chemistry", "biology", "ap physics", "ap chemistry",
  "ap biology", "environmental science", "apes", "earth science",
  "geology", "anatomy", "physiology", "forensic science",
  "marine biology", "astronomy", "ap environmental science",
  "physical science", "life science", "general science", "science",
  "computer science", "ap computer science", "cs", "engineering",
  "stem", "technology", "robotics", "coding", "programming",
  "information technology", "it", "digital electronics",
  "principles of engineering", "maker space",
].join(", ");

// subjects that sound like stem but aren't — the agent needs to skip these
const EXCLUSIONS = [
  "political science", "social science", "library science",
  "exercise science", "sports science", "science of cooking",
].join(", ");

// ── zod schemas for structured agent output ────────────────────────────────

const SiteInfoSchema = z.object({
  siteType: z.enum(["district", "school"]),
  name: z.string().nullable(),
});

const TeacherSchema = z.object({
  name: z.string(),
  email: z.string().nullable(),
  role: z.string().nullable(),
  department: z.string().nullable(),
});

const TeachersSchema = z.object({
  teachers: z.array(TeacherSchema),
});

// (single-school mode only; no umbrella probing)

// ── browser-use task prompts ──

function promptSchoolInfo(schoolUrl: string): string {
  return `Go to ${schoolUrl}.

Your task: this is a SINGLE SCHOOL website. Extract identifying information about THE ONE school.

Before extracting: after the page loads, check the final URL in the address bar. If it differs from ${schoolUrl}, treat the FINAL domain as the authoritative source for subsequent navigation and email inference.

━━ NAME EXTRACTION ━━

Use the OFFICIAL, FULL name as it appears in the footer, "Contact Us" page, or "About" page. Do NOT use shorthand from the navigation bar.

━━ OUTPUT ━━

Return your answer as structured JSON matching the required schema:
- siteType: "school" (always)
 - name: the official full name (null only if you genuinely cannot find one)

  Do NOT save output to a file. Do NOT use save_output_json. Return the JSON data as your final response via the structured output format.`;
}

// (no umbrella prompt in single-school mode)

function promptFindStaffDirectory(): string {
  const commonLabels = `"Staff", "Faculty", "Our Team", "Our Staff", "Directory", "Teachers", "Staff Directory", "Faculty & Staff", "Meet Our Staff", "Employee Directory", "Who's Who", "Our People"`;

  return `Find the staff directory, faculty page, or teacher listing on this single-school website.

━━ SEARCH STRATEGY ━━

1. Top nav: ${commonLabels}
2. Hover/expand every top-level menu — staff pages are commonly nested under "About", "Our School", "Community", or "Parents".
3. Footer quick-links.
  4. Department-specific pages (science, math, computer science, engineering, world languages, arts, PE).

━━ WHAT TO REPORT ━━

- URL(s) of every staff/faculty directory page
- URL(s) of any STEM department-specific teacher listings
- Structure notes (paginated? filterable? tabs?)
- If no directory, say so clearly.

  Do NOT save output to a file — describe what you found as your response.`;
}

function promptExtractTeachers(): string {
  return `Visit the staff directory pages you found. Extract EVERY teacher you can find on the site, across ALL subjects.

━━ INCLUDE ━━

- All teachers in any subject (math, science, computer science, engineering, technology, world languages, ELA, social studies, arts, PE, etc.)
- Department chairs/heads/leads who actively teach
- Coordinators, coaches, specialists, interventionists who actively teach students
- Long-term substitutes

━━ EXCLUDE ━━

- Support staff who don't teach (custodians, secretaries, bus drivers, food services)
- Administrators with no current subject-teaching role (principals, vice principals, superintendents, counselors)
- Librarians unless explicitly teaching a class (e.g. library technology integrator)
━━ FIELD RULES ━━

- name: full name (first + last). Strip titles (Dr., Mr., Mrs., Ms.) and postnominals (Jr., PhD, MEd).
- email: exact email address — mailto: links, on-page text, contact sections. Normalize obfuscated forms ("a [at] b [dot] edu" → "a@b.edu"). If no email is visible anywhere, set null — never guess. **Returning teachers with email=null is always better than returning an empty teacher array** — downstream validation infers emails from the district's naming pattern when ≥3 real emails are seen, but it needs SOME teachers to work with. Never skip extracting a teacher just because their email isn't visible.
- role: their job title as written ("AP Physics Teacher", "Math Department Chair"). What they DO.
- department: SUBJECT only — e.g. "Science", "Mathematics", "Computer Science", "Engineering", "Technology". NEVER a school name. NEVER a grade level. Infer from role if the site doesn't name a department.
 - department: SUBJECT only — e.g. "Science", "Mathematics", "Computer Science", "Engineering", "Technology". NEVER a school name. NEVER a grade level. Infer from role if the site doesn't name a department.

━━ CRITICAL ANTI-PATTERNS ━━

❌ Never put a school name in the department field
❌ Never put a grade level in the department field
❌ Never put a district name in assignedSchool
❌ Never combine two real schools into one assignedSchool
❌ Never fabricate an email

━━ IF THE PAGE LOOKS EMPTY — DON'T GIVE UP ━━

Cheap HTTP fetches only return the raw HTML the server ships. Modern districts (Apptegy, React/Vue SPAs, etc.) ship a near-empty shell — something like \`<div id="app"></div>\` plus a few \`<script>\` tags — and the actual directory is rendered by JavaScript AFTER the page loads. If your first fetch returns:

- a body that's mostly empty, or
- an app shell with \`<div id="app">\`, \`<div id="root">\`, or similar single-div mount points, or
- script tags pointing to \`/js/app.*.js\`, \`/static/js/main.*.js\`, chunk bundles, etc., or
- a \`<noscript>\` tag saying "please enable JavaScript"

…**do NOT conclude the page has no content.** Switch to the browser/navigate tool and wait for JS to hydrate the DOM before reading. The directory data is there, just not in the initial HTML.

One extra try is cheap and often succeeds where the fetch tool fails. Returning 0 teachers because the raw HTML looked empty is almost always wrong — the correct response is "retry this URL in the browser".

━━ PLATFORM SHORTCUTS (save yourself iteration rounds) ━━

If the directory page HTML contains \`class="fsConstituentItem"\`, it's a Finalsite directory (common for K-12 districts). Key facts:
- the listing shows generic titles like "Teacher" without subjects — DO NOT rely on the listing view
- use \`?const_search_keyword=<term>\` to reveal subject-specific titles (this searches both name AND title/profile fields)
- sweep with ALL of these terms (do not skip any — each surfaces a different cohort): "math", "mathematics", "algebra", "geometry", "calculus", "statistics", "science", "biology", "chemistry", "physics", "environmental", "earth", "anatomy", "astronomy", "forensic", "computer", "computer science", "CS", "coding", "programming", "software", "tech ed", "technology", "IT", "digital", "stem", "engineering", "pre-engineering", "design technology", "maker", "robotics", "CAD", "drafting", "woodworking" — merge results by constituent ID. Missing ANY of these sweeps causes teachers to be silently dropped.
- emails are JS-obfuscated: \`FS.util.insertEmail("elId", "<reversed domain>", "<reversed username>", true)\` — REVERSE BOTH to decode. This is MANDATORY — do not leave emails blank on a Finalsite directory. Read the raw HTML (or the profile's detail page) and find the FS.util.insertEmail call for each teacher; the 2nd arg is the reversed domain, the 3rd is the reversed username. Reconstruct as \`<reversed(username)>@<reversed(domain)>\`. Example: \`FS.util.insertEmail("x","gro.elpmaxe","enaj.eod")\` → \`jane.doe@example.org\`. If you find teachers on a Finalsite site without also finding their FS.util.insertEmail calls, you're extracting from the wrong page — inspect the HTML source, not the rendered text.
- paginate by CLICKING the "next page" anchor in the widget (some sites, e.g. newtrier.k12.il.us, ignore the \`?const_page=N\` query param on direct load — they only advance via the JS click handler). the anchor will have \`disabled="disabled"\` when there are no more pages. total count is in \`.fsPaginationLabel\`
- do NOT paginate all pages if the directory has 500+ entries — targeted keyword searches are 10× faster than scanning 29 pages of generic titles

━━ COVERAGE ━━

- Handle pagination: click "Next"/"Load More"/page numbers to visit ALL pages.
- If organized by school or department, visit EVERY relevant section.
- Click into individual profiles when emails or titles aren't on the listing page.
- When in doubt about STEM, include — we'll filter later.
- For districts with many per-school subsites (10+), prioritize BREADTH: hit 2-3 keyword sweeps (e.g. \`math\`, \`science\`, \`stem\`) across ALL schools before exhausting subject-by-subject sweeps on any one school. A district that ships 5 teachers per school × 30 schools beats 40 teachers from one school + 0 from the other 29. Do NOT stop after 3-5 schools and report "most had generic titles" — that's a coverage failure, not a finding.

━━ OUTPUT ━━

Return the data using structured output — an object with a "teachers" array where each entry matches the required fields above. Do NOT save to a file. Do NOT call save_output_json. Do NOT summarize in prose. Return the literal structured data.`;
}

// ── main export ──

export interface ScraperOutput {
  teachers: RawTeacherData[];
  siteInfo: RawSiteInfo;
  sessionId: string;
}

// browser-use model. gpt-5.4-mini is the cheapest tier ($0.90/1M in, $5.40/1M
// out) and handles classify + navigation well. the extract task is where
  // recall matters most — missed teachers = silent data loss and per-run
// variance — so we bump extraction to claude-sonnet-4.6 ($3.60/$18.00) for
// stronger coverage on long keyword sweeps.
const SCRAPER_MODEL_DEFAULT = "gpt-5.4-mini" as const;
const SCRAPER_MODEL_EXTRACT = "claude-sonnet-4.6" as const;

export interface ScrapeSchoolOptions {
  onStatus?: (msg: string) => void;
  onMilestone?: (msg: string, level?: "info" | "warn") => void;
  onLiveUrl?: (url: string) => void;
  /**
   * fires at each sub-task boundary inside the scraper. lets the orchestrator
   * advance its phase indicator deterministically without substring-matching
   * browser-agent reasoning (which can spuriously contain phrases like
   * "extracting STEM teachers" mid-task 2 and cause premature transitions).
   */
  onScraperPhase?: (phase: "classify" | "directory" | "extract") => void;
}

export async function scrapeSchool(
  schoolUrl: string,
  options: ScrapeSchoolOptions = {},
): Promise<ScraperOutput> {
  const status = options.onStatus ?? (() => {});
  const milestone = options.onMilestone ?? (() => {});
  const onLiveUrl = options.onLiveUrl;

  // Hasura bypass removed in single-school mode — always use browser agent.

  debug("SCRAPER", `scrapeSchool → ${schoolUrl}`);

  const client = createClient();
  const session = await createSession(client);
  const { id: sessionId, liveUrl } = session;
  debug("SCRAPER", `created session · id=${sessionId} liveUrl=${liveUrl || "(none)"}`);

  if (liveUrl) onLiveUrl?.(liveUrl);

  try {
    // task 1 — gather school info (structured output)
    options.onScraperPhase?.("classify");
    status("gathering school info...");
    let rawSite: z.output<typeof SiteInfoSchema>;
    try {
      rawSite = await runTaskStructured(
        client,
        sessionId,
        promptSchoolInfo(schoolUrl),
        SiteInfoSchema,
        { onMessage: options.onStatus, model: SCRAPER_MODEL_DEFAULT },
      );
    } catch (classifyErr) {
      debug("SCRAPER", `info failed with ${SCRAPER_MODEL_DEFAULT}, retrying with ${SCRAPER_MODEL_EXTRACT}`, classifyErr);
      status(`info failed with ${SCRAPER_MODEL_DEFAULT}, retrying with ${SCRAPER_MODEL_EXTRACT}...`);
      rawSite = await runTaskStructured(
        client,
        sessionId,
        promptSchoolInfo(schoolUrl),
        SiteInfoSchema,
        { onMessage: options.onStatus, model: SCRAPER_MODEL_EXTRACT },
      );
    }
    debug("SCRAPER", `school info result`, rawSite);
    const siteInfo: RawSiteInfo = {
      siteType: "school",
      name: rawSite.name,
    };

    // milestone fires RIGHT NOW, not after all 3 tasks finish. previously the
    // orchestrator emitted this after scrapeSchool returned, so "detected
    // district" only showed up alongside the final "extracted teachers" at
    // the very end of phase 3. firing inline means the user sees it the
    // moment classification completes.
    milestone(`detected school: ${siteInfo.name ?? "(unknown)"}`);

    // task 2 — discover staff directory/directories (free-form text is fine here;
    // used only as navigation context for the agent on its next task)
    options.onScraperPhase?.("directory");
    status("finding staff directory...");
    try {
      await runTask(client, sessionId, promptFindStaffDirectory(), {
        onMessage: options.onStatus,
        model: SCRAPER_MODEL_DEFAULT,
      });
    } catch (dirErr) {
      debug("SCRAPER", `directory failed with ${SCRAPER_MODEL_DEFAULT}, retrying with ${SCRAPER_MODEL_EXTRACT}`, dirErr);
      status(`directory failed with ${SCRAPER_MODEL_DEFAULT}, retrying with ${SCRAPER_MODEL_EXTRACT}...`);
      await runTask(client, sessionId, promptFindStaffDirectory(), {
        onMessage: options.onStatus,
        model: SCRAPER_MODEL_EXTRACT,
      });
    }

    // task 3 — extract teachers (structured output)
    options.onScraperPhase?.("extract");
    status("extracting teachers...");
    const extraction = await runTaskStructured(
      client,
      sessionId,
      promptExtractTeachers(),
      TeachersSchema,
      { onMessage: options.onStatus, model: SCRAPER_MODEL_EXTRACT },
    );
    debug("SCRAPER", `extract raw result · ${extraction.teachers.length} teachers`, extraction);

    // normalize to our RawTeacherData shape (strip nulls where our type expects undefined)
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
