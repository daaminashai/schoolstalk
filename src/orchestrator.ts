// ── orchestrator: coordinates the 4-phase scraping pipeline ──

import type { ScrapeConfig, ScrapeResult, Teacher } from "./types";
import { scrapeSchool } from "./scraper";
// NCES and district resolution removed
import { validateTeachers } from "./validator";
import { validateEmailsBatched } from "./emailValidator";
import { generateCsv, writeCsv } from "./csv";
import { extractDomain } from "./utils";
import { debug } from "./debug";

// ── progress phase model ─────────────────────────────────────────────────────
// each phase renders as "[i/N] label" in the spinner. only phases that actually
// run are counted — linkedin is skipped in the total when disabled.
export type PhaseId =
  | "classify"
  | "directory"
  | "extract"
  | "export";

export const PHASE_LABELS: Record<PhaseId, string> = {
  classify: "classifying site",
  directory: "finding staff directory",
  extract: "extracting teachers",
  export: "writing CSV",
};

export interface RunOptions {
  /** transient status updates — shown in the spinner */
  onStatus?: (msg: string) => void;
  /** phase transitions — lets the UI update its phase indicator */
  onPhase?: (phase: PhaseId, index: number, total: number) => void;
  /** persistent milestone messages — shown above the spinner and preserved */
  onMilestone?: (msg: string, level?: "info" | "warn") => void;
  /** live-url for watching the browser session */
  onLiveUrl?: (url: string) => void;
}

/**
 * runs the full pipeline: scrape → nces verify → csv export.
 */
export async function run(
  config: ScrapeConfig,
  options: RunOptions = {},
): Promise<ScrapeResult> {
  const log = options.onStatus ?? (() => {});
  const milestone = options.onMilestone ?? (() => {});
  const onLiveUrl = options.onLiveUrl;
  const onPhase = options.onPhase ?? (() => {});

  const startTime = Date.now();
  const warnings: string[] = [];

  debug("ORCH", `run() start`, config);

  // the classify + directory + extract tasks all run inside scrapeSchool. we
  // still model them as distinct phases so the ui shows meaningful progress
  // while the scraper thinks.
  const totalPhases = 4;
  const phaseIndex: Record<PhaseId, number> = {
    classify: 1,
    directory: 2,
    extract: 3,
    export: 4,
  };

  function enterPhase(phase: PhaseId) {
    onPhase(phase, phaseIndex[phase], totalPhases);
  }

  // ── scraping (phases 1-3: classify, directory, extract) ──
  // the scraper drives these subphases itself and calls onScraperPhase at each
  // boundary. we listen for that callback to advance our phase indicator —
  // substring-matching agent messages was unreliable (agent reasoning could
  // mention "extracting STEM teachers" mid-task-2, causing premature jumps).
  enterPhase("classify");

  const scrapeResult = await scrapeSchool(config.schoolUrl, {
    onStatus: log,
    onMilestone: milestone,
    onLiveUrl,
    onScraperPhase: (phase) => {
      if (phase === "classify") enterPhase("classify");
      else if (phase === "directory") enterPhase("directory");
      else if (phase === "extract") enterPhase("extract");
    },
  });

  const schoolDomain = extractDomain(config.schoolUrl);

  // ── validate + clean teachers ──
  const preValidateCount = scrapeResult.teachers.length;
  const preWithEmail = scrapeResult.teachers.filter((r) => r.email).length;

  // two-stage filter: validator handles name parsing, email inference, dedup,
  // and confidence scoring — but defers STEM classification + hacker score to
  // an LLM batch pass (one call for the whole district). keyword fallback
  // runs if the LLM errors or returns a malformed response, so the pipeline
  // never blocks on this.
  const candidates = validateTeachers(scrapeResult.teachers, schoolDomain, {
    stemFilter: false,
  });
  debug("ORCH", `validateTeachers · raw=${preValidateCount} → candidates=${candidates.length} (had email raw=${preWithEmail})`);

  // keep ALL teacher candidates (no STEM filtering). validator handled
  // normalization, email inference, dedup, and scoring.
  let teachers: Teacher[] = candidates;

  // ── email validation (DNS MX + SMTP RCPT TO, no API keys) ──
  // null out emails that the destination server explicitly rejects (550/551
  // /553 at RCPT TO) or whose domain has no MX record. inconclusive emails
  // (timeouts, catch-all servers, transient errors) stay untouched — we
  // don't trash data on a flaky probe.
  const emailsToCheck = teachers
    .filter((t) => t.email)
    .map((t) => t.email!);
  if (emailsToCheck.length > 0) {
    log(`validating ${emailsToCheck.length} emails via DNS + SMTP...`);
    debug("ORCH", `email validation · ${emailsToCheck.length} emails`, emailsToCheck);
    const emailStatuses = await validateEmailsBatched(emailsToCheck);
    debug("ORCH", `email validation results`, Object.fromEntries(emailStatuses));
    let invalid = 0;
    let noMx = 0;
    let valid = 0;
    let inconclusive = 0;
    for (const t of teachers) {
      if (!t.email) continue;
      const status = emailStatuses.get(t.email) ?? "inconclusive";
      if (status === "invalid") {
        invalid++;
        t.email = null;
      } else if (status === "no_mx") {
        noMx++;
        t.email = null;
      } else if (status === "valid") {
        valid++;
      } else {
        inconclusive++;
      }
    }
    const removed = invalid + noMx;
    const total = valid + inconclusive + invalid + noMx;
    if (removed > 0) {
      milestone(
        `email check: ${valid} valid, ${inconclusive} inconclusive, ${removed} removed (${invalid} bounced + ${noMx} no MX)`,
        "warn",
      );
    } else if (valid > 0) {
      milestone(
        `email check: ${valid}/${total} verified, ${inconclusive} inconclusive`,
      );
    } else if (inconclusive === total && total > 0) {
      // every probe came back inconclusive — almost certainly port 25 is
      // blocked on this network. SMTP validation is effectively disabled;
      // surface that to the user so they don't wonder why nothing changed.
      milestone(
        `email check: SMTP probe inconclusive for all ${total} emails (port 25 likely blocked — DNS MX still verified)`,
        "warn",
      );
    } else {
      milestone(
        `email check: ${valid} valid, ${inconclusive} inconclusive, ${removed} removed`,
      );
    }
  }

  const inferredCount = teachers.filter((t) =>
    t.sources.includes("inferred"),
  ).length;
  const missingEmailCount = teachers.filter((t) => !t.email).length;

  if (inferredCount > 0) {
    milestone(
      `inferred ${inferredCount} missing email${inferredCount === 1 ? "" : "s"} from the district's pattern`,
    );
  }
  if (missingEmailCount > 0) {
    milestone(
      `${missingEmailCount} teacher${missingEmailCount === 1 ? "" : "s"} still missing email — couldn't infer from the directory`,
      "warn",
    );
  }

  if (teachers.length === 0) {
    warnings.push(
      "no teachers found — the school site may not have a staff directory, or the directory may be empty",
    );
    milestone(
      `⚠ pipeline returned 0 teachers — the scraper could not reach a staff directory on this site. common causes: (1) CMS with no public staff listing (e.g. Apptegy-CMS districts), (2) cross-domain redirect the scraper didn't follow.`,
      "warn",
    );
  } else if (teachers.length < 3) {
    milestone(
      `⚠ pipeline returned only ${teachers.length} teacher${teachers.length === 1 ? "" : "s"} — suspiciously low; the directory may be partially reachable or the scraper stopped short.`,
      "warn",
    );
  }

  // ── phase 4: nces ──
  // NCES/district resolution removed — proceed directly to export

  // LinkedIn enrichment removed — keep teachers as-is

  // ── assemble the final result ──
  const result: ScrapeResult = {
    sourceUrl: config.schoolUrl,
    teachers,
    metadata: {
      scrapedAt: new Date().toISOString(),
      durationMs: Date.now() - startTime,
      pagesVisited: 0,
      browserUseSessionId: scrapeResult.sessionId,
      warnings,
    },
  };

  // ── phase 6: export csv ──
  enterPhase("export");
  log("writing CSV...");
  const csv = generateCsv(result);
  await writeCsv(config.outputPath, csv);

  debug("ORCH", `run() complete · ${((Date.now() - startTime) / 1000).toFixed(2)}s · ${teachers.length} teachers, ${warnings.length} warnings`, {
    outputPath: config.outputPath,
    warnings,
  });

  return result;
}

// ── helpers ──

/**
 * resolves the district (if any) and the full list of schools relevant to this
 * scrape. in district mode we pull the entire LEA roster from nces so each
 * teacher's assigned school can be matched to a real nces record; in
 * single-school mode we just look up the one school.
 */
// resolveSites removed — single-school mode only

/**
 * single-school mode: look up the one school in nces, wrap it as SchoolInfo.
 * no district info is produced even when nces reports an lea_name — the distinction
 * here is about the SITE being a district site vs a single school site.
 */
/*
async function resolveSingleSchool(
  siteInfo: RawSiteInfo,
  schoolUrl: string,
  state: string | null,
  log: (msg: string) => void,
  warnings: string[],
): Promise<{ district: DistrictInfo | null; schools: SchoolInfo[] }> {
  const nameForSearch =
    siteInfo.name ?? extractSchoolNameFromUrl(schoolUrl) ?? "";

  // scrape-side ground truth: parse the address the classifier pulled off the
  // school's own footer/contact page. this is the authoritative signal for
  // whether an NCES candidate actually refers to THIS school — private schools
  // in particular get spuriously matched to same-state public schools with
  // no shared city, and those matches pollute every output field.
  const scrapedAddress = parseAddress(siteInfo.address);
  const scrapedCity = scrapedAddress?.city?.toLowerCase().trim() || null;
  const scrapedZip = scrapedAddress?.zip?.replace(/[^0-9]/g, "").slice(0, 5) || null;
  const scrapedStreetNum = scrapedAddress?.street?.match(/^\d+/)?.[0] ?? null;

  let record: NCESSchoolRecord | null = null;
  if (nameForSearch) {
    // primary path: match the school's name to a state LEA (works because the
    // districts endpoint respects state_location and we fuzzy-match client-side
    // — same pattern as resolveDistrict). a "single school" is often also a
    // 1-school district (e.g. New Trier Township HSD 203, TJHSST → Fairfax
    // County). once we have the LEA, pull its roster and pick the best match.
    //
    // the bare lookupSchool() path below silently ignores school_name= server-
    // side and picks the alphabetical first match of the whole state, so we
    // only use it as a last-resort fallback after the district path fails.
    if (state) {
      const d = await lookupDistrict(nameForSearch, state, scrapedCity ?? undefined);
      if (d) {
        const roster = await lookupSchoolsInDistrict(d.leaid);
        // only accept when the school fuzzy-matches inside the district roster.
        // falling back to roster[0] would silently pick a random school from
        // whatever district lookupDistrict happened to match (e.g. Stuyvesant
        // HS → "Ontech Charter" district → 1-school roster where Ontech wins
        // by default). better to bail and let lookupSchool try instead.
        const m = roster.length > 0 ? matchSchoolInDistrict(nameForSearch, roster) : null;
        if (m && ncesRecordAgreesWithScrapedSite(m, scrapedCity, scrapedZip, scrapedStreetNum, nameForSearch)) {
          record = m;
          log(`matched NCES record via district roster: ${record.school_name} (${record.ncessch})`);
        } else if (m) {
          log(
            `rejected NCES roster match ${m.school_name} (${m.city_location}) — scraped city "${scrapedCity ?? "unknown"}" doesn't agree`,
          );
        }
      }
    }

    if (!record && state) {
      // without a state, lookupSchool returns ~10K random national schools
      // (API ignores school_name filter) — skip entirely, there's nothing
      // useful to match against.
      const fallback = await lookupSchool(nameForSearch, state);
      if (fallback && ncesRecordAgreesWithScrapedSite(fallback, scrapedCity, scrapedZip, scrapedStreetNum, nameForSearch)) {
        record = fallback;
        log(`matched NCES record: ${record.school_name} (${record.ncessch})`);
      } else if (fallback) {
        log(
          `rejected NCES fallback ${fallback.school_name} (${fallback.city_location}) — scraped city "${scrapedCity ?? "unknown"}" doesn't agree`,
        );
      }
    }

    if (!record) {
      log("no NCES match found — using school website data");
      warnings.push("school not found in NCES database — address from school website only");
    }
  }

  const address: Address | null = record
    ? ncesRecordToAddress(record)
    : scrapedAddress;

  const school: SchoolInfo = {
    name: normalizeSchoolName(record?.school_name ?? siteInfo.name) || "Unknown School",
    url: schoolUrl,
    address,
    phone: record && record.phone !== "-1" ? record.phone : null,
    district:
      record && record.lea_name !== "-1" ? normalizeDistrictName(record.lea_name) : null,
    ncesId: record?.ncessch ?? null,
  };

  return { district: null, schools: [school] };
}

/**
 * district mode: look up one of the scraped schools to grab the LEA id, pull
 * the full district roster from nces, wrap every school as SchoolInfo, and
 * produce a DistrictInfo carrying the district office address.
 */
/*
async function resolveDistrict(
  siteInfo: RawSiteInfo,
  schoolUrl: string,
  state: string | null,
  teachers: Teacher[],
  log: (msg: string) => void,
  warnings: string[],
): Promise<{ district: DistrictInfo | null; schools: SchoolInfo[] }> {
  // primary path: search the NCES LEA directory by district name, scoped to
  // the state. the schools-directory endpoint silently ignores school_name /
  // lea_name filters, so seeding via lookupSchool often lands on a wrong LEA.
  // the districts endpoint respects state_location and returns the full state
  // roster, so we fuzzy-match client-side for guaranteed correctness.
  let leaid: string | null = null;
  let districtNameFromNces: string | null = null;
  let districtPhoneFromNces: string | null = null;
  let districtAddressFromNces: Address | null = null;

  if (state && siteInfo.name) {
    const scrapedAddr = parseAddress(siteInfo.address);
    const d = await lookupDistrict(siteInfo.name, state, scrapedAddr?.city ?? undefined);
    if (d) {
      leaid = d.leaid;
      districtNameFromNces = d.lea_name;
      districtPhoneFromNces = d.phone && d.phone !== "-1" ? d.phone : null;
      districtAddressFromNces = districtRecordToAddress(d);
      log(`matched NCES district: ${d.lea_name} (LEA ${d.leaid})`);
    }
  }

  // fallback: the school-name seeding path (kept for states where the scraper
  // returned no district name, or where district lookup missed). each
  // lookupSchool call fetches ~10K records (the API ignores school_name), so
  // we need both a state filter and a hard attempt cap to stay under budget.
  // without a state the API returns random national schools — useless, skip.
  let seedRecord: NCESSchoolRecord | null = null;
  if (!leaid && state) {
    const seedCandidates = [
      ...(siteInfo.schools ?? []),
      ...teachers.map((t) => t.schoolName).filter((s): s is string => !!s),
      siteInfo.name ?? "",
    ].filter((s) => s.trim().length > 0);

    const MAX_SEED_ATTEMPTS = 5;
    let attempts = 0;
    for (const candidate of dedupeStrings(seedCandidates)) {
      if (attempts >= MAX_SEED_ATTEMPTS) break;
      attempts++;
      seedRecord = await lookupSchool(candidate, state);
      if (seedRecord) {
        // sanity check: the schools endpoint's fuzzy match can return a totally
        // unrelated school (endpoint ignores school_name filter). verify the
        // match by requiring either a token overlap with the candidate name or
        // with the district name.
        if (plausibleSeed(seedRecord, candidate, siteInfo.name)) {
          leaid = seedRecord.leaid;
          log(`seeded district lookup via ${seedRecord.school_name} (LEA ${seedRecord.leaid})`);
          break;
        }
        seedRecord = null;
      }
    }
  }

  if (!leaid) {
    log("no NCES match for any school in the district — falling back to scraped data");
    warnings.push("district not found in NCES database — addresses may be incomplete");

    // fallback: emit a synthetic SchoolInfo per scraped school name so csv still renders
    const fallbackSchools: SchoolInfo[] = (siteInfo.schools ?? []).map((name) => ({
      name,
      url: schoolUrl,
      address: null,
      phone: null,
      district: siteInfo.name ?? null,
      ncesId: null,
    }));

    const district: DistrictInfo = {
      name: normalizeDistrictName(siteInfo.name) || "Unknown District",
      leaId: null,
      url: schoolUrl,
      officeAddress: parseAddress(siteInfo.address),
      officePhone: null,
    };

    return { district, schools: fallbackSchools };
  }

  // pull the full LEA roster
  const roster = await lookupSchoolsInDistrict(leaid);
  log(`loaded ${roster.length} schools from NCES for LEA ${leaid}`);

  const schools: SchoolInfo[] = roster.map((r) => ({
    name: normalizeSchoolName(r.school_name),
    url: schoolUrl,
    address: ncesRecordToAddress(r),
    phone: r.phone !== "-1" ? r.phone : null,
    district: r.lea_name !== "-1" ? normalizeDistrictName(r.lea_name) : null,
    ncesId: r.ncessch,
  }));

  // synthesize SchoolInfo entries for schools that exist on the site under a
  // shared-campus umbrella but aren't listed individually in NCES (e.g.
  // "Williston Schools" in NCES covers both "Williston Central School" and
  // "Allen Brook School" on the site). the synthetic entries inherit the
  // umbrella's address/phone/ncesId so teachers get correct federal data,
  // while keeping their site-level display name.
  const rosterShim = roster.map(
    (r) => ({ school_name: r.school_name, ncessch: r.ncessch } as NCESSchoolRecord),
  );

  // track NCES umbrella ids that we've split into members — we drop the
  // umbrella SchoolInfo once its members are synthesized so no teacher row
  // ever falls back to the umbrella name.
  const splitUmbrellaIds = new Set<string>();

  for (const group of siteInfo.schoolGroups ?? []) {
    const umbrellaMatch = matchSchoolInDistrict(group.umbrella, rosterShim);
    if (!umbrellaMatch) continue;

    const umbrellaSchool = schools.find((s) => s.ncesId === umbrellaMatch.ncessch);
    if (!umbrellaSchool) continue;

    let addedAny = false;
    for (const memberName of group.members) {
      const alreadyExists = schools.some(
        (s) => s.name.toLowerCase() === memberName.toLowerCase(),
      );
      if (alreadyExists) continue;

      schools.push({
        name: memberName,
        url: schoolUrl,
        address: umbrellaSchool.address,
        phone: umbrellaSchool.phone,
        district: umbrellaSchool.district,
        ncesId: umbrellaSchool.ncesId,
      });
      addedAny = true;
    }

    if (addedAny && umbrellaSchool.ncesId) {
      splitUmbrellaIds.add(umbrellaSchool.ncesId);
    }
  }

  // second-chance umbrella routing: catch scraped schools that (a) don't match
  // any NCES record directly and (b) weren't covered by the scraper's
  // schoolGroups. this happens when a scraped child-school doesn't share a
  // first-word prefix with its umbrella (e.g. "Allen Brook School" is
  // physically part of "Williston Schools" but the scraper's heuristic only
  // linked "Williston Central School" as a member via first-word match). if
  // the NCES roster has a plural-"Schools" umbrella record, route unmatched
  // scraped schools through it so their federal address/phone still resolves.
  const ncesUmbrellas = roster.filter(
    (r) =>
      /\bschools\s*$/i.test(r.school_name) &&
      r.school_name.split(/\s+/).length >= 2,
  );

  if (ncesUmbrellas.length > 0) {
    const existingNames = new Set(schools.map((s) => s.name.toLowerCase()));

    for (const scrapedName of siteInfo.schools ?? []) {
      const lower = scrapedName.toLowerCase();
      if (existingNames.has(lower)) continue;
      // skip if the fuzzy matcher resolves it to a real NCES record already
      if (matchSchoolInDistrict(scrapedName, rosterShim)) continue;

      // pick the best-matching umbrella. if exactly one umbrella exists, route
      // there. with multiple umbrellas, require a first-word match with the
      // scraped name OR a first-word match with any other scraped school already
      // linked to that umbrella via the scraper's schoolGroups — a transitive
      // signal that the unmatched school lives on the same campus. if neither
      // signal holds, skip (district-office fallback beats a wrong-campus guess).
      let target: NCESSchoolRecord | null = null;
      if (ncesUmbrellas.length === 1) {
        target = ncesUmbrellas[0]!;
      } else {
        const scrapedFirst = scrapedName.toLowerCase().split(/\s+/)[0] ?? "";
        // direct first-word match
        target =
          ncesUmbrellas.find(
            (u) => u.school_name.toLowerCase().split(/\s+/)[0] === scrapedFirst,
          ) ?? null;

        // transitive match: find an umbrella that the scraper already grouped
        // with a sibling scraped school in the same campus. if the scraped
        // school shares a campus with a grouped member (same first-word), route
        // to that umbrella.
        if (!target) {
          for (const group of siteInfo.schoolGroups ?? []) {
            const siblingFirst = group.members
              .map((m) => m.toLowerCase().split(/\s+/)[0])
              .find((w) => w === scrapedFirst);
            if (!siblingFirst) continue;
            const umbrellaRecord = ncesUmbrellas.find(
              (u) =>
                u.school_name.toLowerCase().split(/\s+/)[0] ===
                group.umbrella.toLowerCase().split(/\s+/)[0],
            );
            if (umbrellaRecord) {
              target = umbrellaRecord;
              break;
            }
          }
        }
      }

      if (!target) {
        warnings.push(
          `could not route "${scrapedName}" to an NCES umbrella — ambiguous between multiple umbrellas; address falls back to district office`,
        );
        continue;
      }

      const umbrellaSchool = schools.find((s) => s.ncesId === target.ncessch);
      if (!umbrellaSchool) continue;

      schools.push({
        name: scrapedName,
        url: schoolUrl,
        address: umbrellaSchool.address,
        phone: umbrellaSchool.phone,
        district: umbrellaSchool.district,
        ncesId: umbrellaSchool.ncesId,
      });
      existingNames.add(lower);
    }
  }

  // prefer the LEA-directory name (canonical) over a school record's lea_name,
  // then fall back to the scraped site name.
  const rawDistrictName =
    districtNameFromNces ??
    (seedRecord && seedRecord.lea_name !== "-1" ? seedRecord.lea_name : null) ??
    siteInfo.name ??
    "Unknown District";

  const scrapedAddress = parseAddress(siteInfo.address);

  const district: DistrictInfo = {
    name: normalizeDistrictName(rawDistrictName) || "Unknown District",
    leaId: leaid,
    url: schoolUrl,
    // prefer the scraper-sniffed address (the actual district office) but
    // fall back to the NCES district record's mailing address if the scraper
    // couldn't find one. this is a huge quality win for districts where the
    // site footer omits the address.
    officeAddress: scrapedAddress ?? districtAddressFromNces,
    officePhone: districtPhoneFromNces,
  };

  // drop NCES umbrella records that have been split into their member schools
  // via schoolGroups. the umbrella's address/phone/ncesId live on in each
  // member via inheritance, so removing the umbrella entry doesn't lose data —
  // it just prevents downstream "X Schools" entries with zero teachers from
  // inflating the school count and confusing the summary UI.
  //
  // an entry is the "original umbrella" (not a synthesized member) when its
  // name matches the NCES roster's school_name for that ncesId.
  const finalSchools = schools.filter((s) => {
    if (!s.ncesId || !splitUmbrellaIds.has(s.ncesId)) return true;
    const rosterMatch = roster.find((r) => r.ncessch === s.ncesId);
    const isOriginalUmbrella =
      !!rosterMatch &&
      rosterMatch.school_name.toLowerCase() === s.name.toLowerCase();
    return !isOriginalUmbrella;
  });

  return { district, schools: finalSchools };
}

/**
 * for each teacher, resolve their schoolName/schoolNcesId to a school record
 * in the roster. in single-school mode everyone maps to schools[0]; in
 * district mode each teacher's scraped assignedSchool is fuzzy-matched against
 * the roster.
 */
function resolveTeacherSchools(
  teachers: Teacher[],
  schools: SchoolInfo[],
  district: DistrictInfo | null,
  warnings: string[],
): void {
  if (schools.length === 0) return;

  // single-school mode: everyone goes to the one school
  if (!district) {
    const only = schools[0]!;
    for (const t of teachers) {
      t.schoolName = only.name;
      t.schoolNcesId = only.ncesId;
    }
    return;
  }

  // district mode: map scraped assignedSchool → nces record in the roster.
  // build a pseudo-roster of NCESSchoolRecord-shaped candidates so we can reuse
  // the matcher (it only needs school_name + ncessch).
  const rosterShim = schools
    .filter((s) => s.ncesId)
    .map((s) => ({ school_name: s.name, ncessch: s.ncesId! } as NCESSchoolRecord));

  const unmatched = new Set<string>();

  for (const t of teachers) {
    if (!t.schoolName) {
      // teacher missing a school assignment — leave unresolved; csv will fall back
      warnings.push(`teacher ${t.firstName} ${t.lastName} has no school assignment`);
      continue;
    }

    const match = matchSchoolInDistrict(t.schoolName, rosterShim);
    if (match) {
      const resolved = schools.find((s) => s.ncesId === match.ncessch);
      if (resolved) {
        t.schoolName = resolved.name;
        t.schoolNcesId = resolved.ncesId;
        continue;
      }
    }

    unmatched.add(t.schoolName);
  }

  if (unmatched.size > 0) {
    warnings.push(
      `could not match ${unmatched.size} scraped school name(s) to NCES district roster: ${[...unmatched].join(", ")}`,
    );
  }
}

/** finds the SchoolInfo corresponding to a teacher, or null */
function findSchoolForTeacher(
  teacher: Teacher,
  schools: SchoolInfo[],
): SchoolInfo | null {
  if (teacher.schoolNcesId) {
    const byId = schools.find((s) => s.ncesId === teacher.schoolNcesId);
    if (byId) return byId;
  }
  if (teacher.schoolName) {
    const byName = schools.find(
      (s) => s.name.toLowerCase() === teacher.schoolName!.toLowerCase(),
    );
    if (byName) return byName;
  }
  return null;
}

/** dedupe while preserving order */
function dedupeStrings(arr: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const s of arr) {
    const key = s.trim().toLowerCase();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(s);
  }
  return out;
}

/**
 * the schools-directory endpoint fuzzy-matches by levenshtein against the
 * first page of all-state results (because the `school_name` filter is
 * silently ignored). this means it can return a totally unrelated school.
 *
 * when the scraper produced a district name, require the seed record's
 * lea_name to have MULTI-token overlap with it — a single shared token is
 * usually a place name ("Austin", "Philadelphia") which isn't enough to
 * distinguish e.g. "Austin Independent School District" (115 schools, real
 * district) from "Austin Discovery School" (1-school charter in the same
 * state). without this, AISD scrapes get seeded into Austin Discovery's LEA
 * and every teacher collapses onto the one charter's address.
 *
 * when there's no scraped district name (rare), fall back to requiring at
 * least one content-word overlap with the candidate school name.
 */
function plausibleSeed(
  record: NCESSchoolRecord,
  candidate: string,
  districtName: string | null,
): boolean {
  const recordTokens = new Set(
    record.school_name.toLowerCase().match(/[a-z0-9]+/g) ?? [],
  );
  const leaTokens = new Set(
    (record.lea_name ?? "").toLowerCase().match(/[a-z0-9]+/g) ?? [],
  );
  const GENERIC = new Set([
    "school", "schools", "the", "of", "and", "at", "for",
    "high", "middle", "elementary", "primary", "academy",
    "district", "public", "unified", "county", "city",
    "township", "twp", "area", "regional", "union", "independent",
    "consolidated", "isd", "usd",
  ]);

  const candidateTokens = (candidate.toLowerCase().match(/[a-z0-9]+/g) ?? []).filter(
    (t) => t.length > 2 && !GENERIC.has(t),
  );
  const districtTokens = (districtName?.toLowerCase().match(/[a-z0-9]+/g) ?? []).filter(
    (t) => t.length > 2 && !GENERIC.has(t),
  );

  // when we have a district name, compare it against the record's LEA tokens.
  // require ≥2 shared content tokens so a single shared place-name ("austin",
  // "philadelphia") doesn't carry the match on its own.
  if (districtTokens.length > 0) {
    const leaContentTokens = [...leaTokens].filter(
      (t) => t.length > 2 && !GENERIC.has(t),
    );
    const leaContentSet = new Set(leaContentTokens);
    const leaShared = districtTokens.filter((t) => leaContentSet.has(t)).length;
    if (leaShared >= 2) return true;
    // one-token match allowed only when it is a tight subset — the record's
    // discriminative tokens must ALSO be a subset of the district's (no extra
    // discriminative tokens added). this rejects "Austin Discovery School"
    // (tokens {austin, discovery}) as a seed for "Austin ISD" (tokens
    // {austin}) while allowing "Deerfield" district scrapes to match the NCES
    // "Deerfield" LEA (both {deerfield}).
    if (districtTokens.length === 1 && leaShared === 1) {
      const extraInLea = leaContentTokens.filter(
        (t) => !districtTokens.includes(t),
      );
      if (extraInLea.length === 0) return true;
    }
    return false;
  }

  // no district name — fall back to the looser per-candidate check.
  for (const t of candidateTokens) {
    if (recordTokens.has(t) || leaTokens.has(t)) return true;
  }
  return false;
}

const STATE_NAME_TO_ABBR: Record<string, string> = {
  "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
  "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
  "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
  "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
  "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
  "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
  "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
  "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
  "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
  "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
  "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
  "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
  "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
};

const US_STATES = [
  "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
  "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
  "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
  "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
  "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
  "DC",
];
const US_STATE_SET = new Set(US_STATES);

/** tries to pull a 2-letter state code from an address string or url */
function extractState(
  address: string | null,
  url: string,
): string | null {
  // primary path: match the "STATE ZIP" suffix at the end of a well-formed
  // postal address. this AVOIDS false matches like "NE" in "12111 NE 1st St"
  // (bellevue WA's district office address — the literal regex \bNE\b hits
  // the street direction before it reaches the real state field).
  if (address) {
    const suffix = address.match(/\b([A-Z]{2})\s+\d{5}(?:-\d{4})?\b/);
    if (suffix && US_STATE_SET.has(suffix[1]!)) return suffix[1]!;

    // secondary: "..., STATE <something>" near a comma — still safer than
    // scanning the whole string. tries the last 2-3 comma-segments.
    const parts = address.split(",").map((s) => s.trim());
    for (let i = parts.length - 1; i >= Math.max(0, parts.length - 3); i--) {
      const m = parts[i]?.match(/^([A-Z]{2})\b/i);
      if (m && US_STATE_SET.has(m[1]!.toUpperCase())) return m[1]!.toUpperCase();
    }
  }

  // check url for .k12.XX.us or .XX.gov pattern
  const k12Match = url.match(/\.k12\.(\w{2})\.us/i);
  if (k12Match) return k12Match[1]!.toUpperCase();
  const govMatch = url.match(/\.([a-z]{2})\.gov\b/i);
  if (govMatch && US_STATE_SET.has(govMatch[1]!.toUpperCase())) {
    return govMatch[1]!.toUpperCase();
  }

  // try full state name match (e.g. "Golden, Colorado 80401" when the 2-letter
  // path didn't hit). more reliable than the whole-string 2-letter scan below.
  if (address) {
    const lower = address.toLowerCase();
    for (const [fullName, abbr] of Object.entries(STATE_NAME_TO_ABBR)) {
      const pattern = new RegExp(`\\b${fullName.replace(/\s+/g, "\\s+")}\\b`, "i");
      if (pattern.test(lower)) return abbr;
    }
  }

  // last-resort whole-string scan (original behavior). matches the first
  // state code it finds — prone to "NE"/"OR"/"IN" false positives from
  // non-state words, so it's the very last fallback.
  if (address) {
    for (const st of US_STATES) {
      const pattern = new RegExp(`\\b${st}\\b`, "i");
      if (pattern.test(address)) return st;
    }
  }

  return null;
}

/**
 * sanity-check that an NCES record actually refers to the site we scraped.
 * the urban-institute schools endpoint silently ignores school_name filters
 * and the districts endpoint fuzzy-matches by name alone, so we frequently get
 * back wrong-city matches for private schools (Choate → "Chester Elementary",
 * Bronx HS of Science → "Bronx Arts & Science Charter") that poison every
 * downstream row. trusting the scraped city/zip as a tiebreaker catches this.
 *
 * returns true if:
 *   - we have no scraped city AND no scraped zip (can't verify; accept)
 *   - NCES record's city or zip agrees with the scraped city/zip
 * returns false otherwise.
 */
function ncesRecordAgreesWithScrapedSite(
  record: NCESSchoolRecord,
  scrapedCity: string | null,
  scrapedZip: string | null,
  scrapedStreetNum: string | null,
  scrapedName: string | null,
): boolean {
  const recordCityLoc = (record.city_location ?? "").toLowerCase().trim();
  const recordCityMail = (record.city_mailing ?? "").toLowerCase().trim();
  const recordZipLoc = (record.zip_location ?? "").replace(/[^0-9]/g, "").slice(0, 5);
  const recordZipMail = (record.zip_mailing ?? "").replace(/[^0-9]/g, "").slice(0, 5);
  const recordStreetLoc = (record.street_location ?? "").toLowerCase().trim();
  const recordStreetMail = (record.street_mailing ?? "").toLowerCase().trim();

  // 1. name-overlap gate: before location checks, the record's school name
  // must share at least one discriminative (non-generic) token with the
  // scraped school name. this catches cases where same-zip different-school
  // pollutes the match (e.g. "The Lawrenceville School" at 2500 Main St
  // matched to "Lawrence Middle School" at 2455 Princeton Pike — both in
  // Lawrenceville NJ 08648, but no shared discriminative tokens).
  if (scrapedName) {
    if (!shareDiscriminativeToken(scrapedName, record.school_name)) return false;
  }

  if (!scrapedCity && !scrapedZip && !scrapedStreetNum) return true;

  // 2. if scrapedStreetNum is present and record's street starts with a
  // different house number, reject. tightest address check — two schools in
  // the same zip almost never share the same leading number.
  if (scrapedStreetNum) {
    const recordStreetNum =
      recordStreetLoc.match(/^\d+/)?.[0] ??
      recordStreetMail.match(/^\d+/)?.[0] ??
      null;
    if (recordStreetNum && recordStreetNum !== scrapedStreetNum) return false;
  }

  // 3. zip is the strongest disambiguator — two schools in the same city can
  // have different zips (e.g. Bronx HS of Science is 10468, Bronx Arts &
  // Science Charter is 10465, both city="Bronx"). when scraped zip is
  // present, require it to match; fall through to city-only only when zip
  // wasn't extracted.
  if (scrapedZip) {
    return recordZipLoc === scrapedZip || recordZipMail === scrapedZip;
  }

  if (scrapedCity) {
    if (recordCityLoc === scrapedCity || recordCityMail === scrapedCity) return true;
    // allow looser match when either side is a multi-word city name (e.g.
    // "New York" vs "New York City" or "Washington" vs "Washington, DC"):
    // require one to contain the other AND share at least 4 chars.
    for (const rc of [recordCityLoc, recordCityMail]) {
      if (!rc || rc === "-1") continue;
      if ((rc.includes(scrapedCity) || scrapedCity.includes(rc)) && rc.length >= 4) {
        return true;
      }
    }
  }
  return false;
}

// generic tokens that carry no discriminative signal — the same set used in
// nces.ts fuzzy matching. kept in sync by hand (small enough set that dedupe
// isn't worth the coupling).
const NAME_GENERIC_TOKENS = new Set([
  "school", "schools", "the", "of", "and", "at", "for",
  "high", "middle", "elementary", "primary", "academy",
  "district", "public", "unified", "county", "city",
  "campus", "institute", "learning", "community", "education",
]);

function shareDiscriminativeToken(a: string, b: string): boolean {
  const toTokens = (s: string) =>
    new Set(
      (s.toLowerCase().match(/[a-z0-9]+/g) ?? []).filter(
        (t) => t.length > 2 && !NAME_GENERIC_TOKENS.has(t),
      ),
    );
  const ta = toTokens(a);
  const tb = toTokens(b);
  if (ta.size === 0 || tb.size === 0) return true; // not enough to judge; accept
  for (const t of ta) if (tb.has(t)) return true;
  return false;
}

/** parses a freeform address string into our Address type */
function parseAddress(raw: string | null): Address | null {
  if (!raw?.trim()) return null;

  // try to split "123 Main St, City, ST 12345" format
  const parts = raw.split(",").map((s) => s.trim());

  if (parts.length >= 3) {
    const lastPart = parts[parts.length - 1]!;
    // primary: "ST 12345" (2-letter code)
    let stateZip = lastPart.match(/^([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$/);
    // fallback: "Connecticut 06795" (full state name)
    if (!stateZip) {
      const fullNameMatch = lastPart.match(/^([A-Za-z]+(?:\s+[A-Za-z]+)?)\s+(\d{5}(?:-\d{4})?)$/);
      if (fullNameMatch) {
        const abbr = STATE_NAME_TO_ABBR[fullNameMatch[1]!.toLowerCase()];
        if (abbr) {
          stateZip = [lastPart, abbr, fullNameMatch[2]!] as unknown as RegExpMatchArray;
        }
      }
    }

    return {
      street: parts.slice(0, -2).join(", "),
      city: parts[parts.length - 2]!,
      state: stateZip?.[1] ?? "",
      zip: stateZip?.[2] ?? lastPart,
      source: "school_website",
    };
  }

  // can't parse reliably — stuff it all in street
  return {
    street: raw,
    city: "",
    state: "",
    zip: "",
    source: "school_website",
  };
}
