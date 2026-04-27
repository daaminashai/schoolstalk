// ── core data model for schoolyank ──

export interface Teacher {
  firstName: string;
  lastName: string;
  email: string | null;
  role: string;
  department: string | null;
  linkedinUrl: string | null;
  sources: DataSource[];
  confidence: ConfidenceScore;
  // 1-5 affinity for Hack Club values (project-based CS / maker / engineering).
  // 5 = CS/coding/software teacher, 1 = math/non-tinker teacher. computed from
  // role + department keywords by the validator.
  hackerScore: HackerScore;
}

export type HackerScore = 1 | 2 | 3 | 4 | 5;

// Legacy stubs retained for compile compatibility (not used at runtime)
export interface Address {
  street: string;
  city: string;
  state: string;
  zip: string;
  source: "nces" | "school_website" | "inferred";
}

export interface SchoolInfo {
  name: string;
  url: string;
  address: Address | null;
  phone: string | null;
  district: string | null;
  ncesId: string | null;
}

export interface DistrictInfo {
  name: string;
  leaId: string | null;
  url: string;
  officeAddress: Address | null;
  officePhone: string | null;
}

export type DataSource =
  | "school_website"
  | "nces"
  | "linkedin"
  | "district_website"
  | "inferred";

export type ConfidenceScore = 1 | 2 | 3 | 4 | 5;

export interface ScrapeResult {
  sourceUrl: string; // canonical URL scraped for this result
  teachers: Teacher[];
  metadata: {
    scrapedAt: string;
    durationMs: number;
    pagesVisited: number;
    browserUseSessionId: string | null;
    warnings: string[];
  };
}

// raw shape returned by the AI extraction before validation
export interface RawTeacherData {
  name: string;
  email?: string;
  role?: string;
  department?: string;
}

// raw shape for site info returned by the scraper ai pass
export interface RawSiteInfo {
  // the official school name for the scraped URL
  name: string | null;
  // optional legacy fields — present in older flows, ignored in current runtime
  address?: string | null;
  schools?: string[];
  schoolGroups?: Array<{ umbrella: string; members: string[] }>;
}

// Legacy NCES type stubs (not used at runtime)
export interface NCESDistrictRecord {
  lea_name: string;
  leaid: string;
  state_location: string;
  street_mailing: string;
  city_mailing: string;
  state_mailing: string;
  zip_mailing: string;
  street_location: string;
  city_location: string;
  phone: string;
}

export interface NCESSchoolRecord {
  school_name: string;
  ncessch: string;
  leaid: string;
  street_mailing: string;
  city_mailing: string;
  state_mailing: string;
  zip_mailing: string;
  street_location: string;
  city_location: string;
  state_location: string;
  zip_location: string;
  phone: string;
  lea_name: string;
  teachers_fte: number;
  school_level: number;
  fips: number;
}

// config passed through the pipeline
export interface ScrapeConfig {
  schoolUrl: string;
  outputPath: string;
  /**
   * Optional: pre-seeded staff directory URLs to try in order before running
   * the generic directory-discovery step. Typically sourced from staff_urls.csv
   * when running in --schools-csv mode. If none of these yield teachers, the
   * scraper falls back to the normal discovery flow.
   */
  preferredDirectoryUrls?: string[];
  /**
   * Optional: high_schools.id from the canonical roster. When present (always
   * true in --schools-csv mode), the orchestrator upserts each school's
   * teachers into public.extracted_teachers as soon as the CSV is written.
   */
  hsId?: number;
}
