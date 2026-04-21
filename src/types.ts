// ── core data model for schoolyank ──

export interface Teacher {
  firstName: string;
  lastName: string;
  email: string | null;
  role: string;
  department: string | null;
  // the specific school this teacher is assigned to. in single-school mode this
  // matches the top-level school; in district mode it's the feeder school
  // (williston central, hinesburg community, etc.).
  schoolName: string | null;
  schoolNcesId: string | null;
  phoneExtension: string | null;
  linkedinUrl: string | null;
  sources: DataSource[];
  confidence: ConfidenceScore;
  // 1-5 affinity for Hack Club values (project-based CS / maker / engineering).
  // 5 = CS/coding/software teacher, 1 = math/non-tinker teacher. computed from
  // role + department keywords by the validator.
  hackerScore: HackerScore;
}

export type HackerScore = 1 | 2 | 3 | 4 | 5;

export interface SchoolInfo {
  name: string;
  url: string;
  address: Address | null;
  phone: string | null;
  district: string | null;
  ncesId: string | null;
}

// district-level info — populated when the scraped url is a district site
// covering multiple schools rather than a single school.
export interface DistrictInfo {
  name: string;
  leaId: string | null;
  url: string;
  officeAddress: Address | null;
  officePhone: string | null;
}

export interface Address {
  street: string;
  city: string;
  state: string;
  zip: string;
  source: "nces" | "school_website" | "inferred";
}

export type DataSource =
  | "school_website"
  | "nces"
  | "linkedin"
  | "district_website"
  | "inferred";

export type ConfidenceScore = 1 | 2 | 3 | 4 | 5;

export interface ScrapeResult {
  // district is null for single-school scrapes, populated for district sites.
  district: DistrictInfo | null;
  // always non-empty: one school for single-school mode, multiple for districts.
  schools: SchoolInfo[];
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
  phone?: string;
  // the specific school this teacher works at, when the site covers multiple
  // schools (typical for district sites). left empty for single-school sites.
  assignedSchool?: string;
}

// raw shape for district/school site info returned by the scraper ai pass
export interface RawSiteInfo {
  siteType: "district" | "school";
  // the primary name: district name when siteType is district, school name otherwise.
  name: string | null;
  // primary mailing address: district office in district mode, school in school mode.
  address: string | null;
  // list of feeder schools when siteType is district; ignored for school mode.
  schools?: string[];
  // when a shared campus hosts multiple schools under one umbrella name (e.g.
  // "Williston Schools" = "Williston Central School" + "Allen Brook School"),
  // record the grouping. the federal NCES data often lists only the umbrella,
  // so we use these mappings as a matcher fallback when a specific school
  // can't be found directly in the NCES roster.
  schoolGroups?: Array<{ umbrella: string; members: string[] }>;
}

// nces district (LEA) directory endpoint shape (subset). note the district
// endpoint has different field set than the school endpoint — no ncessch,
// different location fields, different phone.
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

// nces api response shape (subset of fields we care about)
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
}
