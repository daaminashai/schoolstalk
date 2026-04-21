# schoolyank — design document

> given any arbitrary school website url, extract science/math/stem teacher names, emails, mailing addresses, and roles to csv with the highest possible data quality.

---

## 1. data sources (ranked by authority)

### tier 1 — school website (primary source, via browser-use)

the school's own website is the single most authoritative source for current staff. if a teacher is listed on the school site, they work there right now.

**what we extract:**
- teacher names (first, last)
- email addresses (mailto links, on-page text, js-rendered)
- role / title / department (e.g. "ap physics teacher", "math department chair")
- phone extensions (when available)

**challenges:**
- every school site is structured differently
- staff directories can be: flat lists, card grids, tabbed by department, paginated, pdf-only, or nested 3 levels deep
- common directory platforms: finalsite, blackboard, schoolpointe, powerschool, custom cms
- emails sometimes obfuscated with js or images
- some schools split staff across department sub-pages (e.g. `/science-department`, `/math-department`)

**browser-use handles:** js rendering, stealth browsing, captchas, pagination navigation, dynamic content loading

### tier 2 — nces / urban institute education data portal (address verification)

the us department of education's common core of data (ccd) is the federal gold standard for school information. the urban institute exposes it via a free, open, no-auth-required rest api.

**endpoint:** `https://educationdata.urban.org/api/v1/schools/ccd/directory/{year}/`

**what we get:**
- `school_name` — official registered name
- `street_mailing`, `city_mailing`, `state_mailing`, `zip_mailing` — **verified mailing address**
- `street_location`, `city_location`, `state_location`, `zip_location` — physical address
- `phone` — main phone number
- `ncessch` — unique nces school id
- `lea_name` — school district name
- `latitude`, `longitude` — geocoordinates
- `school_level` — primary/middle/high
- `teachers_fte` — full-time equivalent teacher count (sanity check)

**why this matters:** school website footers often have abbreviated or outdated addresses. the nces mailing address is verified annually by the department of education. this is the address the bounty is asking for.

**lookup strategy:** we search by school name + state. the api supports filtering by `school_name` and `state_location`. if the url contains the school name (which it almost always does, e.g. `lincolnhigh.edu`), we can extract it.

### tier 3 — linkedin (enrichment, via browser-use with profile auth)

linkedin is the best source for **verifying** teacher data and filling gaps. not every school site lists emails or full titles.

**what we get:**
- current position verification (confirms they still work at that school)
- full professional title (e.g. "stem coordinator & ap computer science teacher")
- education background
- linkedin profile url (enrichment data)

**how it works:**
1. sync linkedin cookies to browser-use via `profile sync` (one-time setup)
2. for each teacher found on the school site, search linkedin: `"{teacher name}" "{school name}"`
3. verify the match (same school, same role domain)
4. extract enrichment data

**rate limiting:** linkedin aggressively throttles. we batch searches with delays and cap at ~50 lookups per session. linkedin enrichment is **optional** — the tool works without it but produces richer data with it.

### tier 4 — district website (fallback source)

many schools are part of a larger district that maintains a centralized staff directory. if the school's own site has a sparse or missing directory, the district site is the fallback.

**detection:** browser-use agent checks if the school url redirects to or is hosted on a district domain (e.g. `schools.district.k12.state.us`). if so, navigate to the specific school's sub-section.

### sources we are NOT using

| source | reason |
|--------|--------|
| composio | orchestration platform, no unique data |
| hunter.io / clearbit | paid, and school emails are already public |
| rate my teachers | no contact data, just reviews |
| state certification databases | too many different portals (50 states), requires solving captchas per state, and the data doesn't add email/address info we don't already have |
| google places api | costs money, and nces gives us better verified data for free |

---

## 2. architecture

```
┌─────────────────────────────────────────────────────────┐
│                     cli (clack/prompts)                  │
│            user inputs school url + options              │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│                   orchestrator (index.ts)                │
│        coordinates all phases, manages state             │
└──┬──────────┬───────────┬──────────┬────────────────────┘
   │          │           │          │
   ▼          ▼           ▼          ▼
 phase 1    phase 2     phase 3    phase 4
 school     nces        linkedin   csv
 scrape     lookup      enrich     export
```

### phase 1 — school website scrape (browser-use)

**goal:** find all science/math/stem teachers on the school website.

**browser-use session strategy:** use follow-up tasks within a single session so the browser state (page, cookies, tabs) carries over between tasks.

**task 1: discover staff directory**
```
prompt: "go to {url}. find the staff directory, faculty page, or 
teacher listing. look for links labeled 'staff', 'faculty', 
'our team', 'directory', 'teachers', 'staff directory', 
'faculty & staff'. if you find department-specific pages for 
science, math, or stem, note those urls too. report back all 
staff/faculty page urls you find."
```

**task 2: extract teacher data**
```
prompt: "navigate to the staff directory pages found. extract ALL 
teachers who teach science, math, stem, physics, chemistry, 
biology, environmental science, earth science, computer science, 
engineering, technology, algebra, geometry, calculus, statistics, 
precalculus, trigonometry, ap science, ap math, or any related 
subject.

for each teacher extract:
- full name (first and last)
- email address
- role/title/position
- department
- phone extension (if listed)

handle pagination — if there are multiple pages, click through 
all of them. if teachers are organized by department, check 
each relevant department page.

return the data as a JSON array."
```

**task 3: extract school address**
```
prompt: "find the school's mailing address. check the footer, 
the 'contact us' page, or the 'about' page. return the full 
address including street, city, state, and zip code."
```

### phase 2 — nces address verification

**goal:** cross-reference the school with the federal nces database to get the verified mailing address.

**strategy:**
1. extract school name from the url or from phase 1 results
2. query urban institute api: `GET /api/v1/schools/ccd/directory/2023/?school_name={name}&state_location={state}`
3. if multiple results, use fuzzy matching on school name + city to find the best match
4. extract `street_mailing`, `city_mailing`, `state_mailing`, `zip_mailing`
5. compare with the address found on the school website
6. **prefer nces address** (it's verified by the dept of education) but flag discrepancies

**address resolution priority:**
1. nces mailing address (highest confidence)
2. nces physical address (if mailing differs)
3. school website address (fallback if nces lookup fails)

### phase 3 — linkedin enrichment (optional)

**goal:** verify teacher data and fill gaps for teachers missing emails or clear role descriptions.

**prerequisites:**
- user must have synced linkedin cookies via browser-use profile sync
- user explicitly opts in (linkedin enrichment is off by default)

**strategy:**
1. create a browser-use session with the linkedin profile id
2. for each teacher missing an email OR with an ambiguous role:
   - search linkedin: `"{first} {last}" "{school name}"`
   - verify match: same school name in current position
   - extract: full title, profile url
3. rate limit: 2-second delay between searches, max 50 per session

### phase 4 — csv export

**goal:** produce a clean, enriched csv file.

**columns:**
```
first_name, last_name, email, role, department, school_name, 
school_address, school_city, school_state, school_zip, 
school_phone, school_district, linkedin_url, 
data_source, confidence_score
```

**data quality fields:**
- `data_source`: comma-separated list of where each field came from (e.g. "school_website,nces,linkedin")
- `confidence_score`: 1-5 rating based on how many sources corroborate the data
  - 5: name + email + role all confirmed by school site, address from nces
  - 4: name + role confirmed, email from school site, address from nces
  - 3: name + role confirmed, email inferred from pattern, address from school site
  - 2: name confirmed, role or email uncertain
  - 1: partial data, needs manual verification

---

## 3. stem subject detection

the llm needs to correctly identify science/math/stem teachers. here's the comprehensive keyword taxonomy:

### math
algebra, geometry, calculus, precalculus, pre-calculus, trigonometry, statistics, probability, ap calculus, ap statistics, integrated math, math, mathematics, finite math, discrete math, multivariable calculus, linear algebra, math analysis

### science
physics, chemistry, biology, ap physics, ap chemistry, ap biology, environmental science, apes, earth science, geology, anatomy, physiology, forensic science, marine biology, astronomy, ap environmental science, physical science, life science, general science, science

### stem / technology / engineering
computer science, ap computer science, cs, engineering, stem, technology, robotics, coding, programming, information technology, it, cte (career and technical education, when stem-related), maker space, digital electronics, principles of engineering

### exclusions (common false positives)
- "political science" → not a stem teacher
- "science of cooking" → not a stem teacher  
- "sports science" → maybe, flag for review
- "social science" → not a stem teacher
- "library science" → not a stem teacher
- "exercise science" → not a stem teacher

the browser-use prompt includes these keywords so the llm agent applies them during extraction. we also do a post-processing pass locally to catch any the agent missed.

---

## 4. data quality pipeline

### 4a. email validation

```
step 1: format check — valid email regex
step 2: domain check — email domain matches school website domain
step 3: pattern detection — if we find 3+ emails like first.last@school.edu,
        infer emails for teachers who are missing them
step 4: deduplication — remove exact duplicates
```

### 4b. name normalization

```
- strip prefixes: mr., mrs., ms., dr., prof.
- strip suffixes: jr., sr., iii, ph.d., m.ed., ed.d.
- title case: "john doe" → "John Doe"
- handle "last, first" format → "first last"
```

### 4c. role normalization

```
- standardize titles: "AP Chem Teacher" → "AP Chemistry Teacher"
- extract department from role if not separately listed
- flag ambiguous roles for human review
```

### 4d. address standardization

```
- use nces format as canonical
- standardize abbreviations: St → Street, Ave → Avenue, Dr → Drive
- ensure zip code is 5-digit or 5+4 format
- validate state abbreviation is 2-letter usps format
```

### 4e. cross-reference validation

```
- if nces says school has 45 FTE teachers and we found 12 stem teachers,
  that's plausible (~25% stem is normal)
- if we found 45 stem teachers at a school with 45 total, something's wrong
- if a teacher's email domain doesn't match the school website domain, flag it
- if linkedin shows a different school, exclude that teacher
```

---

## 5. file structure

```
schoolyank/
├── index.ts              # cli entrypoint (clack/prompts)
├── src/
│   ├── orchestrator.ts   # coordinates all phases
│   ├── browser.ts        # browser-use sdk client setup
│   ├── scraper.ts        # phase 1: school website scraping
│   ├── nces.ts           # phase 2: nces/urban institute api
│   ├── linkedin.ts       # phase 3: linkedin enrichment
│   ├── csv.ts            # phase 4: csv generation
│   ├── validator.ts      # data quality pipeline
│   ├── types.ts          # shared types
│   └── utils.ts          # helpers (fuzzy match, url parsing)
├── output/               # generated csv files
├── .env                  # BROWSER_USE_API_KEY
├── package.json
└── DESIGN.md             # this file
```

---

## 6. types

```typescript
interface Teacher {
  firstName: string;
  lastName: string;
  email: string | null;
  role: string;
  department: string | null;
  phoneExtension: string | null;
  linkedinUrl: string | null;
  sources: DataSource[];
  confidence: 1 | 2 | 3 | 4 | 5;
}

interface SchoolInfo {
  name: string;
  url: string;
  address: Address;
  phone: string | null;
  district: string | null;
  ncesId: string | null;
}

interface Address {
  street: string;
  city: string;
  state: string;
  zip: string;
  source: "nces" | "school_website" | "google";
}

type DataSource = "school_website" | "nces" | "linkedin" | "district_website" | "inferred";

interface ScrapeResult {
  school: SchoolInfo;
  teachers: Teacher[];
  metadata: {
    scrapedAt: string;
    durationMs: number;
    pagesVisited: number;
    browserUseSessionId: string;
    warnings: string[];
  };
}
```

---

## 7. cli flow

```
┌───────────────────────────────────────┐
│         ◆ schoolyank                  │
│                                       │
│  school url                           │
│  > https://example-school.edu         │
│                                       │
│  enable linkedin enrichment?          │
│  ○ yes (requires profile sync)        │
│  ● no                                 │
│                                       │
│  output file                          │
│  > ./output/example-school.csv        │
│                                       │
│  ◇ starting scrape...                 │
│  ├ phase 1: crawling school website   │
│  │ ├ found staff directory            │
│  │ ├ found 47 total staff             │
│  │ └ filtered to 14 stem teachers     │
│  ├ phase 2: verifying with nces       │
│  │ ├ matched: Example High School     │
│  │ └ verified mailing address         │
│  ├ phase 3: linkedin enrichment       │
│  │ └ skipped (not enabled)            │
│  └ phase 4: exporting csv             │
│    └ wrote 14 teachers to csv         │
│                                       │
│  ◆ done! output/example-school.csv    │
└───────────────────────────────────────┘
```

---

## 8. error handling & edge cases

| edge case | handling |
|-----------|----------|
| school site has no staff directory | check district website as fallback. if still nothing, report error with suggestions. |
| staff directory is a pdf | browser-use can navigate to the pdf. extract text and parse with llm. |
| email addresses are images (anti-scrape) | browser-use sees rendered page. llm can read text from screenshots. |
| paginated directory (100+ teachers) | browser-use agent clicks through all pages. prompt explicitly says to handle pagination. |
| department-specific pages | agent visits each stem department page separately. |
| school is a private school | nces has separate private school data (`pss` source). fall back to school website address. |
| school not found in nces | use school website address. lower confidence score. |
| teacher has no email listed | try to infer from email pattern of other teachers at same school. flag as inferred. |
| ambiguous role (e.g. "teacher") | if no department/subject info, exclude. better to miss one than include a false positive. |
| school url redirects to district site | detect redirect, navigate to specific school section within district site. |
| browser-use session timeout | sessions timeout after 15 min inactivity. each phase uses its own session if needed. |
| rate limiting on linkedin | 2-second delay between searches. max 50 per session. gracefully degrade. |

---

## 9. quality benchmarks

for the bounty to be accepted, it needs to work on 15 different school websites. our targets:

| metric | target |
|--------|--------|
| teacher detection rate | ≥95% of stem teachers listed on the site |
| email accuracy | 100% — no wrong emails |
| name accuracy | 100% — no misspellings |
| role accuracy | ≥90% — correct subject/department |
| address accuracy | 100% — nces verified |
| false positive rate | <5% — very few non-stem teachers included |
| runtime per school | <3 minutes |

---

## 10. prior art

the us census bureau built a nearly identical system for the national teacher and principal survey (ntps). their research papers ([fcsm 2022](https://nces.ed.gov/surveys/ntps/pdf/research/FCSM_2022_NTPS_Web_Scraping.pdf), [fcsm 2023](https://nces.ed.gov/surveys/ntps/pdf/research/FCSM_2023_Automated_Staff_Scraping.pdf)) document:

- using google places api to find school websites (we skip this since user provides the url)
- string similarity scoring to detect staff directory pages (we use llm instead — strictly better)
- named entity recognition for extracting names/titles/emails (we use llm instead — strictly better)
- html motif detection for structured data (browser-use handles this natively)
- relationship extraction via html tree traversal (llm does this implicitly)

our approach is essentially a next-generation version of theirs: same pipeline structure (query → crawl → extract), but replacing every manual/ml component with browser-use's ai agent, which is more robust to website variation and requires zero per-site configuration.

---

## 11. cost estimate

| component | cost |
|-----------|------|
| browser-use cloud | ~$0.10-0.50 per school (3-5 tasks × ~$0.05-0.10 per task) |
| nces api | free |
| linkedin enrichment | ~$0.10-0.30 per school (if enabled) |
| **total per school** | **~$0.10-0.80** |

---

## 12. implementation order

1. **types.ts** — define all interfaces
2. **nces.ts** — simplest module, pure http, testable independently
3. **browser.ts** — browser-use client setup
4. **scraper.ts** — school website extraction (the core)
5. **validator.ts** — data quality pipeline
6. **csv.ts** — csv generation
7. **linkedin.ts** — optional enrichment
8. **orchestrator.ts** — wire everything together
9. **index.ts** — cli with clack/prompts
10. **testing** — run against 5+ school websites, iterate on prompts
