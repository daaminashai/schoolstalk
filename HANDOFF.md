# schoolyank session handoff

snapshot of where things stand after the 2026-04-17 session (day 2 — added llm judges, batch mode, email validation, CLI flags). start here if you're picking this up fresh.

## what it does

extracts STEM teacher contact data from any K-12 school or district website → CSV with names, emails, roles, departments, per-teacher school + address (NCES-verified), and optional LinkedIn profile URLs + job titles.

## run it

```bash
bun install
cp .env.example .env   # set BROWSER_USE_API_KEY, optionally EXA_API_KEY + AI_BASE_URL
bun index.ts                                            # interactive (one school)
bun index.ts --url https://cvsdvt.org --linkedin        # one school, non-interactive
bun index.ts --urls-file schools.txt --linkedin         # batch (3-way parallel)
bun index.ts --help                                     # full flag reference
```

the interactive CLI prompts for a school URL and whether to enable LinkedIn enrichment. if LinkedIn is enabled and no Exa key is set, a setup wizard opens the Exa signup page, prompts for the pasted key, and saves it to `.env` for future runs. non-interactive mode skips prompts; missing EXA_API_KEY falls back to DDG scraping with a warning.

batch mode launches one local browser per concurrent scrape, skips any school whose output csv already exists (override with `--force`), retries each failing school once before giving up, and writes both per-school `output/<slug>.csv` files + a merged `output/all.csv` covering every teacher across every school.

## pipeline (6 phases)

1. **classify** — is the URL a district or single-school site? (structured zod output via browser-use). follow-up umbrella-probe task drills into plural "X Schools" labels and enumerates their member schools so downstream assignment goes per-building instead of per-umbrella.
2. **directory** — find staff directory URL(s) (navigation context for the next task)
3. **extract** — pull every STEM teacher with per-teacher school assignment (structured Zod/Pydantic output through the local Browser Use runner; model comes from `AI_MODEL` or `BROWSER_USE_*_MODEL` overrides)
4. **NCES verify** — resolve each school to its federal NCES record for canonical address/phone
5. **LLM judge passes** — one batch call classifies STEM + hacker score per teacher; one batch call adjudicates LinkedIn candidates. Keyword fallbacks run silently if the LLM errors.
6. **email validation** — DNS MX lookup + SMTP RCPT TO probe (one connection per domain, reused for every teacher). Confirmed 550/551/553 = null the email out; timeouts = inconclusive, kept.
7. **linkedin** — optional: batch Exa candidates (name + not-retired prefilter) → LLM picks the true match per teacher. Falls back to keyword validator on LLM failure.
8. **export** — write per-school CSV + optional merged CSV

## stack

| concern | solution |
| --- | --- |
| site navigation + extraction | local `browser-use` Python runner with Zod/Pydantic structured output |
| federal school data | Urban Institute NCES API (free, no auth) |
| linkedin profile lookup | Exa `category: "people"` (1000 req/mo free) |
| linkedin fallback | DDG HTML scrape (always on, rate-limited) |
| STEM classification + hacker score + linkedin judge | OpenRouter via the OpenAI-compatible endpoint (see `src/judge.ts`) — batch calls, keyword fallbacks |
| email validation | DNS MX + SMTP RCPT TO (see `src/emailValidator.ts`) — no API keys |
| CLI | `@clack/prompts` interactive + custom argv parser for batch/non-interactive mode |

only `BROWSER_USE_API_KEY` is required. `EXA_API_KEY` → better LinkedIn recall. `AI_BASE_URL` + `AI_MODEL` + `AI_API_KEY` → LLM judges (any openai-compatible endpoint). missing any optional key degrades gracefully with a warning.

## key design decisions

- **district-aware data model** — teachers carry `schoolName` + `schoolNcesId` per-row. `ScrapeResult` holds `district: DistrictInfo | null` + `schools: SchoolInfo[]`. Fixes the old bug where every CVU teacher got stamped with the district office address.
- **umbrella school handling** — some sites label a shared campus with a plural name ("Williston Schools") covering multiple real schools. NCES often lists only the umbrella. The scraper strips umbrella labels from `schools[]` via two signals (model-reported `schoolGroups` + plural-"Schools" heuristic) and the orchestrator's second-chance routing maps unmatched scraped schools to the umbrella's NCES record.
- **linkedin validation is 3-phase**: name must match the profile title → title must NOT mark the person as retired/former → title must mention the teacher's actual school (employer context) OR have a K-12 educator keyword if no employer is in the title. This catches wrong-profession same-name matches (UK machinery salesman "Steve Flint") and wrong-school matches (Samantha Kayhart's profile at Mount Abraham vs her actual CVU job).
- **structured output everywhere** — browser-use's `save_output_json()` tool made the agent save data to its own scratchpad instead of returning it. Zod schemas on `client.run()` force the agent to return conforming data in `result.output`.
- **noisy agent chatter filtered at the stream boundary** — "Output saved to output.json", "Running Python code", `save_output_json()` references are dropped in `browser.ts` so they never reach the UI.

## known working state

as of this session, a clean run against `cvsdvt.org` produces:
- 6 detected schools (Williston Schools umbrella split correctly into Williston Central + Allen Brook)
- ~45-49 STEM teachers extracted with clean role / department / school / email columns
- 100% NCES address coverage
- 15-20 LinkedIn matches (roughly 35-40%, realistic recall for Exa's people index)
- avg confidence 5/5 for correctly-scraped data

total runtime: ~3 min (was ~12 min before Finalsite-specific prompt hints landed).

## the tricky bits (if something breaks)

- **Finalsite CMS**: ~60% of K-12 district sites run on Finalsite. The scraper prompt has explicit hints (use `?const_search_keyword=<subject>` to reveal subject-specific titles, decode emails by reversing both the domain AND username passed to `FS.util.insertEmail`, paginate with `?const_page=N`). Without these hints, a Finalsite site can take 10+ minutes as the agent rediscovers the pattern.
- **Exa's people category is not exhaustive** — some real linkedin profiles aren't indexed. Expect ~40-60% recall, not 100%. A "no match" for a teacher who has a public profile is correct behavior given the data.
- **validator depends on canonical email domain** — if the user types a URL with a typo (`cvs-dvt.org` instead of `cvsdvt.org`), the old code compared URL-domain against email-domain and dropped confidence to 1 for everyone. Fixed: we now infer the canonical email domain from the majority of teacher emails. URL is only a last resort.
- **the browser-use SDK model param** is per-`client.run()`, NOT `client.sessions.create()`. Setting it on session is silently ignored.

## what's NOT done / known limitations

- **multi-umbrella districts**: the second-chance routing picks the right umbrella via first-word match or transitive schoolGroups membership. Districts with multiple umbrellas AND no clear name signal fall back to the district-office address with a warning. No known real case hit yet.
- **LinkedIn recall is provider-bound**: Exa's people index is the recall ceiling. Can't do better without switching to Proxycurl ($0.10/lookup) or similar paid APIs.
- **Exa setup wizard can't auto-fill the key**: browser automation of the Exa signup would violate their TOS. The wizard opens the dashboard and prompts the user to paste — ~45 seconds of human-in-the-loop.
- **art / music / non-STEM edge cases**: a teacher labeled "Digital Learning Leader /Art Teacher" may leak through as STEM because "digital learning" matches. Can tighten by splitting compound titles before keyword check.

## file map

```
index.ts                      argv parser + interactive CLI + batch runner
                              (preflight check for AI config, retry-once
                              on browser-use failures, merged-csv writer)
src/
  orchestrator.ts             pipeline glue — all phases, NCES routing, LLM judges,
                              email validation, warnings
  scraper.ts                  browser-use tasks + Finalsite-aware prompts +
                              umbrella-probe follow-up task
  linkedin.ts                 two-phase Exa enrichment (fetch candidates → batch
                              LLM judge) + DDG fallback + exa rate-limit token bucket
  judge.ts                    batch LLM judges: STEM+hacker_score, linkedin match
  emailValidator.ts           DNS MX + SMTP RCPT TO, no API keys, port-25 fast-fail
  nces.ts                     Urban Institute API client + fuzzy matcher
  validator.ts                name/email/dedup/confidence + keyword fallbacks for
                              STEM and hacker_score (used when LLM judge errors)
  csv.ts                      per-teacher school lookup + per-school + merged CSV
  browser.ts                  browser-use SDK wrapper (runTask + runTaskStructured)
  ai.ts                       openai-compatible llm client used by judge.ts
  types.ts                    shared types (Teacher, SchoolInfo, DistrictInfo, etc.)
  utils.ts                    name/email normalization, fuzzy helpers
```

## cross-session memory

there's additional context auto-loaded in future Claude sessions from `/home/hexatron/.claude/projects/-home-hexatron-code-schoolyank/memory/`:
- `project_goal.md` — "perfect data" north star
- `project_architecture.md` — same pipeline explanation
- `project_test_district.md` — CVSD test case specifics (known-good + known-false-positive teachers)
- `project_dead_ends.md` — approaches that don't work (don't re-explore)
- `reference_external_apis.md` — endpoints, auth patterns, gotchas
- `feedback_no_narration.md` — don't write TODO-prose
- `feedback_auto_mode_action.md` — auto mode = execute, not ask
- `feedback_read_the_output.md` — verify via CSV, not summary
