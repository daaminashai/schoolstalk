# schoolyank

mega-scraping script to retrieve 600k+ teachers and ranking them based on hacker compatibility. 
it goes from a school's website -> all teachers.

to get started:

```bash
git clone <this repo> schoolyank
cd schoolyank
bun install
python3 -m pip install -r requirements.txt
browser-use install
cp .env.example .env
bun index.ts --preflight
bun index.ts
```

first run launches an interactive setup wizard: you paste an openrouter key, pick a model, optionally let it spin up an exa account in a disposable browser, then it drops the keys into .env and kicks off the scrape.

what it does:
- checks for apptegy/finalsite shortcuts first (hasura + thrillshare) before spending tokens on the agent
- spins up the local browser-use runner with a structured prompt to crawl staff directories, tabs, filters, pdf links, whatever it takes
- normalizes the agent output (name parsing, email cleanup, dedupe, role scrub, email pattern inference) without dropping non-stem teachers anymore
- resolves the school in the urban institute nces api for verified addresses + phone numbers and logs why if the match fails
- pings dns mx + smtp rcpt to for every email; nukes addresses the server rejects, keeps inconclusive ones untouched
- writes clean csvs to `output/` and, in batch mode, a merged `output/all.csv`
- pushes rows into postgres when `DATABASE_URL` is set so downstream jobs can keep going even if the cli dies
- streams the play-by-play into slack if `SLACK_BOT_TOKEN` + `SLACK_CHANNEL_ID` show up so you can watch the chaos from your phone

## usage

interactive mode (`bun index.ts`) is the guided flow: asks for one url and prints output.

- `--url https://example.com` scrape one site with no prompts (repeatable)
- bare positional urls work too: `bun index.ts https://a.edu https://b.org`
- `--urls-file urls.txt` load one url per line (lines starting with `#` ignored)
- shorthand `@schools_with_staff_urls.csv` is the same as `--schools-csv`
- `--output path.csv` choose the filename when you pass exactly one url
- `--merged-output batch.csv` override the merged csv path (default `output/all.csv`)
- `--concurrency N` / `-j N` adjust parallel browsers (default 6); `--max` doubles cores up to `SCHOOLYANK_MAX_CONCURRENCY`
- `--force` re-scrape even if the csv already exists or another worker claimed it
- `--interactive` force the cute cli even when you provided urls
- `--debug` firehose every agent/nces/email log line to stderr
- `--preflight` run environment + dependency checks and exit (same as `bun index.ts --preflight`)
- every batch run writes progress to `output/status.csv` (set `STATUS_CSV_PATH` to move it)

## env + config

minimal requirement: an OpenRouter key.

- `OPENROUTER_API_KEY` (required) + optionally `AI_MODEL`, `AI_BASE_URL`
- `EXA_API_KEY` (optional) powers the ranking/enrichment scripts; the main scrape currently runs without it and falls back to DDG when those scripts need web search
- `SLACK_BOT_TOKEN`, `SLACK_CHANNEL_ID`, `SLACK_ALERT_USER` (optional) enable slack threading + error pings
- `DATABASE_URL` (optional) turns on the postgres sink; set `REQUIRE_DATABASE=true` if csv-only mode should hard fail
- concurrency and startup knobs live behind envs: `SCHOOLYANK_MAX_CONCURRENCY`, `SCHOOLYANK_MAX_SCRAPE_ATTEMPTS`, `SCHOOLYANK_MAX_RATE_LIMIT_ATTEMPTS`, `SCHOOLYANK_BROWSER_START_*`
- logging: set `LOG_FILE` to capture a rolling log, `STATUS_CSV_PATH` to relocate the status tracker
- docker compose uses the same envs; `docker-compose up` brings up postgres + the scraper worker if you like running this in a box

## outputs

- default per-school csv: `output/<slugified-domain>.csv`
- schools csv mode writes to `schools/<STATE>/<city>/<Hs ID>.csv` so nothing collides
- merged batch csv lands at `output/all.csv` unless you override it
- status tracker lives at `output/status.csv` (per run)
- when postgres is configured the upsert target is `public.extracted_teachers`
- log files go wherever `LOG_FILE` says; otherwise stdout/stderr is all you get

## limitations

- no staff directory → no teachers; portals that hide everything behind a parent login still beat the agent
- smtp validation needs port 25 — most home ISPs block it, so expect “inconclusive” spam in that case
- captchas, sso walls, and “enter your student id” forms still end the ride
- directories that obfuscate emails with weirdo javascript (other than finalsite) require manual cleanup
- output counts can wobble by a few teachers run-to-run because the agent explores in a slightly different order every time
- long-running batches should stay under whatever your computer can cool; `--max` will happily light all cores

## side quests

- `bun index.ts --help` prints the full flag bible with prettier formatting
- `bun run preflight` standalone env check (same as the `--preflight` flag)
- `bun run stats` quick aggregate counts over everything in `schools/`
- `bun run clean:teachers` re-validates `dist/teachers.csv` in chunks with OpenAI web search (set `OPENAI_API_KEY` first)
- `bun run rank` scores teachers out of 100, pulls web presence (Exa/DDG), and emits ranked csvs per school
- `bun run typecheck` keeps the bun + typescript types honest
- python helpers live in `scripts/` for bulk csv surgery if you enjoy command-line archaeology

thats it. go spam some teachers!! (legally) and send them something nice.
