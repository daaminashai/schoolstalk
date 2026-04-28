# schoolyank

vibe coded script to extract stem teacher data from a school URL.

this got very complicated, very fast. shoutout to claude code and the ollama free tier. sort by hacker_score descending in the CSV for high-value targets.

to get started:

```bash
git clone https://github.com/Hex-4/schoolyank
python3 -m pip install -r requirements.txt
browser-use install
bun index.ts # if .env doesn't exist, runs setup first
```

make sure you have an OpenRouter key ready. the setup script will configure OpenRouter, and then use it to autonomously sign up for Browser Use (via the agent-specific challenge-response flow) and the Exa search API (via a browser use agent). Keys are saved to .env automatically.

script does this:
- spins up a local browser use agent to figure out if the site is a district or a single school
- the agent traverses staff directories and lists stem teachers
- claude wrote a ton of smart code that validates and normalizes what the agent returned
- sends validated listing to an OpenRouter-backed LLM judge - the judge strips non-stem teachers that slipped in and assigns a hacker score to teachers (CS, robotics, etc get high scores, math and etc get lower scores)
- attempts to validate emails (doesn't work on many home networks)
- hits the NCES government database to get better addresses
- uses Exa's people search vertical to get linkedin URLs and better job titles for teachers
- exports to csv
whole pipeline takes ~3min for a 50-teacher district. main slowdown is the browser use agent.

## usage
the interactive mode (just `bun index.ts`) works great. but if you hate joy and pretty colors, run with flags instead:
- `--url https://example.com` for one url. `--urls-file schools.txt` reads schools one-per-line, ignoring lines that start with #.
- `--schools-csv schools_with_staff_urls.csv` reads everything from that one CSV. Required columns: `Hs ID`, `Name`, `State`, `City`, and `School Homepage`.
- Staff URL hints come from the same row: `Primary URL`, `Candidate 1`, `Candidate 1 Score`, `Candidate 2`, `Candidate 2 Score`, `Candidate 3`, `Candidate 3 Score`, and optional `Verified URL`.
- `--output` or `-o` to change the filename for one-school mode. `--merged-output` merges all results from running on multiple schools into one CSV.
- `--linkedin` to enable linkedin enrichment. recommended.
- `-j <n>` to adjust concurrency in batch mode. each job launches a local browser, so tune this for your machine.
- `--force` to overwrite previous CSVs
- `--debug` dumps detailed information, useful to debug flaky or weird sites

## limitations
- sites that don't expose staff emails -> blank column. the script can infer email formats if given >3 seeds, but can't if the site itself only exposes, say, a contact form
- smtp validation doesn't work on most home networks since port 25 is blocked
- federated districts (with per-school subsites and no combined directory) have limited coverage (10-50%)
- amount of found teachers can vary by like 5% each run and i cant fix it sowwy :(
- doesn't work on reCAPTCHAd sites or login-walled sites
- some sites obfuscate emails. finalsite is handled but not other formats
- Exa LinkedIn index is incomplete (10%-60% hit rate) and the free tier has only ~1k searches per mo

thats it, be free and go spam some teachers /j :)
