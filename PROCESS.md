# Process

This covers how I built the project, what I'd change for a real client, how I used AI, and what I found hardest. I numbered my design decisions D1–D37 as I went. [notes/decisions.md](notes/decisions.md) has each one with its evidence and the options I rejected, so I link to those here rather than repeat them. The answers, setup and commands are in the [README](README.md).

## Architecture

### The overall shape

The project splits at the Volume. Ingestion runs on my laptop, and everything from the raw files onward runs in Databricks as a single serverless, triggered Spark Declarative Pipeline.

That split wasn't my first choice. I wanted ingestion inside Databricks, but Free Edition's serverless compute couldn't resolve `download.bls.gov` or `honolulu-api.datausa.io`. It couldn't resolve `example.com` either, while `pypi.org` worked fine, so this was the platform's outbound allowlist and nothing to do with BLS. Verifying my account with LinkedIn, which is supposed to unlock outbound access, made no difference, even after restarting compute. So ingestion runs locally and writes into Unity Catalog through the Databricks SDK Files API (D1).

I wrote it so that it doesn't care where it runs. It keeps no state on my machine, uses no local paths, and its only machine-specific input is an environment variable. On a workspace that allowed those two domains, the same package could run as a scheduled serverless job.

The data lives in three Volumes, each with one job (D8, D10). `raw` holds exactly what the sources publish right now, and it's the only Volume the pipeline reads. `archive` holds every version of every file I've ever written, named by its SHA-256. `state` holds the ingestion manifest. Using separate Volumes rather than separate folders means a careless path pattern can't pick up state or history by accident, and each Volume can get its own permissions.

### Re-running ingestion safely

The brief says a re-run has to cope with the source adding, changing or removing files, without reprocessing anything already ingested. I split that into two questions (D7).

The first is whether to download a file at all. Ingestion reads the BLS directory listing, so no filenames are hardcoded, and compares each file's timestamp and size, as raw strings, with what the manifest recorded last time. Only new or changed files get downloaded. The listing parser is deliberately strict: if it can't parse a row, the run stops. Otherwise a change in the page format could make every file look deleted.

The second is whether to write what was downloaded. Each file is hashed and only written if its content actually changed. If only the listing metadata changed, just the manifest is updated, so a file BLS touched without changing doesn't get downloaded on every run forever. I'd hoped to use ETags for this, but the BLS server derives them from modification time, so they say nothing about content.

Writes happen in a fixed order: the archive copy, then `raw`, then the manifest entry (D10). If a run dies partway, the manifest still has the old hash, so the next run repeats the same writes with the same bytes. Archive copies are named by their hash, so rewriting one is harmless. Even losing the manifest entirely is recoverable: re-running re-lands everything byte for byte, which I tested (D25).

Removals are mirrored (D11). If a file disappears from the listing, it's deleted from `raw` and the archive keeps it. There's a guard, though. A run that would remove more than two files, or any `pr.data.*` file, refuses unless you explicitly allow it, so a bad listing can't wipe out the data the answers depend on.

BLS's access policy is built into the HTTP layer. Every request carries a User-Agent with my contact details, requests are spaced about a second apart, and a 403 stops the run with no retry, since retrying a block is exactly what BLS asks bots not to do. Only timeouts, connection errors and 5xx responses are retried, with backoff. If a downloaded file's size doesn't match the listing, it's retried a few times and then the run fails without writing it (D21).

To prove the no-reprocessing claim, I ran ingestion twice against unchanged sources. The second run made two requests, the listing and DataUSA, and wrote nothing. I checked that independently of the tool's own counters: every file's modification time and the manifest's hash were unchanged ([evidence](docs/evidence/phase3_second_run_unchanged.txt)). I also downloaded `pr.data.0.Current` with curl and compared it byte for byte with the Volume copy, Windows line endings included.

After a run, ingestion starts a pipeline update if `raw` has changed since the last update it successfully started (D19). A `refresh_pending` flag in the manifest carries that across failed runs and `--no-refresh` runs, so a crash between landing data and starting the pipeline can't leave the tables stale. That makes the trigger at-least-once (D24), which is fine, because a duplicate update just recomputes identical tables. Code changes work differently. I deploy and run those on purpose, so they never slip in with the next data load (D32).

### Bronze

Auto Loader was the obvious choice, and I didn't use it. BLS publishes complete files and rewrites them in place when it revises the data. Auto Loader is built for files that arrive once and never change: by default it processes each path only once, and for an overwritten file it doesn't promise which version it reads. So Bronze is a set of materialized views that re-read whatever is currently in `raw` on each refresh (D12). That's about 6 MB, which costs nothing, and changes and removals flow through with no extra logic. The no-reprocessing requirement is met at ingestion instead, which is where it matters for BLS's servers.

Bronze keeps the files as they are: every column a string, padding kept, quote handling off and nothing trimmed (D29). I add two columns, `source_file` and `ingested_at`. `ingested_at` is when ingestion wrote the file, not when the pipeline happened to run.

Expectations only see rows, so they can't check a file's header. Instead, a small pure Python function trims each file's header names and compares them with the expected list, and the update fails on any mismatch. It has unit tests for missing, extra, reordered and renamed columns (D13).

The population data needed an unusual fix (D31). Every JSON reader I tried rewrote the numbers: `316128839.0` came back as `3.16128839E8` or `316128839`. Bronze is supposed to hold the source text exactly, so it reads the file as plain text and cuts the values out with a pattern match. That's more fragile than a parser, so three fail expectations guard it: at least one record, the same record count a real JSON parser finds, and exactly the expected keys.

### Silver

Silver reads only `pr.data.1.AllData`, matched on its exact file name (D6, D30). `pr.data.0.Current` turned out to be a recent slice of AllData, so reading both would double-count those years. I didn't want to just assume that, so a warn-level check confirms on every update that all 46,159 Current rows exist in AllData with the same values.

Every string gets trimmed (D14). Years become integers, so the Q3 join is integer to integer (D15). Values become `DECIMAL(12,3)`, sized from the largest value in the data plus headroom (D16, D33). A decimal type keeps the Q2 sums exact, so a tie is a real tie and not floating-point noise.

One thing caught me out here: casting to DECIMAL silently rounds extra decimal places. So the fit check runs on the source text, and a value that's numeric but too long fails the update instead of being quietly rounded or dropped. Series labels live in the series table and get joined in Gold (D35).

The expectations follow one rule (D13):

- **Warn** when a problem doesn't affect the answers.
- **Drop** when it's a known artifact, or when dropping is safe because only complete years get ranked.
- **Fail** when carrying on would publish a wrong answer with confidence.

Drop and fail conditions never overlap, so every row is accounted for: 92,328 AllData rows in Bronze, 92,328 in Silver, nothing dropped and no duplicates removed (D34).

Some rules span rows, like key uniqueness, the Current check and "AllData isn't empty", and an expectation can't express those directly. Each one is computed into a small check table, and the expectation is attached there. These have to be materialized views, because a temporary view only runs when something reads it, and nothing reads a check.

### Gold

Each answer's primary version is SQL (D37). Gold is for people who work in SQL, such as analysts, dashboards and Genie, so that's the version they should be able to read. Each answer also has a PySpark version, kept as a private dataset that isn't published. A parity check compares the two row for row, in both directions, on every update.

The answers themselves:

- **Q1** reports the population standard deviation, with the sample figure alongside (D2). The six years are the whole set being described, not a sample of something bigger. I show both because Spark's `stddev()` quietly uses the sample formula, and a reader might expect that number.
- **Q2** sums Q01–Q04 for quarterly series and never Q05, which BLS defines as the annual average. The 54 series that only publish annual figures are ranked by their Q05 value, and the result says which basis each row uses. Only complete years are ranked (D3, D4). Completeness is worked out from the data rather than a hardcoded cutoff, so the partial 2026 drops out today and comes back when BLS publishes Q4. Ties go to the earliest year, and the other tied years are listed (D17).
- **Q3** is a left join on integer year, so years without a population figure stay in with a NULL (D15).

Publishing follows a write-audit-publish pattern (D36):

1. **Write.** Each answer is first computed into private candidate tables.
2. **Audit.** A single gate table reads every fail-level check: the Silver checks, the parity check, and checks on the candidates themselves (Q1 covers exactly 2013–2018, Q2 has one row per series and a valid basis, and Q3 isn't empty).
3. **Publish.** Each published Gold table is a copy of its SQL candidate, joined to the gate's single row.

If any check fails, the update stops before a published table changes. Readers may then see answers that are stale, but never wrong ones.

I wanted to see that actually happen, so I temporarily required Q1 to cover seven years and ran an update on the real pipeline. It failed at the gate, all three Gold tables were skipped, and their refresh times didn't move. After I reverted, the next update completed normally ([evidence](docs/evidence/phase6_gate_failure_test.txt)).

### Deployment

Everything is deployed with one Asset Bundle: the schema, the three Volumes and the pipeline. I turned off the bundle's development mode (D26), because it put my workspace username into every table name, and with one developer there's nothing to isolate. Individual updates run in development mode instead, since the pipeline-level setting is deprecated (D28).

The schema and the `archive` Volume are protected with `prevent_destroy` (D27), because the archive is the one thing that can't be rebuilt from the source. When I ran a real `bundle destroy`, it was refused and nothing was deleted. That protection has a hole, though: removing a resource from the configuration deletes it on the next deploy, which is an open CLI issue. So a test fails if either resource disappears from `resources/` or loses the flag.

## Trade-offs

**Where ingestion runs.** Running ingestion locally is a workaround for Free Edition's network limits (D1). For a client, the same package would run as a scheduled serverless job under a service principal, with the two source domains allowed through egress. Its exit codes would map onto job retries and alerts.

**Schema drift.** Here, drift fails loudly: the header contract and the population key checks stop the update rather than guess. For a source that changes shape often, I'd let Auto Loader evolve the schema, keep rescued data, and alert on new columns instead of failing.

The population pattern match works because DataUSA's records are flat and checked on every update. For a richer API, I'd store the raw document or a VARIANT in Bronze and parse it in Silver. Bronze would then be exact in value rather than in text (D31).

**Data volume.** Re-reading every file on every refresh is right for 6 MB that gets rewritten in place, and wrong at scale. There I'd point Auto Loader at the immutable `archive` Volume, which this design already fills. That gives incremental Bronze with full history, with the "latest version of each file" logic moving into Silver (D12).

I'd also move the manifest from a JSON file into a Delta table. Per-file updates would become transactional, concurrent runs would be safe, and the whole file wouldn't be rewritten after every download (D8, D23).

**Cost.** Updates only run when `raw` changed or code was deployed, so an unchanged source costs no compute (D19, D32). That mattered with Free Edition's daily quota. Development-mode updates skip automatic retries, which I'd turn back on in production.

**Access control.** This is implemented for analysts (D38). The `analysts` account group has `USE CATALOG`, `USE SCHEMA` and `SELECT` on the three Gold tables, and nothing else. An idempotent SQL file, run by a bundle job, applies the grants, and a test fails if that file grants anything wider. Ingestion's service principal would get write access to `raw`, `archive` and `state`. Production deploys would run as a service principal rather than as a person (D26), with a development target for each developer, which is what bundle development mode is actually for.

**Monitoring.** The gate makes a failure safe, but it also makes it stale, and stale data nobody notices is its own problem. In production, an alert on pipeline failure is what turns stale into noticed.

That alert matters more than it might look. Ingestion clears `refresh_pending` when an update starts, not when it succeeds, so a failed update has to be caught on the pipeline side (D19). After a failure, Silver can also be newer than Gold (D36). The warn-level metrics, such as the Current check, rescued data and missing titles, belong on a data quality dashboard rather than in a log nobody reads.

**Rules that depend on process.** Two safety rules rely on people:

- Only one ingestion run at a time (D8).
- Never delete a protected resource block. The test catches this, but only if tests run before a deploy (D27).

On a team, both belong in CI, with tests and strict validation on every merge, and a scheduler that can't overlap ingestion runs.

## AI usage

I used Claude throughout, in two ways.

Claude Code, running in VS Code, did most of the hands-on work. It explored the sources, proposed designs with trade-offs, wrote the code and tests, and ran the verification.

Alongside that, I kept a separate Claude conversation as a reviewer. I'd paste Claude Code's reports and proposals into it, talk through the options, and then make the call. Every design decision was mine, but many of them came out of those review conversations, and the decision log says so. The working rules are in [CLAUDE.md](CLAUDE.md): Claude drafts, explains and verifies; I decide.

I hadn't used Spark Declarative Pipelines or Asset Bundles before, so I also used Claude as a teacher. Before it wrote code using a new concept, I asked for a short explanation of it, for example materialized views versus streaming tables, how expectations work, and what a pipeline update actually does.

Some steps only I could do: the OAuth login for the CLI profile, confirming the DataUSA URL, and running `bundle destroy`, since the CLI explicitly tells AI agents not to skip its confirmation prompt.

**How I checked its work.** My main rule was that nothing counted as done until there was evidence I could see, ideally from a source independent of the code making the claim. The no-op ingestion run was proven with file modification times and a manifest hash, not the tool's own counters. Silver and the Q2 answers were hand-checked against the raw files. I also had Claude deliberately inject bugs to make sure the tests would catch them:

| Injected bug | Tests that failed |
|---|---|
| A hash that never matches | 6 at first, 8 after D19 |
| 403 treated as retryable | 2 |
| A `refresh_pending` flag that can never be saved | 9 |

**Mistakes that got caught.** The [AI log](notes/ai_log.md) records 13. A few representative ones:

- A profiling query reported 87,351 duplicate keys that didn't exist. `count(DISTINCT a, b)` skips any row where one of the columns is NULL. It was caught because the numbers didn't make sense together.
- A summary said the lookup tables had 22 rows. I caught that in review because the totals didn't add up; the real number was 38.
- An early draft of D27 claimed `prevent_destroy` blocked more than the test had shown. It was rewritten to record only what had actually been tested.
- Claude bundled a `git push` into a commit when I'd only approved a commit, and later tried an `rm` on a variable path. Claude Code's safety checks blocked both.
- In the gate test, the query for each dataset's status read the wrong events and came back blank. It was re-run against the right ones.

**Corrections went both ways.** I corrected Claude's drafts in several places:

- The reasoning behind D6. It warns because Silver and Gold never read Current, not because of BLS publishing times.
- The wording of D19, which was wrong, not just loose.
- Nine changes to its proposals for D9–D19, including the write order, the removal guard, making DECIMAL overflow fail instead of drop, and the tie rule.

Claude also corrected the plan I'd agreed in review. `DESCRIBE HISTORY` doesn't work on pipeline materialized views, and the JSON reader option we'd settled on for population was already in use, and was in fact the cause of the problem.

Where behaviour could be looked up, I asked for it to be checked against documentation or the installed tools rather than assumed. That covered Auto Loader's handling of overwritten files, the Files API having no move operation, `prevent_destroy` and its open bug, and what Free Edition supports.

This document was drafted with Claude from my decision log and AI log, then edited by me.

## Retrospective

> **Note to self:** this section is a draft. Rewrite it in your own words. It has to be what *you* found hardest.

The hardest thing to get right was making ingestion genuinely safe to re-run. "Download what changed" sounds simple, but the edge cases kept coming:

- a file BLS touches without changing;
- a file that disappears and comes back;
- a crash between writing the data and recording it;
- a run that lands data but fails before starting the pipeline.

Each of those became a decision (D7, D10, D19, D24, D25) and a test, and some only showed up once the earlier ones were fixed.

Free Edition's network was the most frustrating part. I spent a while on the LinkedIn verification before accepting that the block was on Databricks' side and moving ingestion to my machine. In hindsight, I'd have tested a plain site like `example.com` straight away, to tell "the platform is blocking me" apart from "BLS is blocking me".

Several things also behaved differently from what I expected or what the docs suggested:

- bundle development mode renaming Unity Catalog schemas;
- the deprecated pipeline `development` field;
- `prevent_destroy` not surviving a resource being removed from the config;
- `DESCRIBE HISTORY` being refused on materialized views;
- the update API not reporting development mode, even though the event log does.

None of them was hard on its own, but each cost time. They taught me to verify platform behaviour directly rather than trust a summary of it, including one written by an AI.

The source data had its own quirks:

- padded header names and values;
- Windows line endings;
- a blank line at the end of `pr.seasonal`, and a capitalised `Seasonal_code`;
- ETags based on modification time;
- a `pr.txt` from 2011 that's out of date. It says the index base year is 2005 and there are about 282 series; the files say 2017 and 337.

Reading the files rather than trusting the documentation is the habit the brief asks about, and this dataset rewarded it.

If I did it again, I'd test failure paths earlier. Every safeguard I deliberately broke turned out to work, but until I'd watched each one fail on purpose, I was trusting it rather than knowing it.