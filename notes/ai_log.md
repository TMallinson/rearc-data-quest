# AI usage log

## 2026-11-13 — Session 1: setup and source exploration (no pipeline or ingestion code)

**What Claude drafted**
- `notes/decisions.md`: D1, the decision to ingest locally, with the evidence from CLAUDE.md.
- `.venv` (Python 3.13) with requests 2.34.2, databricks-sdk 0.166.0, pytest 9.2.0.
- `.gitignore` entries for `.venv/` and `.env`; `.env.example`; a local `.env` holding `BLS_USER_AGENT`.
- Throwaway exploration scripts, kept in a session scratchpad and not committed: a BLS listing
  parser, a fetcher for the docs and lookup files, a streaming 20-line peek at the data
  files, and a single DataUSA fetch.
- A walkthrough of what each source file means for parsing and for the three questions.

**What I changed or rejected**
- I created the `rearc` CLI profile myself, since browser OAuth can't be done by Claude.
- I confirmed the DataUSA URL. Claude had inferred it because the links were missing
  from the quest text, and the inferred URL was identical.
- I told Claude to delete the old `dbc-faf5bf41-0e82` and `databricks current-user me` profiles.
  It used `databricks auth logout <profile> --delete` so their cached OAuth tokens were
  cleared too, after backing up `~/.databrickscfg`. The CLI also removed the stale
  `default_profile` setting, so every command must now pass `--profile rearc`.
- I made the six decisions Claude raised (recorded as D2–D7 in `notes/decisions.md`):
  - D2: report population standard deviation for Q1, with sample standard deviation as a
    second named column, because the six years are the complete set.
  - D3: for Q2, exclude Q05 for quarterly series, but keep annual-only series and rank
    them by Q05. Add a basis column, and count annual-only series from the data, not
    the metadata.
  - D4: rank only complete years, derive completeness from the data (never hardcode
    2026), and include the quarter count.
  - D5: use `series_title` as the label, plus measure, sector and duration names as
    separate columns, with an expectation that every title is present.
  - D6: land both data files and keep both in Bronze; Silver reads AllData only. A warn
    check confirms Current is a subset of AllData with equal values.
  - D7: listing timestamp and size (raw strings) decide whether to download; a content
    hash decides whether to write. DataUSA is always fetched and hashed. Removal
    handling is deferred to the plan session.
- D8: I chose a dedicated state Volume for the manifest, separate from the raw Volume and
  declared as a bundle resource. Known limitation: one ingestion run at a time.
- **Attribution:** I decided D2–D7 after discussing the options with Claude in a separate
  chat. In this session, Claude raised the options and evidence from the source files
  and drafted the rationale text in `decisions.md`.
- **D9–D19 (design for ingestion, Bronze, expectations and types):** Claude proposed the
  options with trade-offs and recommendations: layout (a), write and removal handling
  (b), Bronze read mode (c), and the expectation table (d). I decided D9–D19 after
  reviewing that proposal with Claude in a separate chat. My changes to Claude's
  recommendations:
  - the write order, with the archive named by hash;
  - the removal guard's specific threshold and the `pr.data.*` rule;
  - the AllData-not-empty fail check;
  - the title-check action and the coalesce label;
  - removing the Q3 population-null warning;
  - DECIMAL overflow must fail, not drop;
  - the tie rule;
  - test DataUSA before normalising;
  - refresh enforced in code.
- **Facts Claude checked rather than assumed while proposing:**
  - **Auto Loader overwrite behaviour** comes from the Databricks FAQ: exactly-once per
    path by default, and no guarantee which version of an overwritten file is processed.
  - **The SDK Files API has no move or rename.** Claude inspected the installed
    databricks-sdk 0.166.0.
  - **Bundle dev-mode naming:** the docs don't confirm whether dev mode prefixes Unity
    Catalog schemas and volumes. It is left to be checked with `bundle validate` in
    Phase 2.
- **Corrections to Claude's drafts:**
  - D2: Claude added that the sample value is worth showing because Spark's `stddev()`
    uses the sample formula by default. I confirmed that's correct.
  - D6: I rewrote the rationale for warning rather than failing. Claude had attributed it
    to BLS publishing timing. The real reason is that Silver and Gold read only AllData,
    so a mismatch doesn't affect the answers, and failing would block correct output
    over a file we don't consume. The warning flags that the Current-in-AllData
    assumption broke. Possible causes: Current holding newer revisions (meaning Silver
    is stale), or publishing timing.
  - D7: Claude raised a gap: with the decision as first written, a file that is
    re-touched but unchanged would be downloaded again on every run. I accepted Claude's
    fix (on an unchanged hash, update only the listing metadata in the manifest). I
    specified what each manifest entry stores, and added that the manifest must sit
    outside any folder the pipeline reads.

**How we verified it**
- `git check-ignore -v` confirmed that `.env`, `.venv/`, and `private/` are ignored.
- The BLS listing returned HTTP 200 with the contact User-Agent and listed 12 files. The 10
  docs and lookup files were saved to `private/bls_docs/`. Their byte sizes match the listing.
- The data files were only streamed (first 20 lines). Nothing was saved.
- DataUSA returned HTTP 200 with 11 rows, 2013 through 2024. 2020 is missing.
- Databricks access: `databricks auth profiles` shows only `rearc`, valid. `current-user me`
  returned an active admin user. `catalogs list` shows the `workspace` catalog. The
  `rearc_quest` schema doesn't exist yet; the bundle will create it.
- Pre-commit checks, before the first commit:
  - `git status --ignored` showed `.env`, `.venv/` and `private/` as ignored.
  - grep found no email in any staged file.
  - An 8-word overlap check against `private/quest.md` found no copied quest text.
  - No `.claude/settings.local.json` exists.
  - Claude's first grep silently checked nothing (zsh passed the file list as a single
    name). It caught this and re-ran the check correctly.
- Claude renamed `claude.md` to `CLAUDE.md` so the file loads on case-sensitive systems,
  and added `.DS_Store` and `.databricks/` to `.gitignore`. `.databricks/` is the CLI
  and bundle's local state directory; Databricks' own bundle template ignores it.

### Phase 1: ingestion package and tests against fakes (no Databricks)

**What Claude drafted**
- The `rearc_ingest` package under `src/`:
  - `config`: settings and the User-Agent check;
  - `http`: retry and pacing policy;
  - `listing`: strict listing parser;
  - `manifest`, `plan` (D7 comparison and D11 guard) and `store` (D10 write order);
  - `volume`: SDK Files API wrapper;
  - `refresh`: D19 trigger;
  - `sources/bls`, `sources/datausa`, `run` and `cli`.
- `pyproject.toml` with the `rearc-ingest` entry point.
- 56 pytest tests, using fakes for the Volume, HTTP and the pipeline trigger.

**Small deviations from the approved plan**
- The fakes live in `tests/fakes.py`, not inside the package modules, so test code
  doesn't ship.
- D10/D11 write logic is in its own `store.py`; summary counters are in `summary.py`.

**Verification**
- D18 test: two DataUSA fetches 5 seconds apart returned identical bytes (SHA-256
  `31e7b5d3…`), so the raw bytes are hashed. The result is recorded under D18.
- `pytest`: 56 passed. This includes the second-run proof: zero writes or deletes, zero
  BLS file downloads, only 2 requests (listing and DataUSA), and no refresh.
- **Mutation check:** Claude injected two deliberate bugs from a scratch pytest plugin,
  without editing the source.
  - A hash that never matches: 6 tests failed, including the second-run proof.
  - 403 treated as retryable: both 403 tests failed.
- `rearc-ingest --help` works. No Databricks calls were made in this phase.

### Phase 1 review: four follow-up decisions (D19 revised, D20–D22)
- **Attribution:** I decided these after reviewing them with Claude in a separate chat.
  Claude had raised them as open choices at the end of Phase 1.
- **D19 revised:**
  - Start an update whenever raw has changed since the last successfully started update,
    tracked by a `refresh_pending` flag in the manifest.
  - The original wording ("only if this run changed raw") was wrong: a failed run after a
    raw write left the pipeline stale.
  - `--no-refresh` leaves the flag set. The flag is cleared once an update has started,
    not when it succeeds; that is a PROCESS.md trade-off.
- **D20:** the DataUSA User-Agent is `rearc-ingest/<version> (+<repo URL>)`, taken from
  `pyproject.toml` metadata, not my email. Claude's draft had used a bare tool name.
- **D21:**
  - Any size mismatch, shorter or longer, is retried up to `max_attempts`, then fails
    with a "may have changed during the run; re-run" message.
  - A mismatched file is never written.
  - Claude's draft only described short bodies as truncation.
- **D22:** confirm in Phase 3 whether the Files API creates parent folders; create them
  explicitly if it doesn't.
- **Verification:**
  - `pytest` passes 64 tests. New ones cover:
    - pending set after a failure, with the next no-change run starting the update;
    - `--no-refresh` keeping the flag set;
    - a failed trigger keeping the flag set;
    - shorter and longer size mismatches;
    - a persistent mismatch never being written;
    - a separate User-Agent per source.
  - Mutants: a never-matching hash failed 8 tests; a retryable 403 failed 2; a
    `refresh_pending` flag that could never be stored failed 9.

### Phase 1 sign-off
- **My decisions** (after review with Claude in a separate chat):
  - The repo URL is set in `[project.urls]`.
  - Keep the per-file manifest rewrite as an accepted trade-off (D23).
  - The trigger is at-least-once, and duplicate updates are harmless (D24).
  - Recovery from a corrupted manifest is to delete it and re-run (D25), with a test
    proving the Volume ends up byte-identical.
  - Document the exit codes in the README.
- **Claude added**, beyond what I asked: the ManifestError message now names the recovery
  step (delete the manifest path and re-run).
- **Verification:**
  - pytest: 65 passed, including the new D25 test.
  - `tool_user_agent()` resolves to `rearc-ingest/0.1.0 (+https://github.com/tmallinson/rearc-data-quest)`.

### Phase 2: bundle, schema and Volumes
- **D26 (my decision):** no `mode: development`. Claude showed that dev mode resolved the
  schema to `dev_<username>_rearc_quest`, and that `presets.name_prefix: ""` didn't remove
  the prefix.
- **D27 (my instruction): `prevent_destroy` on the archive Volume and the schema.**
  - Claude confirmed the setting exists in CLI v1.22.1's bundle schema.
  - Claude found the open CLI issue (databricks/cli#4872): removing a resource from the
    config deletes it despite the flag. The fix, PR #7499, is unmerged.
  - Claude ran plan-only tests:
    - a rename is an in-place update;
    - a removed block plans a delete with no error;
    - a schema move plans a recreate with no error.
  - Whether a real deploy or destroy refuses is unverified.
- **Correction:** Claude's first D27 draft claimed the flag "blocks forced recreation".
  The plan test didn't show that, so the text was changed to record only what was tested.
- **Verification:**
  - `bundle validate --strict`: OK.
  - Resolved names: `workspace.rearc_quest` with Volumes `raw`, `archive`, `state`.
  - `bundle plan`: 4 to add, 0 to delete.
  - `bundle deploy`: 4 created.
  - `schemas get` and `volumes list` show all four, MANAGED.
  - A plan after deploying shows 0 changes.
  - The 35 files the bundle uploaded exclude `private/`, `.env` and `.venv`.
- **Destroy test:**
  - Claude's non-interactive `bundle destroy` was refused before doing anything, because
    it needed `--auto-approve`. Claude didn't add the flag without my approval, as the
    CLI's own message asks.
  - **I ran the destroy myself, interactively.** `prevent_destroy` refused the whole
    destroy and listed the schema and the archive Volume. Nothing was deleted, including
    the unprotected Volumes.
  - Claude verified afterwards: all four resources exist, and the plan shows 0 changes.
    The exact output is recorded in D27.
- **Automated mitigation for databricks/cli#4872 (my request):**
  `tests/test_bundle_config.py` fails if the archive Volume or the schema is missing or
  unprotected. It has two self-tests. pyyaml was added as a dev dependency.
  pytest: 68 passed.

### Phase 3: real ingestion and the no-op proof
- **My instructions:**
  - Push after each approved phase commit; added to CLAUDE.md.
  - Run the dry run, first run, independent byte check and second-run proof, in that
    order.
  - Save the evidence to `docs/evidence/`.
- **Dry run:** the real listing showed 12 files. 13 were planned as new (12 BLS plus
  DataUSA), and nothing was written: all three Volumes stayed empty.
- **First run (`--no-refresh`):**
  - 12 BLS downloads and 1 DataUSA fetch; 13 archive, 13 raw and 13 manifest writes.
  - Verified with a read-only scratch script:
    - 13 files in raw, 13 in archive (each named by its SHA-256), 1 manifest;
    - `refresh_pending` true;
    - every raw file hashes to its manifest SHA-256;
    - every archive copy is byte-identical to raw;
    - every BLS size equals its listing size.
  - The DataUSA archive hash equals the D18 test hash.
  - **D22 settled:** uploads create parent folders themselves, so no code change.
- **Independent byte check:** `pr.data.0.Current` fetched with curl (BLS User-Agent from
  `.env`) and the raw copy fetched with `databricks fs cp` are byte-identical:
  - 1,943,714 bytes each, SHA-256 `96b35c3f…`, and `cmp` finds no differences;
  - all 46,160 lines end in CRLF in both copies.
- **Second run (`--no-refresh`):**
  - The tool's counters are all 0. It made only the listing request and the DataUSA fetch.
  - Snapshots before and after: all 27 files have unchanged size and last-modified
    time, with none added or removed.
  - The manifest SHA-256 is identical (`2f8fd9ae…`), and `refresh_pending` is still true.
  - The evidence is saved in `docs/evidence/`. An identifier scan found nothing personal.
- **Claude's mistakes, caught by guardrails:**
  - A cleanup `rm -f $D/*` on a variable path was blocked by the safety check. It was
    unnecessary; Claude switched to a fresh directory.
  - Earlier, a `git push` bundled into the Phase 2 commit command was blocked because I
    had approved only a commit.
- **Phase 3 sign-off:**
  - The snapshot, compare and check scripts are now one read-only
    `scripts/volume_check.py`. A test greps it for write or delete calls. It ran
    against the real Volumes with no problems.
  - The README documents it, and has an "expected warnings" note separating the SDK
    endpoint 403 from a BLS 403.

### Phase 4: Bronze
- **Claude explained** pipeline updates, materialized-view refresh and where expectations
  appear. It then built Bronze per C1:
  - `src/pipeline/transformations/bronze_bls.py`: nine BLS tables from one factory, each
    with a header check that raises and a warn expectation `no_rescued_data`;
  - `bronze_population.py`;
  - `resources/rearc_quest.pipeline.yml`: serverless, triggered, reading only `raw`
    through the `rearc.raw_root` configuration.
- **Choices Claude made, flagged for my review:**
  - Python rather than SQL, for the header trimming and check.
  - Quote handling and whitespace trimming off.
  - `Seasonal_code` keeps its capital S.
  - Population keys renamed to snake_case.
- **D28 (my decision):** `development: true` on the pipeline failed strict validation as
  deprecated. I chose a per-update development flag passed by rearc-ingest. Tests were
  added for the trigger.
- **Claude's mistakes, all fixed:**
  - The pipeline's resource key duplicated the schema's (`rearc_quest`), and validation
    rejected it.
  - The poll script read a non-existent `UpdateInfo.development`.
  - A regex group error in one verification query.
- **Verification:**
  - pytest: 73 passed. New tests cover the bundle/settings consistency and the trigger.
  - `bundle validate --strict`: OK. Deploy created 1 pipeline.
  - `rearc-ingest` with refresh: 0 data writes, update `c401ffe6` started, 1 manifest
    write, `refresh_pending` now false.
  - The update completed: about 30s of setup and run after resources were ready.
  - **Bronze, all from SQL queries:**
    - every BLS table's row count equals its file's data-line count:
      `bronze_pr_data` 138,487 = 46,159 Current + 92,328 AllData;
    - trimmed column names as expected;
    - no `\r` in any column;
    - `_rescued_data` 0 everywhere, matching the event-log metrics (`no_rescued_data`
      0 failed in all nine);
    - padding kept (`series_id` is always 17 characters, and every `value` is
      left-padded);
    - `pr.seasonal`'s blank line was skipped by the reader.
- **Findings for my decision:**
  - Empty CSV fields arrive as NULL (128,533 `footnote_codes`).
  - DataUSA population reads as strings like `3.16128839E8`, not the source text
    `316128839.0`.
  - Whether the update ran in development mode is unverified (D28 follow-up).

### Phase 4 review and fixes
- **Development mode (D28):** I asked Claude to check the event log.
  - Claude found the update's own `"development": true` in the `create_update` event,
    separate from the spec's `config.development: false`. **Verified.**
- **My decisions:**
  - Accept NULL for empty `footnote_codes` (D29).
  - Accept Claude's Python, quote-off and snake_case choices, with the reasons recorded
    and the name mapping in D29.
  - Silver selects AllData by exact base name (D30).
  - Make the header check a pure function with failure-path tests.
- **Population (D31):**
  - I asked to try `primitivesAsString` first. Claude pointed out that Bronze already used
    it, and that it was the source of `3.16128839E8`.
  - Claude tested four read methods in SQL against the raw file. Only regex on the raw text
    kept `316128839.0`. VARIANT dropped the `.0`; the JSON reader and `get_json_object`
    gave scientific notation.
  - **I chose exact text via regex.** Its guards (record count against
    `json_array_length`, the key set, and at least one record) are fail expectations on a
    temporary view, because pipeline functions mustn't collect data.
- **Header check (D13):**
  - Moved to `src/pipeline/utilities/bronze_contract.py`, importable because the pipeline
    root folder is on `sys.path` (Databricks docs).
  - 6 pytest cases: match, missing, extra, reordered, renamed, several missing.
- **Verification:**
  - pytest: 79 passed. `bundle validate --strict`: OK.
  - Update `a0e7c6d7`, started with `pipelines start-update --development` because the
    code changed and raw didn't, completed.
  - Before: `3.16128839E8`. After: `316128839.0`.
  - All 11 records are identical to the raw JSON text in the Volume, compared in Python
    against the downloaded copy, and all four columns are strings.
  - The three fail expectations each passed 11 records with 0 failures.
  - The update's development flag is true. BLS row counts are unchanged.

### Phase 5: Silver
- **My notes for PROCESS.md:** the regex-versus-parser trade-off (D31 addendum) and the
  two update triggers (D32).
- **Claude explained** multi-row checks: compute rows that carry the answer, then attach
  the expectation to that dataset. It built:
  - `silver.py`: five lookups, series with labels, data staged then published, population
    staged then published;
  - `checks.py`: `dq_unique_keys`, `dq_current_in_alldata`, `dq_alldata_input`;
  - pure rules in `utilities/silver_rules.py`, with 22 new tests.
- **D33:** `DECIMAL(12,3)`, sized from the full profile (at most 5 integer digits and 3
  decimals).
  - "Fits" is tested on the text, because `try_cast` rounds silently.
  - The profile query first reported 87,351 "duplicates". Claude identified this as the
    NULL-skipping behaviour of `count(DISTINCT a, b, ...)`.
- **Raised, not decided:** check datasets don't automatically gate Gold. Recorded in
  D34 for Phase 6.
- **Verification:** pytest 101 passed; strict validation OK; update `27a6eebd`
  completed.
  - **D6:** 46,159 Current rows; 0 missing from AllData; 0 with a different value.
  - **D3:** 54 Q05-only series in the data, the same 54 as the metadata, with 0
    classified differently.
  - **DECIMAL:** the data has at most 5 integer digits and 3 decimals. Silver is
    `decimal(12,3)`, ranging −60.800 to 25,576.642.
  - **Accounting:** 92,328 Bronze AllData rows = 92,328 Silver + 0 `row_not_blank` drops
    + 0 `value_numeric` drops + 0 exact duplicates removed.
  - **Hand check:** `fs cat | grep` of PRS30006032 2010 in the raw file matches Silver's
    five rows.
  - **Labels:** 337 of 337 from `series_title`; 0 needed the fallback.
  - **Expectations:** all 38 Silver, check and Bronze expectations passed with 0 failures.
- **Correction after my review:**
  - Claude's Phase 5 summary said the lookups had "22 rows in total". 22 is the measure
    lookup alone.
  - Reconciled: `dq_unique_keys` has 92,714 rows = 92,328 data + 337 series + 38 lookup
    rows (period 5 + measure 22 + sector 6 + duration 4 + footnote 1) + 11 population
    years.
  - The check data was right; the summary was wrong.
- **My acceptance of Claude's Silver choices,** with my reasons, is recorded in D34.

### Phase 6: Gold, gate and parity
- **My decisions:**
  - D35: labels only in `silver_pr_series`, joined in Gold.
  - D36: one gate table reading every fail-level check.
  - D37: SQL primary; PySpark as private alternatives.
- **Claude's reading of D36, flagged for review:** some fail checks are about the answers
  themselves (Q1 years, Q2 one row per series, Q3 non-empty, parity).
  - So answers are first computed into private candidates (`q*_candidate_sql` and
    `q*_candidate_pyspark`).
  - The gate reads those checks, and the published `gold_*` tables are `SELECT`s from the
    SQL candidate cross-joined to the gate.
  - Claude also dropped `period_name` from the published `silver_pr_data`, alongside D35.
- **Before spending the update,** Claude dry-ran the three SQL candidate queries on the
  warehouse against Silver. All succeeded.
- **Claude's mistakes:**
  - A third shell-quoting error in an inline Python printer; switched to script files.
  - Reported the update as taking about 6 minutes from the poll script. The event log
    shows 1m52s (82s of dataset work); the poll's observation lagged.
- **Verification** (update `d0b55ee5`, completed; pytest 101 passed; strict validation OK):
  - **Q1:** 6 years (2013–2018); mean 322,069,808; population SD 3,796,119.94; sample SD
    4,158,441.04. This matches the session 1 profile (322,069,808 / 3,796,120 /
    4,158,441).
  - **Q2:**
    - 337 rows for 337 series: 283 `quarterly sum`, 54 `annual value`.
    - 0 series with no complete year (`dq_q2_coverage` 337 passed, 0 failed).
    - 7 series with ties, each with exactly one other tied year (3 annual, 4 quarterly).
  - **Q2 hand checks from the raw file's text** (Python `Decimal`, no Spark):
    - PRS30006032: 1994, total 14.6 (1.7 + 4.0 + 4.2 + 4.7); 1987 and 2026 incomplete.
    - PRS84006021: 1964 and 2010 tied at 4.9, so the earliest, 1964, wins.
    - Both match Gold exactly.
  - **Q3:** 39 rows, 1988–2026. 11 have population (2013–2019 and 2021–2024). 28 have
    none: 1988–2012, 2020, 2025 and 2026.
  - **Parity:** q1 1 row, q2 337, q3 39. 0 rows only in SQL and 0 only in PySpark for
    each.
  - **Gate:**
    - All 7 fail checks passed.
    - Event-log `flow_definition`: each `gold_*` table's inputs are exactly `dq_gate` and
      its SQL candidate.
    - `dq_gate`'s 14 inputs include every fail-level check and Silver table, and no
      warn-level check.
  - **D35:** `silver_pr_data` columns are now `series_id, year, period, value,
    footnote_codes, ingested_at`, with no labels.
  - **Expectations:** all 47 in the update passed with 0 failures.
- **Phase 6 sign-off (my review):**
  - I accepted Claude's D36 interpretation, recorded as write-audit-publish.
  - I accepted dropping `period_name`, with the principle that descriptive attributes
    live in lookups, not facts (D35).

### Gate failure-path test (my instruction)
- **Correction to my plan:** I asked for Delta versions from `DESCRIBE HISTORY`. Claude
  found it isn't supported on pipeline materialized views. It used `DESCRIBE TABLE
  EXTENDED` refresh information, `information_schema.last_altered` and per-dataset
  event-log status instead, and explained why comparing contents couldn't show "not
  refreshed": the data didn't change.
- **Result:** with Q1 temporarily required to cover 7 years, update `d362daab` FAILED at
  `dq_gate` (`q1_six_years`). The three Gold tables were SKIPPED, with `Last Refreshed`
  and `last_altered` unchanged. Silver did refresh.
- **After the revert:** update `4c1736fd` COMPLETED with all 33 datasets. `git status` was
  clean and `bundle plan` showed 0 changes.
- Evidence saved as `docs/evidence/phase6_gate_failure_test.txt`; identifier scan clean.
  Recorded in D36.
- **Claude's mistake:** its first per-dataset status query took each dataset's last event,
  which often has no status. It re-queried using the last event that carries one.
