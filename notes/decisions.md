# Decisions

Each entry records what was decided, the evidence, the reasoning, and the consequences.

---

## D1. Ingestion runs locally; everything downstream of the Volume runs in Databricks

**Status:** Decided (before session 1, recorded 2026-11-13)

### Context
Ingestion has to fetch from two external hosts: the BLS download server
(`download.bls.gov`) and the DataUSA population API (`honolulu-api.datausa.io`).
The workspace is Databricks Free Edition, which only offers serverless compute.

### Evidence
- From Databricks serverless compute, DNS lookups were **refused** for
  `download.bls.gov`, `honolulu-api.datausa.io`, and `example.com`, while
  `pypi.org` returned HTTP 200. Conclusion: Free Edition egress is limited to
  Databricks' own list of trusted domains. This is not a BLS block, because
  `example.com` was refused too.
- I completed Databricks' LinkedIn verification, which is documented as unlocking
  outbound internet access, then terminated and restarted compute. The results
  were the same.
- From my local machine, both sources return HTTP 200 when the request carries the
  BLS User-Agent.

### Options considered
1. **Ingest inside Databricks (job or notebook).** Not possible: the hosts can't
   be resolved from serverless.
2. **Ingest locally and land files in the Volume through the Databricks SDK Files
   API.** Works today. Unity Catalog stays the system of record.
3. **Upload files by hand through the UI.** Rejected: it isn't repeatable and
   breaks the quest's re-run and change-detection requirement.

### Decision
Option 2. A local Python package fetches the sources and writes them to
`/Volumes/<catalog>/rearc_quest/raw` through the SDK Files API, authenticating with
the `rearc` CLI profile. Everything downstream of the Volume (the pipeline and the
Bronze, Silver, and Gold tables) runs in Databricks.

### Consequences and guardrails
- Write to the Volume only through the SDK, never through local file paths.
- Keep the ingestion manifest (state) **in the Volume**, not on the local machine.
- Don't build a Databricks job for ingestion, but avoid local-only assumptions
  (local state, local paths). Then the same package could truthfully run as a
  scheduled serverless job on a paid workspace whose egress allows these two
  domains.
- This decision and its evidence will be written up in PROCESS.md.

---

## D2. Q1: population standard deviation as the answer, sample as a second column

**Status:** Decided 2026-11-13

**Evidence:** DataUSA has all six years from 2013 to 2018. Mean 322,069,808.
Population SD 3,796,119.94; sample SD 4,158,441.04 (about 9% apart).

**Decision:** The Gold answer is the **population** standard deviation (`stddev_pop`).
The **sample** standard deviation (`stddev_samp`) is included as a second, clearly
named column.

**Rationale:** The six years are the complete set being described, not a sample
drawn from a larger population, so the population formula (divide by N) is correct.
The sample value (divide by N-1) stays visible because Spark's `stddev()` defaults to
the sample formula, and a reviewer may expect that number.

---

## D3. Q2: exclude Q05 for quarterly series; rank annual-only series by Q05

**Status:** Decided 2026-11-13

**Evidence:** `pr.period` defines Q05 as "Annual Average", and `pr.txt` says Q05 marks
annual averages. In `pr.series`, 54 manufacturing series (sectors 3000, 3100, 3200)
begin and end at Q05, which suggests they publish only annual values.

**Decision:**
- For series that report quarters (Q01 to Q04), **exclude Q05** from the yearly sum.
- **Keep annual-only series** and rank their years by the Q05 value.
- Add a column stating the basis of each result: `quarterly sum` or `annual value`.
- Classify a series as annual-only **from the loaded data, not from `pr.series`
  metadata**, and verify the count against the 54 from metadata.

**Rationale:**
- Adding an annual average to the four quarters it summarises double-counts the year.
- Dropping annual-only series would silently remove about 16% of series from an answer
  that is meant to cover every series.
- The basis column makes clear that the two kinds of number aren't directly comparable.
- Metadata can drift from the data, so the data is authoritative.

---

## D4. Q2: rank only complete years, with completeness derived from the data

**Status:** Decided 2026-11-13

**Evidence:** Most series end at 2026 Q03 in `pr.series`, so 2026 is a partial year.
Many series are percent changes that can be negative. For those, a year with fewer
quarters can produce a larger sum and win unfairly.

**Decision:**
- A year qualifies for ranking only if it is **complete**: all four quarters for a
  quarterly series, or a Q05 value for an annual-only series.
- Completeness is computed from the data. **2026 is never hardcoded.**
- The result includes the quarter count for the winning year.

**Rationale:**
- Comparing a 3-quarter sum with 4-quarter sums is not like for like.
- Deriving completeness keeps the logic correct as new quarters arrive. The same rule
  also handles series that begin mid-year (`begin_period` is Q02 for 79 series).

---

## D5. Labels: `series_title`, plus decoded lookup names as separate columns

**Status:** Decided 2026-11-13

**Evidence:** `pr.series` has a `series_title` column. It isn't documented in the 2011
`pr.txt`. All 337 titles are distinct, and each reads as a complete description, for
example "Hours worked for the Manufacturing sector, % Change from previous quarter".

**Decision:**
- `series_title` is the human-readable label.
- Measure, sector and duration names from `pr.measure`, `pr.sector` and `pr.duration`
  are carried as separate columns.
- An expectation checks that every series has a non-empty title.

**Rationale:** The title is BLS's own wording and needs no assembly. The separate
columns let users filter and group on each dimension. The expectation guards the
label, which the quest explicitly requires.

---

## D6. Current vs AllData: land and keep both in Bronze; Silver reads AllData only

**Status:** Decided 2026-11-13

**Evidence:** `pr.txt` describes `pr.data.0.Current` as "All current year-to-date data"
and `pr.data.1.AllData` as "All data". In the first 20 lines, Current starts at 1995
and AllData at 1988 for PRS30006011, which suggests Current is a recent subset of
AllData. That hasn't been verified on the full files yet.

**Decision:**
- Ingest both files and keep both in Bronze, so Bronze mirrors the source completely.
- Silver reads **AllData only**.
- A check confirms that every Current row exists in AllData with the same value.
  It **warns** on failure; it does not fail the pipeline.

**Rationale:**
- Combining the two files would double-count overlapping years.
- AllData is documented as the complete set.
- The check turns the subset assumption into something tested on every run.
- **Why warn, not fail:**
  - Silver and Gold read only AllData, so a Current/AllData mismatch doesn't change the
    answers. Failing would block correct output over a file we don't consume.
  - The warning signals that the assumption that Current is a subset of AllData has
    broken and needs investigating.
  - One possible cause is that Current holds newer revisions than AllData, which would
    mean Silver is stale. Another is that BLS published the two files at slightly
    different times.

---

## D7. Change detection: listing metadata decides the download, a content hash decides the write

**Status:** Decided 2026-11-13 (removal handling still open)

**Evidence:**
- The BLS server's ETags are based on modification time: files modified at the same
  time share one ETag, so ETags don't reflect content.
- Responses are gzip-encoded, so `Content-Length` doesn't match the sizes in the listing.
- The listing shows a timestamp (Eastern local time, minute precision, no timezone)
  and a byte size for each file.

**Decision:**
1. **BLS:** compare each file's listing timestamp and size, **as raw strings with no
   timezone parsing**, against the manifest. Download only if either differs or the
   file is new.
2. After downloading, hash the content.
   - **Hash changed (or new file):** write the file to the Volume, then update its
     manifest entry (listing metadata and hash).
   - **Hash unchanged:** don't write the file. Update **only the listing metadata** in
     the manifest. That is a manifest write, not a re-ingest.
3. **DataUSA:** there's no listing, so always fetch, then hash and write only if the
   content changed.
4. **Manifest contents:** one entry per file, holding the last-seen listing metadata
   (timestamp and size, as raw strings) and the hash of the content last written to
   the Volume.
5. **Manifest location:** in the Volume (per D1), but **outside any folder the pipeline
   reads**, so manifest updates never trigger or break pipeline ingestion. The exact
   path is set in the plan session.
6. **Removed files:** still open; options to be proposed in the plan session.

**Rationale:**
- The listing check avoids downloading unchanged files, which is polite to BLS.
- The hash check stops a re-touched but identical file from producing a new write.
  Together they satisfy "a second run with no source changes writes nothing".
- Comparing raw strings avoids guessing the server's timezone. Only equality matters,
  not ordering.
- **Re-touched files (step 2, unchanged hash):** if only the hash were stored, a file
  whose listing timestamp changed but whose content didn't would look changed on every
  later run. Each run would download it again, writing nothing but hitting BLS
  needlessly. Refreshing the listing metadata costs one manifest write, on the run that
  first sees the touch. After that, a second run with no source changes still writes
  nothing.
- **Manifest location:** if the manifest sat inside a folder the pipeline reads, every
  manifest update could look like new source data, or fail to parse as BLS data.

**Known limitation (accepted):** the listing has one-minute precision. A file changed
twice within the same minute with an identical size would be missed. This is low-risk
given BLS's quarterly release schedule.

---

## D8. Ingestion state lives in a dedicated Volume, separate from the raw Volume

**Status:** Decided 2026-11-13

**Context:** D7 requires the manifest to be kept in Databricks (per D1) but outside any
folder the pipeline reads.

**Options considered:**
1. **A subfolder of the raw Volume** (for example `raw/_state/`), with the pipeline
   reading only specific subfolders. This works only if every reader respects the
   path convention.
2. **A dedicated state Volume** next to `raw` in the `rearc_quest` schema.

**Decision:** Option 2. The state Volume is declared as a bundle resource, like the
schema and the raw Volume.

**Rationale:**
- **Separation is enforced by structure, not by path discipline.** No glob or
  Auto Loader path over the raw Volume can pick up the manifest by accident.
- **The raw Volume holds only source data.** Everything in it is something BLS or
  DataUSA published.
- **State can get different permissions.** For example, analysts or the pipeline can
  read `raw` without being able to write to, or even see, ingestion state.

**Known limitation (accepted): one ingestion run at a time.** The manifest is updated
by read-modify-write. Two concurrent runs could each read the same manifest and
overwrite each other's updates, which loses changes or causes needless re-downloads.
Ingestion is run manually from one machine, so this is accepted rather than solved
with locking.

---

## D9. Repository and bundle layout

**Status:** Decided 2026-11-13

**Decision:**
- **One repo with the bundle at the root.**
  - `resources/` holds the schema, the three Volumes, and the pipeline.
  - `src/rearc_ingest/` is the local ingestion package. Its command-line entry point is
    `rearc-ingest`.
  - `src/pipeline/transformations/` holds the bronze, silver, gold and checks code.
  - `tests/` holds pytest tests that need no network and no Spark.
- **Pure logic is kept separate from side effects.** `listing.py` and `plan.py` are pure
  functions. The network (`http.py`), the Volume (`volume.py`) and the pipeline trigger
  (`refresh.py`) sit behind small interfaces, each with a fake for tests.
- **Both versions of each Gold answer run in the pipeline.** The non-primary
  implementation is a **private** dataset: computed but not published to Unity Catalog.
  A SQL-vs-PySpark parity check runs on every update.
- The git branch is renamed from `master` to `main`.

**Rationale:**
- Pure planning logic makes D7, D10 and D11 testable exhaustively without Databricks.
- The fakes let us prove "a second run writes nothing" locally before proving it against
  the real Volume.
- Running both implementations in the pipeline makes parity a checked property, not a
  claim.

---

## D10. Changed files: archive a content-addressed copy, then overwrite raw, then update the manifest (option B2)

**Status:** Decided 2026-11-13

**Decision:** Each new content version is written to two places:
- `archive/<source>/<name>/<sha256>`: immutable and named by hash.
- `raw/<source>/<name>`: the current copy, overwritten in place.

Write order per file:
1. the archive copy;
2. the raw copy;
3. the manifest entry.

**Rationale:**
- `raw/` always mirrors what the source currently publishes, which is what C1 (D12) reads.
- The archive keeps every version without reading anything back from the Volume. The
  Files API has no move or rename, so archiving the old copy at overwrite time would
  mean downloading it first.
- Naming archive copies by hash makes rewriting one harmless.
- The order makes a crash safe. If a run dies after the raw write but before the
  manifest write, the next run sees a hash that differs from the stale manifest. It
  repeats all three steps with the same bytes, which is a safe repeat.

**Consequence:** a third Volume, `archive`, separate from `raw` for the same structural
reason as D8.

---

## D11. Removed files: mirror the removal, with a guard (option R1)

**Status:** Decided 2026-11-13

**Decision:**
- A file missing from a successfully parsed listing is deleted from `raw/` and marked
  `removed` in the manifest. Its archive copies stay.
- **Guard:** a run that would remove **more than 2 files**, or **any `pr.data.*` file**,
  refuses to change anything unless `--allow-removals` is passed.
- The threshold is a setting (default 2), and so is the protected pattern.
- An empty or unparseable listing always aborts the run, before any removal logic runs.

**Rationale:**
- Mirroring keeps `raw/` equal to what the source currently publishes. That avoids the
  double-counting a BLS rename would cause if old files were kept.
- The archive makes every removal reversible.
- The guard stops a bad listing, or a change at BLS, from silently emptying the data
  files that all three answers depend on.

---

## D12. Bronze reads the current files as materialized views (option C1)

**Status:** Decided 2026-11-13

**Decision:**
- **Bronze tables are materialized views** that do batch `read_files` over `raw/` on each
  refresh.
- **One table per file type:**
  - `bronze_pr_data` holds Current and AllData together. The glob `pr.data.*` matches
    them, and a source-file column tells them apart.
  - `bronze_pr_series` holds the series file.
  - Each of the seven lookup files gets its own table.
  - `bronze_population` holds the DataUSA JSON.
  - `pr.txt` and `pr.contacts` are landed but not turned into tables.
- **Header names are trimmed. Values are left exactly as they arrived.**
- **Metadata columns:** `_metadata.file_path` (source file) and
  `_metadata.file_modification_time` (when ingestion wrote the file). Refresh time is
  not used, because a materialized view recomputes on every refresh.
- **A fail expectation checks that Silver's AllData input is not empty.** Silver selects
  that file by name, so a rename at BLS would otherwise produce an empty, plausible-looking
  Silver.

**Rationale:**
- BLS publishes complete snapshots that are rewritten in place, with revisions.
- With Auto Loader's default settings, each file path is processed exactly once. For a
  file that is overwritten, it "cannot guarantee which file version will be processed"
  (Auto Loader FAQ). With `allowOverwrites` it reprocesses the whole file. Streaming
  tables only append, so removals are never reflected.
- C1 sees exactly the current files, with correct handling of changes and removals.
- Re-reading about 6 MB per refresh is negligible.
- The quest's "don't reprocess" requirement is met at ingestion (D7).

**Stretch goal:** C3, which is Auto Loader over the immutable `archive/` Volume. It gives
incremental Bronze plus full version history, with "latest version per file" logic in
Silver. D10 already lands the immutable files C3 needs.

---

## D13. Expectations by layer

**Status:** Decided 2026-11-13

**Decision:** adopt the expectation table proposed this session, with two changes.

- **Guiding principle:**
  - **warn** when a problem doesn't change the answers;
  - **drop** when it's a known artifact, or when D4 makes dropping safe;
  - **fail** when continuing would publish a wrong answer with confidence.
- **Bronze:**
  - Trimmed header names must equal the expected list. This is an explicit code check,
    because expectations only see rows. **Fail.**
  - `_rescued_data IS NULL`. **Warn.**
- **Silver data:**
  - Row is not blank. **Drop**: known artifact.
  - `series_id` not null and matches the pattern. **Fail.**
  - `year` is an integer in a plausible range. **Fail.**
  - `period` is found in `pr.period`. **Fail.**
  - `value` is numeric. **Drop**: safe because of D4. See D16 for values that are
    numeric but don't fit.
  - `series_id` is found in `pr.series`. **Warn**: Q2 needs every series.
  - No key has two different values. **Fail**: exact duplicates are just removed.
  - Footnote codes are found in `pr.footnote`. **Warn.**
- **Silver series:** `series_title` present. **Warn** (change 1, below).
- **Silver population:**
  - Year is unique.
  - Population is not null, positive and a whole number.
  - Nation is United States.
  - All **fail.**
- **Check:** every Current row exists in AllData with the same value. **Warn** (D6).
- **Silver:** the AllData input is not empty. **Fail** (D12).
- **Gold Q1:** exactly six years from 2013 to 2018. **Fail.**
- **Gold Q2:**
  - One row per series, and `basis` is one of the two allowed values. **Fail.**
  - Every series in Silver appears in the result. **Warn.**
- **Gold Q3:** at least one row. **Fail.**
- **Gold, all three:** the SQL and PySpark versions are identical. **Fail.**

**Change 1, the series title (D5):** the check **warns**. The label column is
`coalesce(series_title, a label built from the measure, sector and duration names)`, so
every row always has a readable label.

**Change 2, Q3 population nulls:** the warning is **removed**.
- Those nulls are what the left join is designed to produce. A warning that fires on
  every run teaches people to ignore warnings.
- Instead, document the coverage gaps: DataUSA covers 2013–2019 and 2021–2024, with no
  2020 in the ACS 1-year estimates. BLS years outside that range have null population.

---

## D14. Whitespace: kept in Bronze, every string trimmed in Silver

**Status:** Decided 2026-11-13

**Evidence:**
- `series_id` is right-padded to 17 characters.
- `value` is left-padded.
- Header names are padded.

**Decision:**
- Bronze values are left exactly as they arrived. Only the header *names* are trimmed
  (D12).
- Silver trims every string column.

**Rationale:**
- Bronze stays faithful evidence of what was ingested.
- Trimming once, in Silver, means every consumer gets clean keys. An untrimmed
  `series_id` would make the Q3 filter match nothing.

---

## D15. Q3 join: left join on integer year

**Status:** Decided 2026-11-13

**Decision:**
- Join BLS data to population with a **left** join on `year`, as `INT = INT`.
- The BLS year is cast to an integer in Silver. DataUSA's `Year` is already an integer.

**Rationale:**
- The question asks for every year's value, with population "where available".
- An inner join would silently drop 1987–2012, 2020 and 2025–2026.
- Joining on matching types avoids implicit string-to-integer casts.

---

## D16. Values are DECIMAL, sized from the real data; a value that doesn't fit fails

**Status:** Decided 2026-11-13

**Decision:**
- Store `value` as `DECIMAL(p, s)`. Choose `p` and `s` from the real data: the largest
  number of integer digits and the most decimal places seen, plus headroom.
- A value that is numeric but doesn't fit must **fail loudly**. It must never become a
  null that the value-is-numeric drop rule (D13) silently removes.

**Rationale:**
- Q2 sums decimals. With DOUBLE they pick up rounding noise, which also makes ties
  (D17) unreliable.
- DECIMAL gives exact sums and real ties.
- Separating "not numeric" (drop) from "numeric but doesn't fit" (fail) keeps the drop
  rule from hiding a sizing bug.

---

## D17. Q2 ties: one row per series, earliest year wins, other tied years listed

**Status:** Decided 2026-11-13

**Decision:**
- Q2 returns exactly one row per series.
- If several years share the largest sum, the **earliest** year is reported.
- An `other_tied_years` column lists any other tied years (sorted; empty if none).
- The SQL and PySpark implementations use **identical** tie rules and ordering.

**Rationale:**
- One row per series keeps the Gold table key simple.
- The extra column means no tie is hidden.
- Identical, deterministic rules keep the parity check (D9) from failing at random.

---

## D18. DataUSA hashing: test first; hash the raw bytes if they're stable

**Status:** Decided 2026-11-13

**Decision:**
- Fetch the API twice and compare the bytes.
- If they're identical, hash the raw bytes.
- Normalise only if the test shows it's needed, and only as much as needed.
- In every case, write the bytes exactly as received.

**Rationale:** normalising adds code and its own risks. It only earns its place if the
API's output actually varies between calls.

**Result:** recorded below, under "D18 test result".

---

## D19. Run order is enforced in code: start a pipeline update whenever raw has changed since the last successfully started update

**Status:** Decided 2026-11-13; revised 2026-11-13 (see "Revision" below)

**Decision (revised):**
- The tool starts a pipeline update **whenever raw has changed since the last
  successfully started update**.
- This is tracked by a `refresh_pending` flag stored in the manifest:
  - **Set:** in the same manifest save as any raw write or raw delete, so it costs no
    extra write.
  - **Cleared:** once an update has **successfully started**.
  - **Checked:** an update is started only at the end of a fully successful run, so the
    pipeline never sees a half-finished ingestion.
- **`--no-refresh` sets the flag and leaves it set.** The next run with refresh enabled
  starts the update, even if that run changes nothing.
- If starting the update fails, the flag stays set and the next run tries again.

**Rationale:**
- Ingestion and the pipeline never overlap, so a refresh can't see half-updated files.
  A half-updated pair would make the D6 warning fire for no real reason.
- In the steady state (nothing pending and nothing changed), a run writes nothing and
  uses no compute against the Free Edition daily quota.

**Revision:**
- **Original wording:** "start a pipeline update only if *this run* changed raw".
- **Why it was wrong:** if a run wrote to raw and then failed (for example at DataUSA),
  no update started. The next run found nothing new, so it didn't start one either, and
  the pipeline stayed stale indefinitely.
- Claude raised the gap after Phase 1. I chose the pending flag.
- The original wording was wrong, not merely bent. The rule was always about raw having
  changed since the pipeline last ran.

**Accepted limitation (for PROCESS.md trade-offs):**
- The flag is cleared when the update **starts**, not when it **succeeds**.
- Whether the update succeeds is the pipeline's concern: its own run history, alerting
  and retries.
- Ingestion doesn't wait for the pipeline or track its result. A failed update must be
  noticed and re-run from the pipeline side.

### D18 test result (2026-11-13)
- **Test:** two fetches of the DataUSA URL, 5 seconds apart, with a generic User-Agent.
- **Result:** both were HTTP 200, 1,401 bytes, SHA-256
  `31e7b5d3df039c91f6e64f7f7befd98a9a1b6cb5277b600a425ea83eb1629a0f`. **The bytes were
  identical.**
- The response has no `ETag`, `Last-Modified` or `Cache-Control` header, so a conditional
  request isn't possible. Fetching and hashing is the only way to detect a change.
- **Outcome:** hash the raw bytes. No normalisation.

---

## D20. DataUSA requests identify the tool with a project URL, not a personal email

**Status:** Decided 2026-11-13

**Decision:**
- DataUSA requests send `User-Agent: rearc-ingest/<version> (+<repository URL>)`.
- The version and URL come from the package metadata in `pyproject.toml`
  (`[project.urls] Repository`), so they're configured once.
- A run refuses to start if the URL isn't configured.

**Rationale:**
- It identifies the tool and gives a contact route.
- It doesn't send my personal email to a service that hasn't asked for it. BLS does
  ask, so BLS requests keep `BLS_USER_AGENT`.

---

## D21. Size mismatch: retry a limited number of times, then fail; never write a mismatched file

**Status:** Decided 2026-11-13

**Decision:**
- A BLS download whose byte count differs from the listing size, shorter **or longer**,
  is retried a limited number of times. This uses the same attempt limit and backoff as
  other transient errors (`max_attempts`, default 4).
- If it still doesn't match, the run fails with a message saying the file may have
  changed on the server during the run, and to re-run it.
- A file whose size doesn't match the listing is **never written**.

**Rationale:**
- The listing size is the uncompressed byte count, and it matched exactly for every
  file inspected.
- A short body suggests a broken transfer. A long one suggests BLS republished the file
  between the listing and the download.
- Either way, the listing metadata and the content no longer describe the same version.
  Writing it would record listing metadata in the manifest that doesn't match the
  stored file.
- Re-running fetches a fresh listing and a consistent pair.

---

## D22. Parent folders in the Volumes: confirm in Phase 3, create them explicitly if needed

**Status:** Decided 2026-11-13

**Decision:**
- In Phase 3, confirm whether a Files API upload to a new path creates the missing
  parent folders.
- If it doesn't, create them explicitly in `SdkVolume` before writing.

**Result (Phase 3, 2026-11-13): no change needed.**
- The first real run uploaded to paths whose parent folders didn't exist, for example
  `raw/bls/pr/`, `raw/datausa/` and `archive/bls/pr/pr.class/`. All 26 data uploads and
  13 manifest writes succeeded.
- Listing the Volumes afterwards showed every file at its intended path.
- So `files.upload` creates parent folders itself, and `SdkVolume` keeps no explicit
  directory step.

**Side observation:** every SDK Files API session logs
`WARNING:databricks.sdk.mixins.files:Unexpected status resolving file endpoints (403 ...), using the default endpoints`.
- This is the SDK probing an optional endpoint, which Free Edition refuses, then falling
  back to the standard Files API.
- Uploads and downloads work. It is unrelated to the BLS 403 rule and needs no action.

---

## D23. The manifest is rewritten after every file (accepted trade-off)

**Status:** Decided 2026-11-13

**Decision:** keep saving the whole manifest after each file's write or removal.

**Trade-off:** crash granularity versus number of writes.
- **Benefit:** a crash leaves at most one file's write unrecorded, and the next run
  redoes it with the same bytes (D10).
- **Cost:** one small manifest rewrite per changed file, so 13 on a full first run plus
  one to clear the flag (D19). With about a dozen files that's negligible.

**For PROCESS.md, at scale:** batch the manifest writes (for example every N files or
every T seconds), accepting a larger redo window. Or keep the manifest in a Delta table,
where per-file updates are transactional rows rather than whole-file rewrites. A Delta
table also fixes D8's one-run-at-a-time limitation.

---

## D24. The pipeline trigger is at-least-once (accepted)

**Status:** Decided 2026-11-13

**Decision:** keep the order "start update, then clear `refresh_pending`" (D19).

**Consequence:**
- A crash after the update starts, but before the flag is cleared, leaves the flag set.
- The next run then starts a second update.
- That duplicate is harmless: refreshing a pipeline whose inputs haven't changed
  produces the same tables. The only cost is some compute.
- The reverse order (clear, then start) would be at-most-once. A crash in between could
  then lose an update entirely, which is the stale-pipeline failure D19 exists to prevent.

---

## D25. Recovery from a corrupted or lost manifest: delete it and re-run

**Status:** Decided 2026-11-13

**Decision:**
- A manifest that can't be read stops the run with a `ManifestError` (exit code 1). The
  run never guesses.
- The documented recovery is to delete `state/manifest.json` and run again.

**Why it's safe:**
- With no manifest, every listed file is treated as new and re-landed.
- Archive paths are named by content hash, so archive writes rewrite identical files.
- Raw copies are overwritten with the same bytes, if the source hasn't changed.
- Each write sets `refresh_pending`, so the pipeline is refreshed afterwards.
- The cost is one full download from BLS (about 6 MB, paced at one request per second).

**Verified by:** `test_deleted_manifest_relands_identical_bytes_and_sets_pending`. After a
first run, the manifest is deleted and the run repeated with `--no-refresh`. Every raw
and archive file is byte-identical, and `refresh_pending` is set.

**Limitation:** files removed before the manifest was lost are no longer known to have
been removed. That doesn't matter: they're absent from both the listing and raw, so
nothing reads them.

---

## D26. No `mode: development` on the bundle target

**Status:** Decided 2026-11-13

**Evidence:**
- With `mode: development`, `bundle validate` resolved the schema to
  `workspace.dev_<workspace username>_rearc_quest`.
- Setting `presets.name_prefix: ""` did not remove the prefix.
- With no `mode`, the names resolve exactly as intended: `workspace.rearc_quest` and its
  three Volumes.

**Decision:** the `dev` target has no `mode`. In Phase 4 the pipeline sets
`development: true` explicitly.

**Rationale:**
- Development mode exists to isolate per-developer copies of resources in a shared
  workspace. With one developer in one workspace, there's nothing to isolate.
- It would also put my workspace username in every schema and table name, including
  the screenshots in the submission.

**For PROCESS.md trade-offs:** on a client project, use one development target per
developer (with `mode: development`, so copies are isolated) plus a production target
(`mode: production`) that runs as a service principal, not as a person.

---

## D27. `prevent_destroy` on the archive Volume and the schema

**Status:** Decided 2026-11-13

**Why these two:**

| Volume | Can it be rebuilt? |
|---|---|
| `raw` | Yes: it mirrors what the sources publish now, so it can be re-downloaded. |
| `state` | Yes: per D25, deleting the manifest and re-running rebuilds it. |
| `archive` | **No.** Superseded versions that BLS no longer publishes can't be re-downloaded. |

- So `archive` gets `lifecycle.prevent_destroy: true`.
- The schema gets it too. Deleting the schema may delete the Volumes inside it.
  - I found no authoritative statement on this for bundles.
  - The indirect evidence: the bundle warns that "underlying data may be lost" when a
    schema is deleted or recreated (databricks/cli#3514), and the UC API deletes
    non-empty schemas with `force`.
  - Protecting the schema costs nothing, because it's never meant to be destroyed.
- `raw` and `state` are deliberately left unprotected.

**What the setting does (CLI v1.22.1):**
- It exists for both schemas and Volumes in the bundle JSON schema.
- **Documented behaviour:** it blocks `bundle destroy` and changes that force a resource
  to be recreated, as long as the resource is **still in the configuration**.
- **Tested with `bundle plan` only (2026-11-13; plan changes nothing):**

  | Temporary edit to the config | Plan result |
  |---|---|
  | Rename the archive Volume | `update volumes.archive`: an in-place rename, nothing destroyed, so the flag isn't involved |
  | Remove the archive block | `delete volumes.archive`, **no error**: confirms the known risk below |
  | Move the archive Volume to schema `default` | `recreate volumes.archive` (1 delete, 1 add), **no error or warning** |

- At this point it was unverified whether an actual `bundle deploy` or `bundle destroy`
  refuses. The plan doesn't show the check. The destroy test below settles it for
  `destroy`.

**Destroy test (2026-11-13), run while all three Volumes were empty:**
- **Pre-checks:**
  - `raw`, `archive` and `state` each listed 0 entries.
  - The schema held only those three Volumes, with no tables or functions.
  - `bundle summary` showed the bundle managing exactly the schema and the three Volumes.
- **Attempt 1:** `databricks bundle destroy --profile rearc`, run non-interactively.
  - Exit code 1. **Nothing was deleted.** The CLI stopped before reaching any per-resource
    check, so `prevent_destroy` was never exercised. Exact output:

    ```
    Error: this command will destroy all resources deployed by this bundle, including workspace files in the deployment directory.
    Deleting data assets such as schemas, pipelines, or volumes may cause permanent data loss and should be carefully reviewed.
    To proceed, use --auto-approve.

    Note for AI agents (claude-code): do not retry with the flag suggested above
    unless the user has explicitly approved it. The flag bypasses a safety check
    and the operation may be irreversible.
    ```

  - Afterwards: the schema and all three Volumes exist, and `bundle plan` shows 0 changes.
  - **Takeaway:** a non-interactive destroy without `--auto-approve` is itself a guard.
- **Attempt 2:** I ran `databricks bundle destroy --profile rearc` myself, interactively,
  in my terminal. It was **refused, and nothing was deleted.** Exact output:

  ```
  Error: resources.schemas.rearc_quest has lifecycle.prevent_destroy set, but the plan calls for this resource to be recreated or destroyed. To avoid this error, disable lifecycle.prevent_destroy for resources.schemas.rearc_quest
  resources.volumes.archive has lifecycle.prevent_destroy set, but the plan calls for this resource to be recreated or destroyed. To avoid this error, disable lifecycle.prevent_destroy for resources.volumes.archive
  ```

  - **Deleted:** nothing. That includes the **unprotected** `raw` and `state` Volumes and
    the bundle's workspace files.
  - **Refused:** the whole destroy, before any deletion. Both protected resources were
    reported.
  - **Partial outcome:** none. The check runs first and is all-or-nothing.
  - **Afterwards (Claude checked):**
    - `workspace.rearc_quest` exists, with Volumes `archive`, `raw` and `state`.
    - `bundle summary` lists all four.
    - The bundle files folder is still present.
    - `bundle plan` shows 0 to add, 0 to change, 0 to delete, 4 unchanged.
    - No redeploy was needed.

**Conclusions:**
- **Verified:** on CLI v1.22.1, `prevent_destroy` blocks `bundle destroy` for resources
  still in the configuration. It also protects everything else in the bundle as a side
  effect, because the whole destroy is refused.
- **Inferred, not tested:** the error wording ("recreated or destroyed") suggests a
  `bundle deploy` that would recreate a protected resource is refused the same way.
  `bundle plan` shows no warning for that case (see the table above).
- **Still true:** removing the resource block from the configuration bypasses the
  protection (databricks/cli#4872). The pytest below guards against that.
- **Consequence:** retiring this bundle deliberately now needs two steps. First deploy
  with `prevent_destroy: false`, then destroy. That's the intended friction.

**Automated mitigation for databricks/cli#4872:** `tests/test_bundle_config.py`.
- It reads `databricks.yml` and the resource files it includes.
- It **fails if the archive Volume or the schema is missing from `resources/`, or lacks
  `lifecycle.prevent_destroy: true`.**
- Two self-tests run the same check on a temporary copy of the bundle with the flag
  removed, and with the schema file deleted, and assert both are caught.
- Running pytest before every deploy turns "never remove these blocks" from a rule
  people have to remember into a check that fails.

**Known risk (accepted, documented):**
- **Removing the resource from the configuration still deletes it on the next deploy.**
  The protection is read only from the current configuration and isn't recorded in
  bundle state.
- This is databricks/cli#4872, which is open. The fix, databricks/cli#7499, is unmerged
  and not in v1.26.0, the latest release its CI checked against.
- **Mitigation, by process:** never remove the `archive` or `schema` blocks from
  `resources/`. To retire one deliberately, first deploy it with
  `prevent_destroy: false`, then remove it. Any change under `resources/` should be
  checked with `bundle plan` before deploying.
- **Last-resort mitigation:** Unity Catalog keeps data files for dropped managed tables
  and Volumes for a 7-day recovery window (DROP SCHEMA documentation).

---

## D28. Pipeline development mode is set per update, not on the pipeline

**Status:** Decided 2026-11-13

**Evidence:**
- Phase 4 asked for `development: true` on the pipeline.
- `bundle validate --strict` failed with: `Warning: The "development" property of
  pipelines is deprecated. Use "mode: development" in your databricks.yml instead.`
- The bundle JSON schema describes the field as
  "Deprecated: set development mode for each update instead."
- `mode: development` was rejected in D26.

**Options:**
1. Set the flag per update. **Chosen.**
2. Keep the field and validate without `--strict`.
3. No development mode.

**Decision:**
- The pipeline resource has no `development` field.
- `rearc-ingest` starts each update with `start_update(..., development=True)`,
  controlled by `Settings.pipeline_development`, which defaults to true.

**Rationale:**
- It follows the platform's own guidance.
- It keeps strict validation on every deploy.
- It keeps D26: no username in table names.
- Development mode gives no automatic retries and faster restarts, which suits a
  debugging phase and Free Edition's daily compute quota.

**For PROCESS.md:** a production target would start updates with
`pipeline_development=False`, so the platform's default retries apply.

**Tests:** `tests/test_refresh.py`:
- starts a development update on the exact name match only;
- refuses when zero or several pipelines match.

**D28 follow-up (first update, 2026-11-13): unverified.**
- `rearc-ingest` called `start_update(development=True)`, and update `c401ffe6` completed.
- The update API doesn't echo the flag back:
  - `get_update` shows `config.development: false`. That's the pipeline spec's value,
    which D28 deliberately leaves unset.
  - `mode: DEFAULT` refers to triggered versus continuous execution.
  - The `create_update` event has no details.
- So there's no API evidence yet that the server honoured the per-update flag. Still to
  be confirmed in the pipeline UI's update details.

**D28 resolved (2026-11-13): verified.**
- The pipeline event log's `create_update` event for update `c401ffe6` records the
  update's own settings. It has a top-level `"development": true`, separate from
  `config.development: false`, which is the pipeline spec's value.
- So the per-update flag reached the server and was applied.
- `get_update`'s API response simply doesn't expose it.

---

## D29. Bronze implementation choices (reviewed and accepted)

**Status:** Decided 2026-11-13

- **Python, not SQL.**
  - The header names are padded, for example `'series_id        '`.
  - The header check reads the file's actual header, trims it, and compares it with the
    expected list (D13). In SQL each padded name would have to be hard-coded, and the
    check couldn't be written.
- **Quote handling off (`quote = ""`).**
  - The BLS files are unquoted TSV. A profile of every raw file found no quote
    characters.
  - With quote handling on, a stray `"` in a value such as a series title would make the
    reader treat the following tabs as part of a quoted field, merging tab-separated
    columns.
- **Whitespace trimming off.** Values keep their padding. Trimming is Silver's job (D14).
- **snake_case renames for DataUSA keys.** They change labels, not values, which is
  consistent with trimming the BLS header names. Delta column names can't contain spaces
  without column mapping. The mapping:

  | Source key | Bronze column |
  |---|---|
  | `Nation ID` | `nation_id` |
  | `Nation` | `nation` |
  | `Year` | `year` |
  | `Population` | `population` |

- **`pr.seasonal`'s header is `Seasonal_code` / `Seasonal_text`.** It's trimmed only, so
  the capital S stays in Bronze. Silver may relabel it.
- **Empty fields arrive as NULL.**
  - Spark's CSV reader maps an empty field to NULL; in `bronze_pr_data`, 128,533
    `footnote_codes` values are NULL.
  - **Accepted:** in unquoted TSV, an empty field and a missing value can't be told apart,
    so NULL loses no information.
  - A reader-option change to keep `''` would add complexity for no gain.
- **Blank lines are skipped by the reader.** `pr.seasonal`'s blank last line doesn't
  appear in Bronze. Bronze's row counts equal each file's non-blank data lines.

---

## D30. Silver selects AllData by exact base file name (for Phase 5)

**Status:** Decided 2026-11-13

**Decision:** Silver's data input is the rows of `bronze_pr_data` whose `source_file`
**base name equals exactly `pr.data.1.AllData`**. Not a `contains` or `endswith` match.

**Evidence:** `source_file` is a full path with a scheme, for example
`dbfs:/Volumes/workspace/rearc_quest/raw/bls/pr/pr.data.1.AllData`.

**Rationale:**
- An exact comparison can't accidentally match a future `pr.data.1.AllData.bak` or a
  similarly named file.
- Comparing the base name keeps the rule independent of the path prefix and the scheme.
- If the file is renamed, the AllData-not-empty fail check (D12) catches it, rather than
  Silver silently becoming empty.

---

## D31. bronze_population keeps each value's exact source text (regex on the raw JSON text)

**Status:** Decided 2026-11-13

**Evidence:** every method was tested in SQL against the raw file in the Volume.

| Method | 2013 value | Matches source? |
|---|---|---|
| Source text | `316128839.0` | (reference) |
| JSON reader, `primitivesAsString` (Phase 4's first version) | `3.16128839E8` | no |
| `get_json_object` on the raw text | `3.16128839E8` | no |
| `parse_json` (VARIANT) then `::string` | `316128839` | no: drops `.0` |
| Regex on the raw text (chosen) | `316128839.0` | **yes**, for all 11 records |

**Options:** exact text by regex (**chosen**); VARIANT text, which is value-exact but
loses `.0`; or the whole document stored as one row, with parsing moved to Silver.

**Decision:**
- Read the file as one text value.
- Take the text inside `"data": [ ... ]`, split it into flat `{...}` records, and cut out
  each key's value text. For JSON strings, that's the characters inside the quotes.
- Fail expectations sit on a temporary view, `population_records`. They're expectations
  rather than Python checks because pipeline functions mustn't collect data.
  - `document_has_records`: at least one record was found.
  - `every_record_extracted`: the regex found as many records as a real JSON parser
    counts (`json_array_length`).
  - `record_keys_as_expected`: every record has exactly the four expected keys.

**Rationale:**
- Bronze is a true copy of the source text. Parsing and typing happen once, in Silver.
- **Accepted risk:** it depends on DataUSA's records staying flat, one level deep. A
  nested or reshaped record fails the guards loudly rather than passing silently.

**D31, for PROCESS.md: the regex-versus-parser trade-off.**
- **What a parser gives:** a JSON parser is the robust way to read JSON. It handles
  nesting, escaping and whitespace. But it turns numbers into values and prints them back
  in its own format: `3.16128839E8`, or `316128839` with the `.0` gone.
- **What regex gives:** a pattern match on the raw text keeps the source characters
  exactly. But it's only correct for the shape it assumes: flat records inside a `data`
  array.
- **Why regex here:** Bronze's job is to be a faithful copy. The shape assumption is
  checked on every update against a real parser's record count and an exact key set.
  If DataUSA changes shape, the update fails loudly instead of Bronze silently drifting
  from the source.
- **Where it doesn't fit:** for a large or deeply nested API, or one that changes often,
  I'd store the raw document (or VARIANT) in Bronze and parse in Silver, accepting
  value-exact rather than text-exact Bronze.

---

## D32. Two update triggers: data changes go through ingestion, code changes through deploy plus a deliberate update

**Status:** Decided 2026-11-13 (for PROCESS.md)

- **Data change:** `rearc-ingest` lands the changed files, sets `refresh_pending`, and
  starts a development-mode update at the end of a successful run (D19, D28). Nobody
  needs to remember to refresh the pipeline after new data.
- **Code change** (pipeline source or resources): `databricks bundle validate --strict`,
  then `databricks bundle deploy`, then a deliberate
  `databricks pipelines start-update <id> --development`.
  - `rearc-ingest` doesn't start an update here, because raw hasn't changed.
  - Deploying alone doesn't refresh any table.
- **Why they're separate:** a code change should be reviewed and run on purpose, not
  slipped in by the next ingestion. And an unchanged source shouldn't cost compute
  against the daily quota.
- **On a client project:** CI would deploy on merge and run the update (or a
  `--validate-only` update first). The production target would run as a service
  principal (D26).

---

## D33. Silver value type: DECIMAL(12,3), sized from the full AllData profile

**Status:** Decided 2026-11-13 (from D16's rule; precision and scale chosen by Claude, for my review)

**Evidence** (all 92,328 AllData rows in Bronze, values trimmed):
- Every value is numeric.
- Range −60.8 to 25,576.642.
- At most **5 digits before the point** and **3 after**: 54,431 values have 1 decimal
  place and 37,897 have 3.

**Decision:** `DECIMAL(12,3)`.
- **Scale 3:** exactly the most decimal places present. `pr.txt` also says indexes are
  stored to three places and percent changes to one.
- **Precision 12:** 9 digits before the point, which is 4 digits of headroom over the
  data. That's larger than any plausible index, percent change or scaled level. It also
  matches the documented 12-character `value` field in `pr.txt`.

**Overflow rule (D16):**
- A numeric value whose text doesn't match `^-?[0-9]{1,9}([.][0-9]{1,3})?$` fails the
  update.
- The check is on the text because `try_cast` to DECIMAL rounds extra decimal places
  silently instead of returning NULL. A NULL check alone would miss that.
- **Profiling pitfall found:** SQL `count(DISTINCT a, b, ...)` ignores rows with a NULL in
  any listed column. It reported 87,351 "duplicates" that are really rows with a NULL
  `footnote_codes`. Dedupe and accounting use NULL-safe methods instead (`dropDuplicates`,
  and `DISTINCT` over all columns).

---

## D34. Silver implementation and multi-row checks

**Status:** Built in Phase 5, 2026-11-13; accepted after my review (reasons below)

**How multi-row rules are checked:**
- An expectation evaluates one row of the dataset it's attached to.
- A rule that spans rows is therefore computed into a **check dataset** whose rows carry
  the answer, and the expectation is attached there:
  - `dq_unique_keys`: one row per key, with a `rows` count. Fail if `rows = 1` is
    violated, with one named expectation per dataset.
  - `dq_current_in_alldata`: one row per Current row, with `in_alldata` and
    `value_matches`. Warn (D6). Missing rows aren't counted again as a value mismatch.
  - `dq_alldata_input`: one aggregate row. Fail if `alldata_rows = 0` (D12).
- Check datasets are **materialized views**, because a temporary view is computed only
  when something reads it.

**Silver choices, made by Claude, beyond what D13 specified:**
- Python, with expectations on temporary views
  (`silver_pr_data_staged`, `silver_population_staged`), so the conditions can use raw
  text and flag columns that the published tables don't carry.
- **Drop and fail conditions are mutually exclusive.** Fail conditions skip rows being
  dropped, and `value_numeric` skips blank rows. So each dropped row is counted by
  exactly one expectation.
- **Two added expectations:**
  - `label_present` (warn) on `silver_pr_series`;
  - `lookup_key_unique` (fail), because a repeated lookup key would multiply rows in
    Silver's joins.
- Lookups keep code and text only. Class and seasonal lookups aren't promoted to Silver,
  since nothing uses them.
- `silver_pr_data` carries the label columns, per CLAUDE.md ("labels joined in").

**Reasons for accepting Claude's choices (my review):**
- **Non-overlapping drop and fail rules** keep the row accounting exact. Every Bronze
  AllData row is either in Silver, counted by exactly one drop expectation, or removed as
  an exact duplicate. Phase 5: 92,328 = 92,328 + 0 + 0 + 0.
- **Lookup-key uniqueness is fail, not warn.** A duplicate code would duplicate rows in
  every join that uses that lookup, and that would inflate the Q2 sums.

**Open, for Phase 6: checks must gate Gold** (closed by D36).
- A failing check fails the update, but it doesn't stop Gold from being refreshed in the
  same update unless Gold depends on the check.
- Gold's design must make the check datasets upstream of the Gold tables, or otherwise
  guarantee that a failing check leaves Gold unchanged.

---

## D35. Labels live only in silver_pr_series and are joined in Gold

**Status:** Decided 2026-11-13; implemented in Phase 6

**Decision:**
- Remove `label`, `measure_text`, `sector_name` and `duration_text` from `silver_pr_data`.
- They stay in `silver_pr_series`, and Gold joins them in.
- Done as part of the Phase 6 update, so it costs no extra update.
- **`period_name`** (Claude's addition, accepted) is also dropped from the published
  `silver_pr_data`. It's still used in the staged view by the `period_known` check.
- **Principle:** descriptive attributes live in the lookup tables, not in the facts.
  `silver_pr_data` holds keys and measures; names and labels come from
  `silver_pr_series` and the lookups, joined where they're needed.

**Rationale:**
- One copy of each label means one place to correct it.
- `silver_pr_data` stays a narrow fact table, with no label text repeated on 92,328 rows.

---

## D36. Gold depends on one gate table that reads every fail-level check

**Status:** Decided 2026-11-13; closes D34's open item

**Decision:**
- One gate table, `dq_gate`, reads every **fail-level** check. Every published Gold table
  reads the gate, so it's downstream of it.
- **Warn-level checks don't gate:** `dq_current_in_alldata` and Q2 coverage.

**Rationale, for PROCESS.md:**
- When a check fails, the update stops before Gold is refreshed. Readers keep seeing the
  last good answers: **"stale, but never wrong"**.
- Production would add an alert on pipeline failure, so that staleness is noticed.

**Implementation: the write-audit-publish pattern** (Claude's reading of "every fail-level check", accepted after my review):
- Some fail-level checks are about the answers themselves (D13): Q1 has exactly six years
  2013–2018; Q2 has one row per series and a valid basis; Q3 has at least one row; and
  SQL/PySpark parity.
- So each answer is first computed into **private candidate tables**:
  `q*_candidate_sql` (primary) and `q*_candidate_pyspark` (alternative).
- `dq_gold_parity` compares the two candidates.
- `dq_gate` reads:
  - the Silver checks (`dq_unique_keys`, `dq_alldata_input`);
  - the Silver tables themselves, so their row-level fail expectations are upstream;
  - `dq_gold_parity`;
  - the candidates' output checks.
- The published `gold_*` tables are SQL `SELECT`s from the SQL candidate, cross-joined to
  the one-row gate.
- Any failing check therefore stops the update before any `gold_*` table changes.
- **As write-audit-publish (WAP):**
  - **Write:** candidates are written privately, in SQL and PySpark.
  - **Audit:** the gate checks them, including SQL/PySpark parity, together with every
    other fail-level check.
  - **Publish:** the SQL version is copied through the gate into the published table.
- Gating on Silver checks alone would let a Gold table change even if a check on its own
  output then failed.

---

## D37. SQL is primary for all three Gold answers; PySpark versions are private alternatives

**Status:** Decided 2026-11-13

**Decision:**
- Each Gold answer's primary logic is a SQL materialized view.
- Each PySpark version is a private dataset: computed and parity-checked, but not
  published.

**Rationale:** Gold serves SQL consumers: analysts, Genie and dashboards. Bronze and
Silver are Python because they need code-level checks, such as the header check
(D29) and the staged-view expectations (D34).
