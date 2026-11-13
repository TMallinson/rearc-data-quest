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
