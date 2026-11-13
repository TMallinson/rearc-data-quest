# Rearc Data Quest (Databricks Edition)

## How to work with me
- This is a take-home for a senior data engineer role. I must be able to explain every line in an interview, so I make the decisions and you draft, explain, and verify.
- I'm new to Spark Declarative Pipelines and Databricks Asset Bundles. When you use a concept for the first time, explain it in a sentence or two before writing code.
- Never make a judgment call silently. If a choice affects correctness or interpretation, stop, lay out the options with trade-offs, and wait for my answer. Record my decision and reasoning in `notes/decisions.md`.
- Work in small, reviewable steps, one phase at a time.
- Don't say something works until you've run it and shown me evidence: command output, row counts, query results.
- Your knowledge of Databricks may be out of date (DLT was renamed to Lakeflow, then to Spark Declarative Pipelines, and the API changed). If you're unsure of an API or syntax, say so and check the installed Databricks skills or `databricks <command> --help` instead of guessing.

## Requirements
- `private/quest.md` is the source of truth. Re-read the relevant section before starting each phase.
- `private/` is gitignored. Never commit it, and never copy quest text into committed files.

## Environment
- Databricks Free Edition: serverless compute only. Never put cluster settings (`new_cluster`, `node_type_id`, `spark_version`) in any config. Pipelines and jobs must be serverless.
- Databricks CLI profile: `rearc`. Check the catalog name before using it (expected: `workspace`). Schema: `rearc_quest`. Volume: `raw`. Declare the schema and Volume as bundle resources.
- Local Python environment: `.venv` in the repo root.

## Network constraint (decided: ingest locally)
- Evidence, to be written up in PROCESS.md:
  - From Databricks serverless, DNS lookups were refused for download.bls.gov, honolulu-api.datausa.io and example.com, while pypi.org returned HTTP 200. So Free Edition only allows its own list of trusted domains.
  - I completed Databricks' LinkedIn verification, which is documented as unlocking outbound internet access, then terminated and restarted compute. The results were the same.
  - From my local machine, both sources return HTTP 200 with the BLS User-Agent.
- Decision: ingestion runs locally and lands files in the Volume through the Databricks SDK Files API. Everything downstream of the Volume runs in Databricks.
- Record this decision and the evidence in `notes/decisions.md` in the first session.
- Write to the Volume only through the SDK, never through local file paths, and authenticate with the `rearc` CLI profile.
- Don't build a Databricks job for ingestion. Do avoid local-only assumptions (local state, local paths), so PROCESS.md can truthfully say the same package would run as a scheduled serverless job on a paid workspace that allows these two domains.

## Ingestion rules (BLS policy)
- Read the User-Agent from the `BLS_USER_AGENT` environment variable, set in a gitignored `.env`, with a `.env.example` placeholder committed.
- Discover files by parsing the directory listing. Never hardcode filenames.
- Download one file at a time, pausing about a second between requests.
- Keep the manifest (state) inside the Volume, never on a local machine.
- Detect added, changed, and removed files, and skip unchanged ones. How to handle removed files is my decision: ask me.
- Running ingestion a second time with no source changes must write nothing. Prove it.
- A 403 means stop and report clearly, with no retry. Retry with backoff only on timeouts, connection errors, and 5xx responses.

## Pipeline conventions
- Python API: `from pyspark import pipelines as dp`. Never `import dlt`.
- Bronze: data exactly as ingested, every column as a string, plus metadata (source file, ingestion time).
- Silver: typed, trimmed, deduplicated, with labels joined in. Every expectation needs a stated reason for its action (warn, drop, or fail).
- Gold: one table per question, implemented in both SQL and PySpark, plus an automated check that both produce identical results.
- Read the BLS documentation (pr.txt, pr.period, and the other lookup files) before writing any aggregation. Raise these with me rather than deciding them yourself:
  - Overlap between the Current and AllData files.
  - What Q05 means and how to treat it.
  - Sample vs population standard deviation.
  - Join type and join-key types for Q3.
  - Whitespace padding in fields.

## Repository and workflow
- The Asset Bundle lives at the repo root (`databricks.yml`). Run `databricks bundle validate` before every deploy.
- Use pytest for the pure-Python logic (change detection, listing parsing), with no network or Spark needed.
- Never commit `private/`, `.env`, credentials, or data files.
- After I approve a phase's commit, push it to `origin` (`main`). Don't push anything I haven't approved.
- Ask before any destructive command: deleting files, overwriting existing data, dropping tables, `bundle destroy`.
- Free Edition has a daily compute quota, so don't rerun pipelines in a loop while debugging. Ask me first.
- At the end of each session, append to `notes/ai_log.md`: the date, what you drafted, what I changed or rejected, and how we verified it.