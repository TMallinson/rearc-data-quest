# Rearc Data Quest (Databricks Edition)

This project lands the BLS Major Sector Productivity and Costs (`pr`) time series and the
DataUSA national population into a Unity Catalog Volume. A Spark Declarative Pipeline
then models them as Bronze, Silver and Gold tables.

Design decisions and their evidence are in [notes/decisions.md](notes/decisions.md).

## Layout

| Path | What it is |
|---|---|
| `src/rearc_ingest/` | Local ingestion package and the `rearc-ingest` command |
| `tests/` | pytest suite; no network and no Databricks needed |
| `scripts/volume_check.py` | Read-only checks of the Volumes against the manifest |
| `docs/evidence/` | Saved run output used as evidence in PROCESS.md |
| `databricks.yml`, `resources/` | Asset Bundle: schema, Volumes and (later) the pipeline |
| `notes/` | Decision log and AI-usage log |

## Why ingestion runs locally

Databricks Free Edition serverless compute can't reach `download.bls.gov` or
`honolulu-api.datausa.io`. Ingestion therefore runs locally and writes to the Volume
through the Databricks SDK Files API. Everything downstream of the Volume runs in
Databricks. See D1 in the decision log.

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'
cp .env.example .env          # then set BLS_USER_AGENT to your name and email
databricks auth login --profile rearc --host <workspace URL>
.venv/bin/pytest
```

BLS blocks automated requests that carry no contact information, so `BLS_USER_AGENT` must
include an email address. DataUSA requests identify the tool with the repository URL from
`pyproject.toml` instead.

## Running ingestion

```bash
.venv/bin/rearc-ingest --dry-run          # show what would be downloaded or removed
.venv/bin/rearc-ingest --no-refresh       # ingest without starting a pipeline update
.venv/bin/rearc-ingest                    # ingest, then refresh the pipeline if needed
```

| Option | Effect |
|---|---|
| `--profile NAME` | Databricks CLI profile (default `rearc`) |
| `--catalog`, `--schema` | Override the target (default `workspace.rearc_quest`) |
| `--dry-run` | Fetch the listing and print the plan; download and write nothing |
| `--allow-removals` | Permit removals the guard would refuse (D11) |
| `--max-removals N` | Removals allowed without `--allow-removals` (default 2) |
| `--no-refresh` | Don't start a pipeline update; the change stays marked as pending (D19) |

What a run does:
1. **Detects changes.** Files are discovered from the BLS directory listing. A file is
   downloaded only if its listing timestamp or size changed (D7).
2. **Writes only real changes.** A file is written only if its SHA-256 hash changed. It
   goes first to an archive path named by its hash, then to `raw/` (D10). A second run
   with no source changes writes nothing.
3. **Mirrors removals.** Files that disappear from the listing are removed from `raw/`.
   A guard refuses more than 2 removals, or any `pr.data.*` removal, unless
   `--allow-removals` is passed (D11).
4. **Refreshes the pipeline when needed.** An update starts whenever raw has changed
   since the last successfully started update (D19).

### Exit codes

| Code | Meaning | What to do |
|---|---|---|
| 0 | Success | Nothing. |
| 1 | Failed: fetch error after retries, unparseable listing, size mismatch, or unreadable manifest | Read the message. For a size mismatch, re-run: the file may have changed during the run (D21). For an unreadable manifest, delete `state/manifest.json` and re-run (D25). |
| 2 | Configuration error: `BLS_USER_AGENT` missing or without an email, repository URL missing, or refresh requested with no pipeline configured | Fix the configuration. Nothing was fetched or written. |
| 3 | HTTP 403: the server refused the request | Stop. Check `BLS_USER_AGENT` and the BLS access policy. Never retried automatically. |
| 4 | Removal guard refused the run | Nothing was changed. Check the source, then re-run with `--allow-removals` if the removals are genuine (D11). |
| 5 | Ingested, but the pipeline update couldn't be started | The change stays marked as pending. The next run starts the update (D19). |

### Expected warnings

Every run, and every use of `scripts/volume_check.py`, prints a line like this:

```
WARNING:databricks.sdk.mixins.files:Unexpected status resolving file endpoints (403, request ID ...), using the default endpoints.
```

- **This is not the BLS 403.** It comes from the Databricks SDK, not from BLS. Before its
  first Files API call, the SDK probes an optional endpoint. Databricks Free Edition
  refuses that probe, so the SDK falls back to the standard Files API, and uploads and
  downloads work normally.
- **No action is needed.** The run's exit code is unaffected. See D22 in the decision log.
- **How to tell them apart:** a real BLS 403 stops the run with exit code 3 and a
  `STOPPED (403)` message naming a `download.bls.gov` URL.

## Checking the Volumes

`scripts/volume_check.py` is a read-only check of the Volumes. It only lists and downloads
files and never writes or deletes anything; a test enforces this.

```bash
.venv/bin/python scripts/volume_check.py check
.venv/bin/python scripts/volume_check.py snapshot before.json
.venv/bin/python scripts/volume_check.py compare before.json after.json
```

| Command | What it checks |
|---|---|
| `check` | For every manifest entry: the raw file's SHA-256 matches the manifest; the archive copy is named by that hash and is byte-identical to raw; a BLS raw file's size equals its listing size. |
| `snapshot` | Records every file's size and last-modified time in `raw`, `archive` and `state`, plus the manifest's SHA-256 and `refresh_pending`. |
| `compare` | Compares two snapshots. It reports files added, removed or changed, and whether the manifest hash differs. |

Exit codes: 0 when there are no problems, 1 otherwise.

When to run it:
- **After any ingestion run that wrote something:** run `check`, to confirm the Volumes
  and the manifest agree.
- **To prove a run wrote nothing:** run `snapshot` before the run and `snapshot` after
  it, then `compare` the two. That's how
  [docs/evidence/](docs/evidence/phase3_second_run_unchanged.txt) was produced.
- **Before trusting the Volumes after something unusual:** a crash, a manual change in
  the workspace, or a manifest recovery (D25).

## Volumes

All three Volumes live in `workspace.rearc_quest`:

| Volume | Contents |
|---|---|
| `raw` | Exactly what the sources currently publish: `bls/pr/<file>`, `datausa/population.json` |
| `archive` | Every version ever written, named by content hash: `<source>/<file>/<sha256>` |
| `state` | `manifest.json`: per-file listing metadata, hash, status, and the refresh-pending flag |
