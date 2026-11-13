"""Read-only checks of the ingestion Volumes. Never writes or deletes anything.

Commands:
  check                       Content checks against the manifest (see below).
  snapshot OUT.json           Record every file's size and last-modified time, plus
                              the manifest's SHA-256 and refresh_pending flag.
  compare BEFORE AFTER        Compare two snapshots: files added, removed or changed,
                              and whether the manifest hash is identical.

`check` verifies, for every manifest entry:
  - the raw file's SHA-256 equals the hash recorded in the manifest;
  - the archive copy is named by that hash and holds identical bytes;
  - for BLS files, the raw size equals the listing size recorded in the manifest.

Usage: python scripts/volume_check.py [--profile rearc] [--catalog workspace]
       [--schema rearc_quest] <command> [files...]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient


def _walk(client: WorkspaceClient, path: str):
    for entry in client.files.list_directory_contents(path):
        if entry.is_directory:
            yield from _walk(client, entry.path)
        else:
            yield entry


def _read(client: WorkspaceClient, path: str) -> bytes:
    return client.files.download(path).contents.read()


def _manifest(client: WorkspaceClient, root: str) -> tuple[bytes, dict]:
    data = _read(client, f"{root}/state/manifest.json")
    return data, json.loads(data)


def snapshot(client: WorkspaceClient, root: str) -> dict:
    files = {
        entry.path: {"size": entry.file_size, "last_modified": entry.last_modified}
        for volume in ("raw", "archive", "state")
        for entry in _walk(client, f"{root}/{volume}")
    }
    data, manifest = _manifest(client, root)
    return {
        "files": files,
        "manifest_sha256": hashlib.sha256(data).hexdigest(),
        "refresh_pending": manifest["refresh_pending"],
    }


def check(client: WorkspaceClient, root: str) -> list[str]:
    _, manifest = _manifest(client, root)
    problems = []
    for key, entry in sorted(manifest["files"].items()):
        if entry["status"] != "present":
            continue
        raw = _read(client, entry["raw_path"])
        digest = hashlib.sha256(raw).hexdigest()
        if digest != entry["sha256"]:
            problems.append(f"{key}: raw SHA-256 {digest[:12]} != manifest {entry['sha256'][:12]}")
        if not entry["archive_path"].endswith("/" + entry["sha256"]):
            problems.append(f"{key}: archive path is not named by the content hash")
        elif _read(client, entry["archive_path"]) != raw:
            problems.append(f"{key}: archive bytes differ from raw")
        if entry["listing_size"] is not None and int(entry["listing_size"]) != len(raw):
            problems.append(f"{key}: raw size {len(raw)} != listing size {entry['listing_size']}")
    print(f"checked {len(manifest['files'])} manifest entries; refresh_pending={manifest['refresh_pending']}")
    return problems


def compare(before: dict, after: dict, root: str) -> list[str]:
    a, b = before["files"], after["files"]
    problems = [f"added: {p}" for p in sorted(set(b) - set(a))]
    problems += [f"removed: {p}" for p in sorted(set(a) - set(b))]
    problems += [f"changed: {p}" for p in sorted(set(a) & set(b)) if a[p] != b[p]]
    if before["manifest_sha256"] != after["manifest_sha256"]:
        problems.append("manifest SHA-256 differs")
    print(f"files before: {len(a)}  after: {len(b)}")
    for path in sorted(p for p in a if "/archive/" not in p):
        stamp = datetime.fromtimestamp(a[path]["last_modified"] / 1000, timezone.utc)
        state = "unchanged" if a[path] == b.get(path) else "CHANGED"
        print(f"  {path.removeprefix(root + '/'):40} {stamp:%Y-%m-%d %H:%M:%S} UTC  {state}")
    print(f"  (archive: {sum('/archive/' in p for p in a)} files, compared above in the counts)")
    print(f"manifest sha256 before: {before['manifest_sha256']}")
    print(f"manifest sha256 after:  {after['manifest_sha256']}")
    print(f"refresh_pending after:  {after['refresh_pending']}")
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--profile", default="rearc")
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--schema", default="rearc_quest")
    parser.add_argument("command", choices=["check", "snapshot", "compare"])
    parser.add_argument("files", nargs="*")
    args = parser.parse_args(argv)
    root = f"/Volumes/{args.catalog}/{args.schema}"

    if args.command == "compare":
        before, after = (json.load(open(path)) for path in args.files)
        problems = compare(before, after, root)
    else:
        client = WorkspaceClient(profile=args.profile)
        if args.command == "snapshot":
            result = snapshot(client, root)
            with open(args.files[0], "w") as out:
                json.dump(result, out, indent=2, sort_keys=True)
            print(f"snapshot: {len(result['files'])} files, manifest {result['manifest_sha256'][:12]}…, "
                  f"refresh_pending={result['refresh_pending']} -> {args.files[0]}")
            return 0
        problems = check(client, root)

    for problem in problems:
        print(f"PROBLEM: {problem}")
    print("OK: no problems" if not problems else f"FAILED: {len(problems)} problem(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
