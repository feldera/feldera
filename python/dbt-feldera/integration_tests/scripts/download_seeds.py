#!/usr/bin/env python3
"""Download seed data for a dbt project from GitHub Gist.

Reads a ``ci_seeds.yaml`` manifest from the given project directory
and idempotently downloads any missing files.
Uses only Python stdlib — no extra dependencies required.

Usage:
    python download_seeds.py /path/to/dbt-project
"""

from __future__ import annotations

import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

MANIFEST = "ci_seeds.yaml"


def _load_manifest(project_dir: Path) -> dict:
    path = project_dir / MANIFEST
    if not path.exists():
        raise SystemExit(f"ERROR: manifest not found at {path}")
    text = path.read_text()

    gist_id_m = re.search(r'^gist_id:\s*"(.+?)"', text, re.MULTILINE)
    gist_owner_m = re.search(r'^gist_owner:\s*"(.+?)"', text, re.MULTILINE)
    if not gist_id_m or not gist_owner_m:
        raise SystemExit("ERROR: could not parse gist_id / gist_owner from manifest")

    files = [
        {"path": m.group(1).strip(), "filename": m.group(2).strip()}
        for m in re.finditer(r"-\s+path:\s*(.+?)\n\s+filename:\s*(.+?)(?:\n|$)", text)
    ]
    return {"gist_id": gist_id_m.group(1), "gist_owner": gist_owner_m.group(1), "files": files}


def main(project_dir: Path) -> int:
    project_dir = project_dir.resolve()
    manifest = _load_manifest(project_dir)
    base_url = f"https://gist.githubusercontent.com/{manifest['gist_owner']}/{manifest['gist_id']}/raw"

    downloaded = skipped = failed = 0
    for entry in manifest["files"]:
        dest = project_dir / entry["path"]
        if dest.exists():
            skipped += 1
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".tmp")
        try:
            urllib.request.urlretrieve(f"{base_url}/{entry['filename']}", tmp)
            tmp.rename(dest)
            print(f"  Downloaded {entry['path']}")
            downloaded += 1
        except urllib.error.URLError as exc:
            tmp.unlink(missing_ok=True)
            print(f"  FAILED {entry['path']}: {exc}", file=sys.stderr)
            failed += 1

    total = len(manifest["files"])
    print(f"seed-ci: {downloaded} downloaded, {skipped} already present, {failed} failed (total: {total} files)")
    return 1 if failed else 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <project-directory>", file=sys.stderr)
        raise SystemExit(1)
    raise SystemExit(main(Path(sys.argv[1])))
