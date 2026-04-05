#!/usr/bin/env python3
"""Upload / update dbt-adventureworks seed files on GitHub Gist, then regenerate ci_seeds.yaml.

Requires ``gh`` CLI (authenticated).

Usage:
    python upload_seeds.py                    # create new gist
    python upload_seeds.py --update           # update existing gist from ci_seeds.yaml
    python upload_seeds.py /path/to/seeds     # explicit seeds dir
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

MANIFEST = "ci_seeds.yaml"
EXTS = {".csv", ".yml", ".sql"}
SCAN_DIRS = ["macros", "models", "seeds"]


def _project_dir_default() -> Path:
    return Path(__file__).resolve().parent.parent / "dbt-adventureworks"


def _find_files(project_dir: Path) -> list[Path]:
    files = []
    for d in SCAN_DIRS:
        target = project_dir / d
        if target.is_dir():
            files.extend(p for p in target.rglob("*") if p.is_file() and p.suffix in EXTS and p.name != MANIFEST)
    return sorted(files)


def _gh(*args: str) -> str:
    return subprocess.run(["gh", *args], capture_output=True, text=True, check=True).stdout.strip()


def _read_gist_id(project_dir: Path) -> str:
    text = (project_dir / MANIFEST).read_text()
    m = re.search(r'^gist_id:\s*"(.+?)"', text, re.MULTILINE)
    if not m:
        raise SystemExit("ERROR: could not parse gist_id from ci_seeds.yaml")
    return m.group(1)


def _write_manifest(project_dir: Path, gist_id: str, gist_owner: str, files: list[Path]) -> None:
    lines = [
        "# Manifest for downloading dbt-adventureworks project files from GitHub Gist.",
        "# Used by integration_tests/scripts/download_seeds.py",
        "#",
        "# Raw URL pattern:",
        "#   https://gist.githubusercontent.com/{gist_owner}/{gist_id}/raw/{filename}",
        "",
        f'gist_id: "{gist_id}"',
        f'gist_owner: "{gist_owner}"',
        "",
        "files:",
    ]
    cur_dir = None
    for f in files:
        rel = str(f.relative_to(project_dir))
        d = str(Path(rel).parent)
        if d != cur_dir:
            cur_dir = d
            lines.append(f"  # {d}")
        lines.append(f"  - path: {rel}")
        lines.append(f"    filename: {f.name}")
    (project_dir / MANIFEST).write_text("\n".join(lines) + "\n")
    print(f"Wrote {project_dir / MANIFEST}")


def main() -> int:
    args = sys.argv[1:]
    update = "--update" in args
    args = [a for a in args if a != "--update"]
    project_dir = (Path(args[0]) if args else _project_dir_default()).resolve()

    files = _find_files(project_dir)
    if not files:
        print(f"No seed files found in {project_dir}", file=sys.stderr)
        return 1
    print(f"Found {len(files)} seed files")

    gist_owner = _gh("api", "user", "-q", ".login")

    if update:
        gist_id = _read_gist_id(project_dir)
        print(f"Updating gist {gist_id} ...")
        cmd = ["gist", "edit", gist_id]
        for f in files:
            cmd.extend(["-a", str(f)])
        _gh(*cmd)
    else:
        print("Creating new public gist ...")
        url = _gh(
            "gist",
            "create",
            "--public",
            "-d",
            "dbt-adventureworks seed data for feldera integration tests",
            *[str(f) for f in files],
        )
        gist_id = url.rstrip("/").rsplit("/", 1)[-1]
        print(f"Gist created: {url}")

    _write_manifest(project_dir, gist_id, gist_owner, files)
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
