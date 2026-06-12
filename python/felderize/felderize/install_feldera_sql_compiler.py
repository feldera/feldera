from __future__ import annotations

import json
import re
import sys
import urllib.request
from pathlib import Path

from felderize.constants import (
    COMPILER_JAR_PREFIX,
    FELDERIZE_DIR,
    GITHUB_RELEASES_API,
    HTTP_TIMEOUT,
    MINIMUM_COMPILER_VERSION,
)


def _parse_version(tag: str) -> tuple[int, ...]:
    """Parse a release tag like 'v0.304.0' into a comparable tuple (0, 304, 0).

    Returns an empty tuple when no version numbers are present, which compares
    less than any real version.
    """
    return tuple(int(n) for n in re.findall(r"\d+", tag))


def is_supported_version(tag: str) -> bool:
    """Return True if the release tag meets felderize's minimum compiler version."""
    return _parse_version(tag) >= _parse_version(MINIMUM_COMPILER_VERSION)


def jar_version(path: str | Path) -> str | None:
    """Extract the version tag (e.g. 'v0.304.0') from a compiler JAR filename."""
    match = re.search(r"v\d+\.\d+\.\d+", Path(path).name)
    return match.group(0) if match else None


def _fetch_release(version: str | None) -> dict:
    url = (
        f"{GITHUB_RELEASES_API}/tags/{version}"
        if version
        else f"{GITHUB_RELEASES_API}/latest"
    )
    req = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return json.loads(resp.read())


def _find_jar_asset(release: dict) -> tuple[str, str]:
    for asset in release.get("assets", []):
        name: str = asset["name"]
        if name.startswith(COMPILER_JAR_PREFIX) and name.endswith(".jar"):
            return name, asset["browser_download_url"]
    raise RuntimeError(
        f"No compiler JAR found in release {release['tag_name']}. "
        "Check https://github.com/feldera/feldera/releases for available assets."
    )


def download_compiler(
    output_dir: Path | None = None,
    version: str | None = None,
    force: bool = False,
    logs=None,
) -> Path:
    """Download sql2dbsp JAR from GitHub releases. Returns the path to the JAR.

    Args:
        output_dir: Directory to save the JAR (default: ~/.felderize/).
        version: Release tag (e.g. "v0.291.0"); defaults to latest.
        force: Overwrite existing file if present.
        logs: Where to write progress messages (default: stdout). Pass
            sys.stderr when stdout must stay machine-clean (e.g. --json-output).
    """
    out = logs or sys.stdout
    dest_dir = output_dir or FELDERIZE_DIR
    dest_dir.mkdir(parents=True, exist_ok=True)

    release = _fetch_release(version)
    tag = release["tag_name"]
    name, url = _find_jar_asset(release)
    dest = dest_dir / name

    # When no version is requested, _fetch_release resolves to the newest release.
    is_latest = version is None

    if not is_supported_version(tag):
        print(
            f"Warning: {tag} is older than the minimum supported compiler version "
            f"{MINIMUM_COMPILER_VERSION}; translations validated against it may be "
            "inaccurate.",
            file=sys.stderr,
        )

    if dest.exists() and not force:
        status = "the latest release" if is_latest else "installed"
        print(f"Already on {status}: {name} ({tag})", file=out)
        return dest

    last_pct = [-1]

    def _progress(block_num: int, block_size: int, total_size: int) -> None:
        if total_size <= 0:
            return
        downloaded = min(block_num * block_size, total_size)
        pct = downloaded * 100 // total_size
        if pct == last_pct[0]:
            return
        last_pct[0] = pct
        bar = "#" * (pct // 5)
        print(
            f"\r  [{bar:<20}] {pct:3d}%  {downloaded / 1_048_576:.1f}/{total_size / 1_048_576:.1f} MB",
            end="",
            flush=True,
            file=out,
        )

    latest_note = " (latest release)" if is_latest else ""
    print(f"Downloading {name} ({tag}){latest_note}...", file=out)
    urllib.request.urlretrieve(url, dest, reporthook=_progress)
    print(file=out)  # newline after progress bar

    return dest


def find_local_compiler(search_dir: Path | None = None) -> Path | None:
    """Return the newest compiler JAR already cached in search_dir.

    Looks for ``sql2dbsp-jar-with-dependencies-*.jar`` files in search_dir
    (default ``~/.felderize/``). Prefers versions felderize supports; among
    those, the highest version. Falls back to the highest unsupported version
    when no supported one is cached (validation then warns). Returns None when
    the directory holds no compiler JAR.
    """
    directory = search_dir or FELDERIZE_DIR
    if not directory.is_dir():
        return None

    jars = [p for p in directory.glob(f"{COMPILER_JAR_PREFIX}*.jar") if p.is_file()]
    if not jars:
        return None

    def version_key(path: Path) -> tuple[int, ...]:
        tag = jar_version(path.name)
        return _parse_version(tag) if tag else ()

    supported = [
        p for p in jars if (tag := jar_version(p.name)) and is_supported_version(tag)
    ]
    return max(supported or jars, key=version_key)


def ensure_compiler(
    search_dir: Path | None = None,
    auto_download: bool = True,
    logs=None,
) -> Path | None:
    """Return a usable compiler JAR, downloading the latest one if needed.

    Resolution order:
      1. The newest compiler JAR already cached in search_dir (~/.felderize/).
      2. Otherwise, when auto_download is set, download the latest release.

    Returns None when nothing is cached and downloading is disabled or fails;
    callers then fall back to their "compiler not found" handling.
    """
    local = find_local_compiler(search_dir)
    if local is not None:
        return local
    if not auto_download:
        return None
    try:
        return download_compiler(output_dir=search_dir, logs=logs)
    except Exception as e:  # network/API failure — degrade gracefully
        print(
            f"Warning: could not auto-download the Feldera compiler ({e}); "
            "validation will be skipped. Run 'felderize download-compiler' or set "
            "FELDERA_COMPILER to validate.",
            file=sys.stderr,
        )
        return None
