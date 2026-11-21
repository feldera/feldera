#!/usr/bin/env python3
"""Check all packages in SQL probe lockfiles, including target-specific packages."""

import ast
import subprocess
import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    tomllib = None

BLOCKED = {"openssl", "openssl-sys", "native-tls", "boring", "boring-sys"}
# DBSP uses tokio-rustls and the storage HTTP client. See crates/sql/LICENSING.md.
DBSP_PARENTS = {
    "ring": {"quinn-proto", "rustls-webpki"},
    "rustls": {
        "hyper-rustls",
        "quinn",
        "quinn-proto",
        "reqwest",
        "rustls-platform-verifier",
        "tokio-rustls",
    },
}


def read_cargo_packages(stream):
    """Read package fields from a Cargo-generated lockfile."""
    packages = []
    package = None
    for line in stream:
        content = line.partition("#")[0].strip()
        if not content:
            continue
        if content == "[[package]]":
            package = {}
            packages.append(package)
            continue
        if content.startswith("[") and content.endswith("]"):
            package = None
            continue
        key, separator, value = line.partition("=")
        if not separator:
            raise ValueError(f"expected a lockfile field in {content!r}")
        key = key.strip()
        if package is None or key not in ("name", "version", "dependencies"):
            continue
        if key in package:
            raise ValueError(f"duplicate package field {key}")
        # Cargo writes dependency lists across lines with a trailing comma.
        if key == "dependencies" and value.partition("#")[0].strip() == "[":
            lines = [value]
            for entry in stream:
                lines.append(entry)
                if entry.partition("#")[0].strip().endswith("]"):
                    break
            value = "".join(lines)
        package[key] = ast.literal_eval(value.strip())
    if not packages:
        raise KeyError("package")
    return packages


def check(path):
    with path.open(encoding="utf-8") as stream:
        if tomllib is None:
            packages = read_cargo_packages(stream)
        else:
            packages = tomllib.loads(stream.read())["package"]
    names = {package["name"] for package in packages}
    issues = []
    if blocked := names & BLOCKED:
        issues.append(
            f"remove the second crypto implementation {', '.join(sorted(blocked))}"
        )
    if {"aws-lc-sys", "aws-lc-fips-sys"} <= names:
        issues.append("select one AWS-LC backend across dependencies")
    for backend, allowed in DBSP_PARENTS.items():
        if backend not in names:
            continue
        parents = {
            pkg["name"]
            for pkg in packages
            if any(dep.split()[0] == backend for dep in pkg.get("dependencies", []))
        }
        if path == Path("crates/sql/native-dbsp-probe/Cargo.lock") and parents:
            print(f"  {backend} via {', '.join(sorted(parents))} (DBSP test probe)")
            parents -= allowed
            if not parents:
                continue
        callers = ", ".join(sorted(parents)) or "the probe"
        issues.append(f"remove the dependency on {backend} from {callers}")
    for issue in issues:
        print(
            f"{path}: {issue}. Inspect cargo tree --manifest-path {path.with_name('Cargo.toml')} "
            "--target all -e features.",
            file=sys.stderr,
        )
    return not issues


def main():
    files = subprocess.check_output(
        [
            "git",
            "ls-files",
            "-coz",
            "--exclude-standard",
            "--",
            "crates/sql/*Cargo.lock",
        ]
    )
    ok = True
    for name in sorted(files.decode().split("\0")):
        path = Path(name)
        if path.name == "Cargo.lock":
            print(f"Checking {path}")
            try:
                if not check(path):
                    ok = False
            except (OSError, ValueError, SyntaxError, KeyError) as error:
                print(
                    f"Cannot read {path}: {error}. Restore the lockfile and run the check again.",
                    file=sys.stderr,
                )
                ok = False
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
