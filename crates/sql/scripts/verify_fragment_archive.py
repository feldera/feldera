#!/usr/bin/env python3
"""Verify archived P0 artifact, source and classpath hashes without extracting files.

Integrity verification does not rerun experiments or establish their acceptance
criteria. The index explicitly maps historical source and certificate locations.
"""

import argparse
import hashlib
import json
import tarfile
from pathlib import Path

from normalize_evidence import normalize, portable_paths


def file_digest(stream):
    digest = hashlib.sha256()
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        digest.update(chunk)
    return digest.hexdigest()


def require_equal(actual, expected, description):
    if type(actual) is not type(expected) or actual != expected:
        raise ValueError(f"{description}: expected {expected}, got {actual}")


def classpath_entries(groups):
    """Older certificates group export and import classpaths by tool version."""
    for path, value in groups.items():
        if isinstance(value, dict):
            yield from classpath_entries(value)
        elif isinstance(value, str):
            yield path, value
        else:
            raise TypeError(f"Invalid classpath hash or group: {path}")


def directory_digest(entries, algorithm):
    digest = hashlib.sha256()
    for name, checksum in entries:
        if algorithm == "path-space-hex-sha256-newline":
            digest.update(f"{name} {checksum}\n".encode())
        elif algorithm == "path-null-binary-sha256":
            digest.update(name.encode() + b"\0")
            digest.update(bytes.fromhex(checksum))
        else:
            raise ValueError(f"Unknown classpath hash algorithm: {algorithm}")
    return digest.hexdigest()


def verify(index_path, repository):
    index = json.loads(portable_paths(index_path.read_text()))
    require_equal(index["schema_version"], 1, "archive index version")
    archive = repository / index["archive"].removeprefix("$REPO/")
    require_equal(archive.stat().st_size, index["bytes"], "archive bytes")
    with archive.open("rb") as stream:
        require_equal(file_digest(stream), index["sha256"], "archive SHA-256")
    certificates = {
        index["certificate_archive_prefix"] + name for name in index["certificates"]
    }
    hashes = {}
    portable_certificates = {}
    with tarfile.open(archive, "r:gz") as stream:
        for member in stream:
            if member.isfile():
                name = portable_paths("/" + member.name)
                if name in hashes:
                    raise ValueError(f"Duplicate archived file: {name}")
                with stream.extractfile(member) as contents:
                    if name in certificates:
                        raw = contents.read()
                        hashes[name] = hashlib.sha256(raw).hexdigest()
                        portable = normalize(raw.decode(), is_json=True)
                        portable_certificates[name] = hashlib.sha256(
                            portable.encode()
                        ).hexdigest()
                    else:
                        hashes[name] = file_digest(contents)

    artifacts = sources = classpaths = 0
    for name in index["certificates"]:
        certificate_path = repository / "crates/sql/evidence" / name
        raw = certificate_path.read_bytes()
        archived_name = index["certificate_archive_prefix"] + name
        digest = hashlib.sha256(raw).hexdigest()
        if digest != hashes.get(archived_name):
            require_equal(
                portable_certificates.get(archived_name),
                digest,
                f"archived certificate {name}",
            )
        certificate = json.loads(portable_paths(raw.decode()))
        require_equal(certificate["complete"], True, f"certificate completion {name}")
        for path, expected in certificate["artifacts"].items():
            require_equal(hashes.get(path), expected, f"artifact {path}")
            artifacts += 1
        for path, expected in certificate["sources"].items():
            mappings = [
                mapping
                for mapping in index["source_roots"][name]
                if path.startswith(mapping["source_prefix"])
            ]
            if len(mappings) != 1:
                raise ValueError(f"Source needs exactly one archive mapping: {path}")
            mapping = mappings[0]
            archived = mapping["archive_prefix"] + path[len(mapping["source_prefix"]) :]
            require_equal(hashes.get(archived), expected, f"source {path}")
            sources += 1
        algorithm = index.get("classpath_hash_algorithms", {}).get(
            name, "path-space-hex-sha256-newline"
        )
        for path, expected in classpath_entries(certificate["classpaths"]):
            if path in hashes:
                actual = hashes[path]
            else:
                prefix = path + "/"
                entries = [
                    (name[len(prefix) :], digest)
                    for name, digest in sorted(hashes.items())
                    if name.startswith(prefix)
                ]
                if not entries:
                    raise ValueError(f"Missing classpath directory: {path}")
                actual = directory_digest(entries, algorithm)
            require_equal(actual, expected, f"classpath {path}")
            classpaths += 1
    return {
        "archive_sha256_verified": True,
        "verified_artifact_hashes": artifacts,
        "verified_source_hashes": sources,
        "verified_classpath_hashes": classpaths,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("index", type=Path)
    parser.add_argument(
        "--repository", type=Path, default=Path(__file__).resolve().parents[3]
    )
    args = parser.parse_args()
    print(json.dumps(verify(args.index, args.repository), indent=2))


if __name__ == "__main__":
    main()
