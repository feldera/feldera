"""Tamper checks for local archive integrity, independent of experiment verdicts."""

import hashlib
import io
import json
import tarfile
import tempfile
import unittest
from pathlib import Path

from normalize_evidence import normalize
from verify_fragment_archive import verify


def sha(value):
    return hashlib.sha256(value).hexdigest()


class FragmentArchiveTests(unittest.TestCase):
    def fixture(self, root, change=None, duplicate=False, omit_class=False):
        artifact, source, bytecode, jar = b"result", b"source", b"bytecode", b"jar"
        certificate = {
            "complete": True,
            "artifacts": {"/tmp/result": sha(artifact)},
            "sources": {"src/lib.rs": sha(source)},
            "classpaths": {
                "/tmp/classes": sha(f"A.class {sha(bytecode)}\n".encode()),
                "/tmp/dependency.jar": sha(jar),
            },
        }
        if change:
            change(certificate)
        encoded = json.dumps(certificate).encode()
        evidence = root / "crates/sql/evidence"
        evidence.mkdir(parents=True)
        (evidence / "test.json").write_bytes(encoded)
        entries = [
            ("workspace/evidence/test.json", encoded),
            ("tmp/result", artifact),
            ("tmp/source/lib.rs", source),
            ("tmp/dependency.jar", jar),
        ]
        if not omit_class:
            entries.append(("tmp/classes/A.class", bytecode))
        if duplicate:
            entries.append(("tmp/result", artifact))
        archive = root / "test.tar.gz"
        with tarfile.open(archive, "w:gz") as stream:
            for name, contents in entries:
                member = tarfile.TarInfo(name)
                member.size = len(contents)
                stream.addfile(member, io.BytesIO(contents))
        index = {
            "schema_version": 1,
            "archive": archive.name,
            "bytes": archive.stat().st_size,
            "sha256": sha(archive.read_bytes()),
            "certificates": ["test.json"],
            "certificate_archive_prefix": "/workspace/evidence/",
            "source_roots": {
                "test.json": [
                    {"source_prefix": "src/", "archive_prefix": "/tmp/source/"}
                ]
            },
        }
        index_path = root / "index.json"
        index_path.write_text(json.dumps(index))
        return index_path

    def test_artifact_source_and_both_classpath_forms_are_verified(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = verify(self.fixture(root), root)
            self.assertEqual(result["verified_artifact_hashes"], 1)
            self.assertEqual(result["verified_source_hashes"], 1)
            self.assertEqual(result["verified_classpath_hashes"], 2)
            self.assertTrue(result["archive_sha256_verified"])
            self.assertFalse((root / "tmp").exists())  # No extraction.

    def test_portable_certificate_matches_archived_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            index_path = self.fixture(root)
            certificate = root / "crates/sql/evidence/test.json"
            for path in (index_path, certificate):
                path.write_text(normalize(path.read_text(), is_json=True))
            result = verify(index_path, root)
            self.assertEqual(result["verified_classpath_hashes"], 2)
            certificate.write_text(
                certificate.read_text().replace(sha(b"result"), sha(b"changed"))
            )
            with self.assertRaisesRegex(ValueError, "archived certificate"):
                verify(index_path, root)

    def test_inner_hash_tampering_fails_even_with_valid_archive_hash(self):
        for field, key in [
            ("artifacts", "/tmp/result"),
            ("sources", "src/lib.rs"),
            ("classpaths", "/tmp/classes"),
            ("classpaths", "/tmp/dependency.jar"),
        ]:
            with (
                self.subTest(field=field, key=key),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                index = self.fixture(
                    root,
                    change=lambda cert, field=field, key=key: cert[field].update(
                        {key: "0" * 64}
                    ),
                )
                with self.assertRaises(ValueError):
                    verify(index, root)

    def test_archive_hash_tampering_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            index_path = self.fixture(root)
            archive = root / "test.tar.gz"
            archive.write_bytes(archive.read_bytes() + b"changed")
            index = json.loads(index_path.read_text())
            index["bytes"] = archive.stat().st_size
            index_path.write_text(json.dumps(index))
            with self.assertRaisesRegex(ValueError, "archive SHA-256"):
                verify(index_path, root)

    def test_duplicate_archive_members_are_not_last_writer_wins(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, "Duplicate archived file"):
                verify(self.fixture(root, duplicate=True), root)

    def test_missing_classpath_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, "Missing classpath directory"):
                verify(self.fixture(root, omit_class=True), root)

    def test_completion_requires_boolean_true(self):
        for completion in [False, 1]:
            with (
                self.subTest(completion=completion),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                index = self.fixture(
                    root,
                    change=lambda cert, completion=completion: cert.update(
                        complete=completion
                    ),
                )
                with self.assertRaisesRegex(ValueError, "completion"):
                    verify(index, root)

    def test_source_mapping_must_be_unambiguous(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            index_path = self.fixture(root)
            index = json.loads(index_path.read_text())
            index["source_roots"]["test.json"].append(
                {"source_prefix": "", "archive_prefix": "/tmp/"}
            )
            index_path.write_text(json.dumps(index))
            with self.assertRaisesRegex(ValueError, "exactly one archive mapping"):
                verify(index_path, root)

    def test_historical_grouped_classpaths_are_all_verified(self):
        def group(certificate):
            paths = certificate["classpaths"]
            certificate["classpaths"] = {
                "export": paths,
                "import": {"/tmp/dependency.jar": paths["/tmp/dependency.jar"]},
            }

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = verify(self.fixture(root, change=group), root)
            self.assertEqual(result["verified_classpath_hashes"], 3)

    def test_invalid_classpath_group_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            index = self.fixture(
                root, change=lambda cert: cert.update(classpaths={"export": 1})
            )
            with self.assertRaisesRegex(TypeError, "Invalid classpath hash or group"):
                verify(index, root)

    def test_declared_legacy_classpath_hash_is_verified(self):
        checksum = sha(b"A.class\0" + bytes.fromhex(sha(b"bytecode")))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = self.fixture(
                root,
                change=lambda cert: cert["classpaths"].update(
                    {"/tmp/classes": checksum}
                ),
            )
            index = json.loads(path.read_text())
            index["classpath_hash_algorithms"] = {
                "test.json": "path-null-binary-sha256"
            }
            path.write_text(json.dumps(index))
            self.assertEqual(verify(path, root)["verified_classpath_hashes"], 2)

    def test_wrong_and_unknown_classpath_algorithms_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = self.fixture(root)
            index = json.loads(path.read_text())
            for algorithm in ["path-null-binary-sha256", "unknown"]:
                index["classpath_hash_algorithms"] = {"test.json": algorithm}
                path.write_text(json.dumps(index))
                with self.assertRaises(ValueError):
                    verify(path, root)


if __name__ == "__main__":
    unittest.main()
