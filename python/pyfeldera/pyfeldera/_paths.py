"""Locate bundled artifacts inside the installed pyfeldera package."""

from __future__ import annotations

import os
from pathlib import Path


def _data_root() -> Path:
    """Return the root of the bundled data directory.

    Layout inside ``data/``::

        bin/pipeline-manager
        build/sql2dbsp-jar-with-dependencies.jar
        Cargo.toml
        Cargo.lock
        README.md
        crates/
        sql-to-dbsp-compiler/
    """
    return Path(__file__).parent / "data"


def pipeline_manager_bin() -> Path:
    """Absolute path to the bundled ``pipeline-manager`` binary."""
    return _data_root() / "bin" / "pipeline-manager"


def sql_compiler_jar() -> Path:
    """Absolute path to the bundled SQL compiler JAR."""
    return _data_root() / "build" / "sql2dbsp-jar-with-dependencies.jar"


def cargo_lock() -> Path:
    """Absolute path to the bundled ``Cargo.lock``."""
    return _data_root() / "Cargo.lock"


def dbsp_override_path() -> Path:
    """Path usable as ``--dbsp-override-path`` for the pipeline-manager.

    This is the root that contains ``Cargo.toml``, ``crates/``, etc.
    """
    return _data_root()


def validate() -> None:
    """Raise ``FileNotFoundError`` if required artifacts are missing."""
    for p, label in [
        (pipeline_manager_bin(), "pipeline-manager binary"),
        (sql_compiler_jar(), "SQL compiler JAR"),
        (cargo_lock(), "Cargo.lock"),
        (_data_root() / "Cargo.toml", "workspace Cargo.toml"),
        (_data_root() / "crates", "crates directory"),
    ]:
        if not p.exists():
            raise FileNotFoundError(
                f"Missing {label} at {p}. "
                "Run '.scripts/run.sh build-wheel' to populate data/."
            )

    binary = pipeline_manager_bin()
    if not os.access(binary, os.X_OK):
        binary.chmod(binary.stat().st_mode | 0o111)
