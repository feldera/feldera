"""Copy bundled data to a target directory for pipeline-manager.

When the precompile cache from the official Docker image is reused, the
crate sources must live at the same absolute path that was used when the
cache was built (``/home/ubuntu/feldera`` in the official image).  This
script copies the bundled data there so the cache stays valid.

Usage::

    python -m pyfeldera.setup_home --target /home/ubuntu/feldera
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from pyfeldera._paths import _data_root


def setup_home(target: str | Path) -> Path:
    """Copy pyfeldera's bundled data to *target*, returning the path.

    Existing files under *target* are overwritten.  The binary is made
    executable.
    """
    target = Path(target)
    src = _data_root()

    if not src.exists():
        raise FileNotFoundError(
            f"Data directory {src} not found. Build the wheel first."
        )

    # Remove existing target to ensure clean state
    if target.exists():
        shutil.rmtree(target)

    shutil.copytree(src, target, dirs_exist_ok=True)

    binary = target / "bin" / "pipeline-manager"
    if binary.exists():
        binary.chmod(binary.stat().st_mode | 0o111)

    return target


def main() -> None:
    parser = argparse.ArgumentParser(description="Set up Feldera home directory")
    parser.add_argument(
        "--target",
        required=True,
        help="Destination directory for Feldera data files",
    )
    args = parser.parse_args()
    path = setup_home(args.target)
    print(f"Feldera home set up at {path}")


if __name__ == "__main__":
    main()
