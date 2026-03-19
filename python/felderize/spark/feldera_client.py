from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
_DEFAULT_COMPILER = _REPO_ROOT / "sql-to-dbsp-compiler" / "SQL-compiler" / "sql-to-dbsp"


def validate_sql(sql: str, compiler_path: str | Path | None = None) -> list[str]:
    """Validate SQL using the local sql-to-dbsp compiler. Returns compiler errors."""
    compiler = Path(compiler_path) if compiler_path else _DEFAULT_COMPILER

    if not compiler.is_file():
        return [f"Compiler not found at {compiler}"]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=True) as f:
        f.write(sql)
        f.flush()

        try:
            result = subprocess.run(
                [str(compiler), "-i", "--ignoreOrder", "--alltables", "--noRust", f.name],
                capture_output=True,
                text=True,
                timeout=60,
            )
        except subprocess.TimeoutExpired:
            return ["Compilation timed out after 60s"]
        except FileNotFoundError:
            return [f"Compiler not found at {compiler}"]

    if result.returncode == 0:
        return []

    # Parse errors from stderr, stripping temp file paths for readability
    stderr = result.stderr.replace(f.name, "<input>")
    errors: list[str] = []
    for line in stderr.strip().split("\n"):
        line = line.strip()
        if line.startswith("<input>:") or "error:" in line.lower():
            errors.append(line)

    if not errors and stderr.strip():
        errors.append(stderr.strip())

    return errors if errors else [f"Compilation failed with exit code {result.returncode}"]
