from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path

from felderize.constants import COMPILER_TIMEOUT


def _compiler_cmd(compiler: Path) -> list[str]:
    """Return the command prefix for invoking the compiler."""
    if compiler.suffix == ".jar":
        return ["java", "-jar", str(compiler)]
    return [str(compiler)]


def validate_sql(sql: str, compiler_path: str | Path | None = None) -> list[str]:
    """Validate SQL using the local sql-to-dbsp compiler. Returns compiler errors."""
    if not compiler_path:
        return ["Compiler not found: set FELDERA_COMPILER in .env or use --compiler"]
    compiler = Path(compiler_path)

    if not compiler.is_file():
        return [f"Compiler not found at {compiler}"]

    cmd_prefix = _compiler_cmd(compiler)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql") as f:
        f.write(sql)
        f.flush()

        try:
            result = subprocess.run(
                cmd_prefix + ["-i", "--ignoreOrder", "--alltables", "--noRust", f.name],
                capture_output=True,
                text=True,
                timeout=COMPILER_TIMEOUT,
            )
        except subprocess.TimeoutExpired:
            return [f"Compilation timed out after {COMPILER_TIMEOUT}s"]
        except FileNotFoundError:
            if compiler.suffix == ".jar":
                return ["java not found — install Java 19+ to use the compiler JAR"]
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

    return (
        errors if errors else [f"Compilation failed with exit code {result.returncode}"]
    )
