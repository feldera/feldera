from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path

def validate_sql(sql: str, compiler_path: str | Path | None = None) -> list[str]:
    """Validate SQL using the local sql-to-dbsp compiler. Returns compiler errors."""
    if not compiler_path:
        return ["Compiler not found: set FELDERA_COMPILER in .env or use --compiler"]
    compiler = Path(compiler_path)

    if not compiler.is_file():
        return [f"Compiler not found at {compiler}"]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=True) as f:
        f.write(sql)
        f.flush()

        try:
            result = subprocess.run(
                [
                    str(compiler),
                    "-i",
                    "--ignoreOrder",
                    "--alltables",
                    "--noRust",
                    f.name,
                ],
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

    return (
        errors if errors else [f"Compilation failed with exit code {result.returncode}"]
    )
