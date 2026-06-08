from __future__ import annotations

from unittest.mock import MagicMock, patch


from felderize.feldera_client import _compiler_cmd, validate_sql


class TestCompilerCmd:
    def test_jar_uses_java(self, tmp_path):
        jar = tmp_path / "sql2dbsp.jar"
        cmd = _compiler_cmd(jar)
        assert cmd == ["java", "-jar", str(jar)]

    def test_script_uses_direct(self, tmp_path):
        script = tmp_path / "sql-to-dbsp"
        cmd = _compiler_cmd(script)
        assert cmd == [str(script)]

    def test_other_extension_uses_direct(self, tmp_path):
        binary = tmp_path / "compiler.sh"
        cmd = _compiler_cmd(binary)
        assert cmd == [str(binary)]


class TestValidateSql:
    def test_no_compiler_returns_error(self):
        errors = validate_sql("SELECT 1", compiler_path=None)
        assert any("FELDERA_COMPILER" in e for e in errors)

    def test_missing_file_returns_error(self, tmp_path):
        errors = validate_sql("SELECT 1", compiler_path=tmp_path / "nonexistent")
        assert any("not found" in e for e in errors)

    def test_success_returns_empty(self, tmp_path):
        fake_compiler = tmp_path / "sql-to-dbsp"
        fake_compiler.write_text("#!/bin/sh\nexit 0\n")
        fake_compiler.chmod(0o755)

        errors = validate_sql("SELECT 1;", compiler_path=fake_compiler)
        assert errors == []

    def test_compiler_error_returned(self, tmp_path):
        fake_compiler = tmp_path / "sql-to-dbsp"
        fake_compiler.write_text("#!/bin/sh\necho 'error: bad sql' >&2\nexit 1\n")
        fake_compiler.chmod(0o755)

        errors = validate_sql("BAD SQL", compiler_path=fake_compiler)
        assert any("error" in e.lower() for e in errors)

    def test_timeout_returns_error(self, tmp_path):
        import subprocess

        fake_compiler = tmp_path / "sql-to-dbsp"
        fake_compiler.write_text("#!/bin/sh\nsleep 100\n")
        fake_compiler.chmod(0o755)

        with patch("felderize.feldera_client.subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired(cmd="x", timeout=60)
            errors = validate_sql("SELECT 1;", compiler_path=fake_compiler)
        assert any("timed out" in e.lower() for e in errors)

    def test_jar_missing_java_returns_helpful_error(self, tmp_path):
        jar = tmp_path / "sql2dbsp.jar"
        jar.write_bytes(b"fake jar")

        with patch("felderize.feldera_client.subprocess.run") as mock_run:
            mock_run.side_effect = FileNotFoundError
            errors = validate_sql("SELECT 1;", compiler_path=jar)
        assert any("java" in e.lower() for e in errors)

    def test_jar_invokes_java_dash_jar(self, tmp_path):
        jar = tmp_path / "sql2dbsp.jar"
        jar.write_bytes(b"fake jar")

        captured_cmd: list[list[str]] = []

        def fake_run(cmd, **kwargs):
            captured_cmd.append(cmd)
            result = MagicMock()
            result.returncode = 0
            result.stderr = ""
            return result

        with patch("felderize.feldera_client.subprocess.run", side_effect=fake_run):
            validate_sql("SELECT 1;", compiler_path=jar)

        assert captured_cmd[0][:3] == ["java", "-jar", str(jar)]
