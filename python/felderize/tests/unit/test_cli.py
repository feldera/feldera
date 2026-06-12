from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from click.testing import CliRunner

import pytest

from felderize.cli import _expand_query_paths, _read_text, _split_examples, cli
from felderize.models import Status, TranslationResult


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_SUCCESS = TranslationResult(
    feldera_schema="CREATE TABLE t (x INT NOT NULL);",
    feldera_query="CREATE VIEW v AS SELECT x FROM t;",
    status=Status.SUCCESS,
)

_UNSUPPORTED = TranslationResult(
    feldera_query="CREATE VIEW v AS SELECT x FROM t;",
    unsupported=["PIVOT is not supported"],
    status=Status.UNSUPPORTED,
)

_ERROR = TranslationResult(
    warnings=["Syntax error at line 1"],
    status=Status.ERROR,
)


def _schema(tmp_path: Path, content: str = "CREATE TABLE t (x INT NOT NULL);") -> Path:
    p = tmp_path / "schema.sql"
    p.write_text(content)
    return p


def _query(tmp_path: Path, content: str = "CREATE VIEW v AS SELECT x FROM t;") -> Path:
    p = tmp_path / "query.sql"
    p.write_text(content)
    return p


@pytest.fixture(autouse=True)
def _no_auto_download(monkeypatch):
    """Keep CLI tests hermetic: never reach out to GitHub to fetch a compiler.

    With no --compiler/FELDERA_COMPILER configured, --validate would otherwise
    auto-download the compiler; stub it to "not found" so tests stay offline.
    """
    monkeypatch.setattr("felderize.cli.ensure_compiler", lambda *a, **k: None)


# ---------------------------------------------------------------------------
# _split_examples
# ---------------------------------------------------------------------------


class TestSplitExamples:
    def test_empty_tuple(self):
        dirs, files = _split_examples(())
        assert dirs == [] and files == []

    def test_directory_goes_to_dirs(self, tmp_path):
        dirs, files = _split_examples((str(tmp_path),))
        assert dirs == [tmp_path] and files == []

    def test_file_goes_to_files(self, tmp_path):
        f = tmp_path / "ex.md"
        f.write_text("# example")
        dirs, files = _split_examples((str(f),))
        assert dirs == [] and files == [f]

    def test_mixed_paths(self, tmp_path):
        sub = tmp_path / "subdir"
        sub.mkdir()
        f = tmp_path / "ex.md"
        f.write_text("# example")
        dirs, files = _split_examples((str(sub), str(f)))
        assert dirs == [sub] and files == [f]


# ---------------------------------------------------------------------------
# _expand_query_paths
# ---------------------------------------------------------------------------


class TestExpandQueryPaths:
    def test_single_file_returned_as_is(self, tmp_path):
        f = tmp_path / "q.sql"
        f.write_text("SELECT 1")
        assert _expand_query_paths((f,)) == [f]

    def test_directory_expands_to_sorted_sql_files(self, tmp_path):
        (tmp_path / "b.sql").write_text("SELECT 2")
        (tmp_path / "a.sql").write_text("SELECT 1")
        (tmp_path / "skip.txt").write_text("ignored")
        result = _expand_query_paths((tmp_path,))
        assert result == [tmp_path / "a.sql", tmp_path / "b.sql"]

    def test_empty_directory_returns_empty_list(self, tmp_path):
        assert _expand_query_paths((tmp_path,)) == []

    def test_mixed_file_and_directory(self, tmp_path):
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "c.sql").write_text("SELECT 3")
        lone = tmp_path / "lone.sql"
        lone.write_text("SELECT 0")
        result = _expand_query_paths((lone, sub))
        assert result == [lone, sub / "c.sql"]

    def test_non_sql_files_in_dir_excluded(self, tmp_path):
        (tmp_path / "query.sql").write_text("SELECT 1")
        (tmp_path / "notes.md").write_text("ignore me")
        result = _expand_query_paths((tmp_path,))
        assert all(p.suffix == ".sql" for p in result)


# ---------------------------------------------------------------------------
# download-compiler command
# ---------------------------------------------------------------------------


class TestDownloadCompilerCommand:
    def test_success_prints_path_and_env_hint(self, tmp_path):
        fake_jar = tmp_path / "sql2dbsp.jar"
        runner = CliRunner()
        with patch("felderize.cli.download_compiler", return_value=fake_jar):
            result = runner.invoke(cli, ["download-compiler"])
        assert result.exit_code == 0
        assert str(fake_jar) in result.output
        assert "FELDERA_COMPILER" in result.output

    def test_exception_exits_nonzero(self):
        runner = CliRunner()
        with patch(
            "felderize.cli.download_compiler", side_effect=RuntimeError("no network")
        ):
            result = runner.invoke(cli, ["download-compiler"])
        assert result.exit_code == 1
        assert "no network" in result.output

    def test_version_flag_forwarded(self, tmp_path):
        fake_jar = tmp_path / "sql2dbsp.jar"
        runner = CliRunner()
        with patch("felderize.cli.download_compiler", return_value=fake_jar) as mock_dl:
            runner.invoke(cli, ["download-compiler", "--version", "v0.291.0"])
        mock_dl.assert_called_once_with(
            output_dir=None, version="v0.291.0", force=False
        )


# ---------------------------------------------------------------------------
# spark translate
# ---------------------------------------------------------------------------


class TestTranslateCommand:
    _BASE = ["spark", "translate"]

    def _invoke(self, runner: CliRunner, schema: Path, query: Path, *extra: str):
        return runner.invoke(cli, self._BASE + [str(schema), str(query)] + list(extra))

    def test_empty_schema_exits_with_error(self, tmp_path):
        schema = _schema(tmp_path, "")
        query = _query(tmp_path)
        result = CliRunner().invoke(cli, self._BASE + [str(schema), str(query)])
        assert result.exit_code == 1
        assert "Error" in result.output

    def test_empty_query_exits_with_error(self, tmp_path):
        schema = _schema(tmp_path)
        query = _query(tmp_path, "")
        result = CliRunner().invoke(cli, self._BASE + [str(schema), str(query)])
        assert result.exit_code == 1

    def test_schema_without_create_table_exits(self, tmp_path):
        schema = _schema(tmp_path, "SELECT 1;")
        query = _query(tmp_path)
        result = CliRunner().invoke(cli, self._BASE + [str(schema), str(query)])
        assert result.exit_code == 1

    def test_binary_schema_file_errors_cleanly(self, tmp_path):
        """A non-UTF-8 (binary) input is rejected with a message, not a traceback."""
        schema = tmp_path / "schema.sql"
        schema.write_bytes(b"\x88\x00\xff binary not text")
        query = _query(tmp_path)
        result = CliRunner().invoke(cli, self._BASE + [str(schema), str(query)])
        assert result.exit_code == 1
        assert "cannot read" in result.output and "not binary" in result.output
        assert "Traceback" not in result.output

    def test_success_text_output(self, tmp_path):
        runner = CliRunner()
        with patch("felderize.cli.translate_spark_to_feldera", return_value=_SUCCESS):
            result = self._invoke(
                runner, _schema(tmp_path), _query(tmp_path), "--validate"
            )
        assert result.exit_code == 0
        assert "-- Schema --" in result.output
        assert "-- Query --" in result.output

    def test_validate_auto_resolves_compiler(self, tmp_path, monkeypatch):
        """With --validate and no --compiler, the auto-resolved JAR is passed
        through to the translator via the config."""
        fake_jar = tmp_path / "sql2dbsp-jar-with-dependencies-v0.310.0.jar"
        monkeypatch.setattr("felderize.cli.ensure_compiler", lambda *a, **k: fake_jar)
        runner = CliRunner()
        with patch(
            "felderize.cli.translate_spark_to_feldera", return_value=_SUCCESS
        ) as mock_fn:
            self._invoke(runner, _schema(tmp_path), _query(tmp_path), "--validate")
        config = mock_fn.call_args.args[2]
        assert config.feldera_compiler == str(fake_jar)

    def test_explicit_compiler_skips_auto_resolution(self, tmp_path, monkeypatch):
        """An explicit --compiler must win; auto-download is never consulted."""
        called = []
        monkeypatch.setattr(
            "felderize.cli.ensure_compiler",
            lambda *a, **k: called.append(True),
        )
        runner = CliRunner()
        with patch("felderize.cli.translate_spark_to_feldera", return_value=_SUCCESS):
            self._invoke(
                runner,
                _schema(tmp_path),
                _query(tmp_path),
                "--validate",
                "--compiler",
                "/opt/my-compiler.jar",
            )
        assert called == []

    def test_env_compiler_skips_auto_resolution(self, tmp_path, monkeypatch):
        """A configured FELDERA_COMPILER must be used as-is; no auto-download."""
        called = []
        monkeypatch.setattr(
            "felderize.cli.ensure_compiler",
            lambda *a, **k: called.append(True),
        )
        monkeypatch.setenv("FELDERA_COMPILER", "/opt/env-compiler.jar")
        runner = CliRunner()
        with patch(
            "felderize.cli.translate_spark_to_feldera", return_value=_SUCCESS
        ) as mock_fn:
            self._invoke(runner, _schema(tmp_path), _query(tmp_path), "--validate")
        assert called == []
        config = mock_fn.call_args.args[2]
        assert config.feldera_compiler == "/opt/env-compiler.jar"

    def test_success_json_output(self, tmp_path):
        runner = CliRunner()
        with patch("felderize.cli.translate_spark_to_feldera", return_value=_SUCCESS):
            result = self._invoke(
                runner,
                _schema(tmp_path),
                _query(tmp_path),
                "--validate",
                "--json-output",
            )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["status"] == "success"
        assert "feldera_query" in data
        assert "feldera_schema" in data

    def test_output_writes_sql_file(self, tmp_path):
        runner = CliRunner()
        out = tmp_path / "out.sql"
        with patch("felderize.cli.translate_spark_to_feldera", return_value=_SUCCESS):
            result = self._invoke(
                runner,
                _schema(tmp_path),
                _query(tmp_path),
                "--validate",
                "-o",
                str(out),
            )
        assert result.exit_code == 0
        text = out.read_text()
        assert "CREATE TABLE t" in text and "CREATE VIEW v" in text
        assert f"Wrote {out}" in result.output  # status note (stderr)
        # the human section-by-section print is skipped when writing a file
        assert "Transformations" not in result.output

    def test_output_file_includes_caveats_as_comments(self, tmp_path):
        runner = CliRunner()
        out = tmp_path / "out.sql"
        with patch(
            "felderize.cli.translate_spark_to_feldera", return_value=_UNSUPPORTED
        ):
            result = self._invoke(
                runner, _schema(tmp_path), _query(tmp_path), "-o", str(out)
            )
        assert result.exit_code == 0
        text = out.read_text()
        assert "-- Translated by felderize — status: unsupported" in text
        assert "-- Unsupported" in text
        assert "PIVOT is not supported" in text
        # every header note line is a SQL comment (no leaked uncommented text)
        for line in text.splitlines():
            if line.strip() and not line.startswith("--"):
                assert line.lstrip().upper().startswith("CREATE"), line

    def test_output_with_json_writes_file_and_prints_json(self, tmp_path):
        runner = CliRunner()
        out = tmp_path / "out.sql"
        with patch("felderize.cli.translate_spark_to_feldera", return_value=_SUCCESS):
            result = self._invoke(
                runner,
                _schema(tmp_path),
                _query(tmp_path),
                "-o",
                str(out),
                "--json-output",
            )
        assert result.exit_code == 0
        assert out.is_file()  # the .sql file was written
        assert '"status": "success"' in result.output  # and JSON still printed

    def test_no_docs_flag_forwarded(self, tmp_path):
        runner = CliRunner()
        with patch(
            "felderize.cli.translate_spark_to_feldera", return_value=_SUCCESS
        ) as mock_fn:
            self._invoke(
                runner, _schema(tmp_path), _query(tmp_path), "--validate", "--no-docs"
            )
        assert mock_fn.call_args.kwargs["include_docs"] is False

    def test_without_validate_prints_warning(self, tmp_path):
        runner = CliRunner()
        with patch("felderize.cli.translate_spark_to_feldera", return_value=_SUCCESS):
            result = self._invoke(runner, _schema(tmp_path), _query(tmp_path))
        assert "without validation" in result.output

    def test_error_result_exits_zero_but_reports_failure(self, tmp_path):
        runner = CliRunner()
        with patch("felderize.cli.translate_spark_to_feldera", return_value=_ERROR):
            result = self._invoke(
                runner, _schema(tmp_path), _query(tmp_path), "--validate"
            )
        assert result.exit_code == 0
        assert "Translation Failed" in result.output

    def test_unsupported_result_shows_unsupported_section(self, tmp_path):
        runner = CliRunner()
        with patch(
            "felderize.cli.translate_spark_to_feldera", return_value=_UNSUPPORTED
        ):
            result = self._invoke(
                runner, _schema(tmp_path), _query(tmp_path), "--validate"
            )
        assert result.exit_code == 0
        assert "Unsupported" in result.output
        assert "PIVOT" in result.output


# ---------------------------------------------------------------------------
# spark translate-file
# ---------------------------------------------------------------------------


class TestReadText:
    def test_reads_utf8_file(self, tmp_path):
        p = tmp_path / "x.sql"
        p.write_text("CREATE TABLE t (x INT);")
        assert _read_text(p) == "CREATE TABLE t (x INT);"

    def test_binary_file_exits_cleanly(self, tmp_path):
        p = tmp_path / "x.bin"
        p.write_bytes(b"\x88\x00\xff not text")
        with pytest.raises(SystemExit) as exc:
            _read_text(p)
        assert exc.value.code == 1

    def test_missing_file_exits_cleanly(self, tmp_path):
        with pytest.raises(SystemExit) as exc:
            _read_text(tmp_path / "does_not_exist.sql")
        assert exc.value.code == 1


class TestTranslateFileCommand:
    _BASE = ["spark", "translate-file"]

    def test_binary_file_errors_cleanly(self, tmp_path):
        combined = tmp_path / "combined.sql"
        combined.write_bytes(b"\x88\xff binary")
        result = CliRunner().invoke(cli, self._BASE + [str(combined)])
        assert result.exit_code == 1
        assert "cannot read" in result.output
        assert "Traceback" not in result.output

    def test_empty_file_exits_with_error(self, tmp_path):
        combined = tmp_path / "combined.sql"
        combined.write_text("")
        result = CliRunner().invoke(cli, self._BASE + [str(combined)])
        assert result.exit_code == 1

    def test_success_with_combined_sql(self, tmp_path):
        combined = tmp_path / "combined.sql"
        combined.write_text(
            "CREATE TABLE t (x INT NOT NULL);\nCREATE VIEW v AS SELECT x FROM t;\n"
        )
        runner = CliRunner()
        with patch("felderize.cli.translate_spark_to_feldera", return_value=_SUCCESS):
            result = runner.invoke(cli, self._BASE + [str(combined), "--validate"])
        assert result.exit_code == 0
        assert "-- Query --" in result.output

    def test_json_output_flag(self, tmp_path):
        combined = tmp_path / "combined.sql"
        combined.write_text(
            "CREATE TABLE t (x INT NOT NULL);\nCREATE VIEW v AS SELECT x FROM t;\n"
        )
        runner = CliRunner()
        with patch("felderize.cli.translate_spark_to_feldera", return_value=_SUCCESS):
            result = runner.invoke(
                cli, self._BASE + [str(combined), "--validate", "--json-output"]
            )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["status"] == "success"


# ---------------------------------------------------------------------------
# spark translate-batch
# ---------------------------------------------------------------------------


class TestTranslateBatchCommand:
    _BASE = ["spark", "translate-batch"]

    def _invoke(self, runner: CliRunner, schema: Path, *extra: str):
        return runner.invoke(cli, self._BASE + [str(schema)] + list(extra))

    def test_invalid_schema_exits_before_any_translation(self, tmp_path):
        schema = _schema(tmp_path, "")
        query = _query(tmp_path)
        runner = CliRunner()
        with patch("felderize.cli.translate_spark_to_feldera") as mock_fn:
            result = self._invoke(runner, schema, str(query))
        assert result.exit_code == 1
        mock_fn.assert_not_called()

    def test_directory_with_no_sql_files_exits(self, tmp_path):
        schema = _schema(tmp_path)
        query_dir = tmp_path / "queries"
        query_dir.mkdir()
        result = CliRunner().invoke(cli, self._BASE + [str(schema), str(query_dir)])
        assert result.exit_code == 1
        assert "no .sql" in result.output.lower()

    def test_translates_multiple_explicit_files(self, tmp_path):
        schema = _schema(tmp_path)
        q1 = tmp_path / "q1.sql"
        q1.write_text("CREATE VIEW v1 AS SELECT x FROM t;")
        q2 = tmp_path / "q2.sql"
        q2.write_text("CREATE VIEW v2 AS SELECT x FROM t;")
        runner = CliRunner()
        with patch(
            "felderize.cli.translate_spark_to_feldera", return_value=_SUCCESS
        ) as mock_fn:
            result = self._invoke(runner, schema, str(q1), str(q2))
        assert result.exit_code == 0
        assert mock_fn.call_count == 2

    def test_binary_query_is_skipped_not_crashed(self, tmp_path):
        """A binary query file is reported and skipped; good queries still run."""
        schema = _schema(tmp_path)
        good = tmp_path / "good.sql"
        good.write_text("CREATE VIEW v AS SELECT x FROM t;")
        bad = tmp_path / "bad.sql"
        bad.write_bytes(b"\x88\xff not text")
        runner = CliRunner()
        with patch(
            "felderize.cli.translate_spark_to_feldera", return_value=_SUCCESS
        ) as mock_fn:
            result = self._invoke(runner, schema, str(good), str(bad))
        assert "cannot read" in result.output
        assert "Traceback" not in result.output
        assert mock_fn.call_count == 1  # only the good query was translated

    def test_expands_directory_of_sql_files(self, tmp_path):
        schema = _schema(tmp_path)
        query_dir = tmp_path / "queries"
        query_dir.mkdir()
        (query_dir / "a.sql").write_text("CREATE VIEW a AS SELECT x FROM t;")
        (query_dir / "b.sql").write_text("CREATE VIEW b AS SELECT x FROM t;")
        runner = CliRunner()
        with patch(
            "felderize.cli.translate_spark_to_feldera", return_value=_SUCCESS
        ) as mock_fn:
            result = self._invoke(runner, schema, str(query_dir))
        assert result.exit_code == 0
        assert mock_fn.call_count == 2

    def test_output_dir_creates_one_file_per_query(self, tmp_path):
        schema = _schema(tmp_path)
        q1 = tmp_path / "q1.sql"
        q1.write_text("CREATE VIEW v AS SELECT x FROM t;")
        out = tmp_path / "out"
        runner = CliRunner()
        with patch("felderize.cli.translate_spark_to_feldera", return_value=_SUCCESS):
            result = self._invoke(runner, schema, str(q1), "--output-dir", str(out))
        assert result.exit_code == 0
        assert (out / "q1_feldera.sql").is_file()
        content = (out / "q1_feldera.sql").read_text()
        assert "Schema" in content
        assert "Query" in content

    def test_output_dir_created_if_missing(self, tmp_path):
        schema = _schema(tmp_path)
        q1 = tmp_path / "q1.sql"
        q1.write_text("CREATE VIEW v AS SELECT x FROM t;")
        out = tmp_path / "nested" / "out"
        runner = CliRunner()
        with patch("felderize.cli.translate_spark_to_feldera", return_value=_SUCCESS):
            result = self._invoke(runner, schema, str(q1), "--output-dir", str(out))
        assert result.exit_code == 0
        assert out.is_dir()

    def test_bad_query_skipped_loop_continues(self, tmp_path):
        schema = _schema(tmp_path)
        bad = tmp_path / "bad.sql"
        bad.write_text("")  # empty → validate_query fails
        good = tmp_path / "good.sql"
        good.write_text("CREATE VIEW v AS SELECT x FROM t;")
        runner = CliRunner()
        with patch(
            "felderize.cli.translate_spark_to_feldera", return_value=_SUCCESS
        ) as mock_fn:
            result = self._invoke(runner, schema, str(bad), str(good))
        assert result.exit_code == 0
        mock_fn.assert_called_once()  # only the good query reaches translation

    def test_summary_line_shows_counts(self, tmp_path):
        schema = _schema(tmp_path)
        q1 = tmp_path / "q1.sql"
        q1.write_text("CREATE VIEW v AS SELECT x FROM t;")
        runner = CliRunner()
        with patch("felderize.cli.translate_spark_to_feldera", return_value=_SUCCESS):
            result = self._invoke(runner, schema, str(q1))
        assert "1/1" in result.output


# ---------------------------------------------------------------------------
# spark example
# ---------------------------------------------------------------------------


class TestExampleCommand:
    _BASE = ["spark", "example"]

    def test_no_name_lists_examples(self):
        result = CliRunner().invoke(cli, self._BASE)
        assert result.exit_code == 0
        assert "simple" in result.output

    def test_no_name_shows_schema_query_and_combined_tags(self):
        result = CliRunner().invoke(cli, self._BASE)
        assert "[schema+query]" in result.output or "[combined]" in result.output

    def test_unknown_name_exits_nonzero(self):
        result = CliRunner().invoke(cli, self._BASE + ["__no_such_example__"])
        assert result.exit_code == 1

    def test_schema_query_pair_example(self):
        runner = CliRunner()
        with patch("felderize.cli.translate_spark_to_feldera", return_value=_SUCCESS):
            result = runner.invoke(cli, self._BASE + ["simple", "--validate"])
        assert result.exit_code == 0

    def test_combined_file_example(self):
        runner = CliRunner()
        with patch("felderize.cli.translate_spark_to_feldera", return_value=_SUCCESS):
            result = runner.invoke(cli, self._BASE + ["arithmetic", "--validate"])
        assert result.exit_code == 0

    def test_json_output_for_example(self):
        runner = CliRunner()
        with patch("felderize.cli.translate_spark_to_feldera", return_value=_SUCCESS):
            result = runner.invoke(
                cli, self._BASE + ["simple", "--validate", "--json-output"]
            )
        assert result.exit_code == 0
        # The example command echoes the Spark SQL to stderr before the JSON, so
        # result.output contains the preamble followed by the JSON object.
        json_start = result.output.index("{")
        data = json.loads(result.output[json_start:])
        assert data["status"] == "success"
