from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from felderize.install_feldera_sql_compiler import (
    _find_jar_asset,
    _parse_version,
    download_compiler,
    ensure_compiler,
    find_local_compiler,
    is_supported_version,
    jar_version,
)


def _make_jar(directory: Path, version: str) -> Path:
    """Create a fake compiler JAR named for the given version tag."""
    jar = directory / f"sql2dbsp-jar-with-dependencies-{version}.jar"
    jar.write_bytes(b"fake jar content")
    return jar


_FAKE_RELEASE = {
    "tag_name": "v0.291.0",
    "assets": [
        {
            "name": "fda-x86_64-unknown-linux-gnu.zip",
            "browser_download_url": "https://example.com/fda.zip",
        },
        {
            "name": "sql2dbsp-jar-with-dependencies-v0.291.0.jar",
            "browser_download_url": "https://example.com/sql2dbsp.jar",
        },
    ],
}

_RELEASE_NO_JAR = {
    "tag_name": "v0.0.0",
    "assets": [
        {
            "name": "fda-x86_64-unknown-linux-gnu.zip",
            "browser_download_url": "https://example.com/fda.zip",
        },
    ],
}


class TestFindJarAsset:
    def test_finds_jar(self):
        name, url = _find_jar_asset(_FAKE_RELEASE)
        assert name == "sql2dbsp-jar-with-dependencies-v0.291.0.jar"
        assert "sql2dbsp" in url

    def test_missing_jar_raises(self):
        with pytest.raises(RuntimeError, match="No compiler JAR"):
            _find_jar_asset(_RELEASE_NO_JAR)


class TestDownloadCompiler:
    def _mock_fetch(self, release: dict):
        return patch(
            "felderize.install_feldera_sql_compiler._fetch_release",
            return_value=release,
        )

    def _mock_urlretrieve(self, dest_path: Path):
        def fake_retrieve(url, dest, reporthook=None):
            Path(dest).write_bytes(b"fake jar content")

        return patch(
            "felderize.install_feldera_sql_compiler.urllib.request.urlretrieve",
            side_effect=fake_retrieve,
        )

    def test_downloads_jar(self, tmp_path):
        with self._mock_fetch(_FAKE_RELEASE), self._mock_urlretrieve(tmp_path):
            result = download_compiler(output_dir=tmp_path)

        assert result.name == "sql2dbsp-jar-with-dependencies-v0.291.0.jar"
        assert result.exists()

    def test_skips_existing_without_force(self, tmp_path):
        existing = tmp_path / "sql2dbsp-jar-with-dependencies-v0.291.0.jar"
        existing.write_bytes(b"original")

        with self._mock_fetch(_FAKE_RELEASE):
            with patch(
                "felderize.install_feldera_sql_compiler.urllib.request.urlretrieve"
            ) as mock_dl:
                result = download_compiler(output_dir=tmp_path, force=False)
                mock_dl.assert_not_called()

        assert result == existing
        assert existing.read_bytes() == b"original"

    def test_force_redownloads(self, tmp_path):
        existing = tmp_path / "sql2dbsp-jar-with-dependencies-v0.291.0.jar"
        existing.write_bytes(b"original")

        with self._mock_fetch(_FAKE_RELEASE), self._mock_urlretrieve(tmp_path):
            result = download_compiler(output_dir=tmp_path, force=True)

        assert result.read_bytes() == b"fake jar content"

    def test_creates_output_dir(self, tmp_path):
        new_dir = tmp_path / "subdir" / "nested"
        with self._mock_fetch(_FAKE_RELEASE), self._mock_urlretrieve(new_dir):
            result = download_compiler(output_dir=new_dir)

        assert new_dir.is_dir()
        assert result.parent == new_dir

    def test_no_jar_in_release_raises(self, tmp_path):
        with self._mock_fetch(_RELEASE_NO_JAR):
            with pytest.raises(RuntimeError, match="No compiler JAR"):
                download_compiler(output_dir=tmp_path)

    def test_version_forwarded_to_fetch(self, tmp_path):
        with patch(
            "felderize.install_feldera_sql_compiler._fetch_release",
            return_value=_FAKE_RELEASE,
        ) as mock_fetch:
            with self._mock_urlretrieve(tmp_path):
                download_compiler(output_dir=tmp_path, version="v0.291.0")
        mock_fetch.assert_called_once_with("v0.291.0")


class TestFindLocalCompiler:
    def test_none_when_dir_missing(self, tmp_path):
        assert find_local_compiler(tmp_path / "does-not-exist") is None

    def test_none_when_no_jars(self, tmp_path):
        (tmp_path / "notes.txt").write_text("not a jar")
        assert find_local_compiler(tmp_path) is None

    def test_picks_highest_supported_version(self, tmp_path):
        _make_jar(tmp_path, "v0.304.0")
        newest = _make_jar(tmp_path, "v0.310.0")
        _make_jar(tmp_path, "v0.305.0")
        assert find_local_compiler(tmp_path) == newest

    def test_prefers_supported_over_higher_unsupported(self, tmp_path):
        # An unsupported version must not win even when numerically higher would
        # never happen; here the supported one is also the only acceptable choice.
        supported = _make_jar(tmp_path, "v0.304.0")
        _make_jar(tmp_path, "v0.300.0")  # below MINIMUM_COMPILER_VERSION
        assert find_local_compiler(tmp_path) == supported

    def test_falls_back_to_unsupported_when_no_supported(self, tmp_path):
        _make_jar(tmp_path, "v0.300.0")
        newest_unsupported = _make_jar(tmp_path, "v0.303.0")
        assert find_local_compiler(tmp_path) == newest_unsupported

    def test_ignores_unrelated_jars(self, tmp_path):
        (tmp_path / "some-other.jar").write_bytes(b"x")
        wanted = _make_jar(tmp_path, "v0.304.0")
        assert find_local_compiler(tmp_path) == wanted


class TestEnsureCompiler:
    def test_returns_cached_without_downloading(self, tmp_path):
        cached = _make_jar(tmp_path, "v0.304.0")
        with patch(
            "felderize.install_feldera_sql_compiler.download_compiler"
        ) as mock_dl:
            result = ensure_compiler(search_dir=tmp_path)
        assert result == cached
        mock_dl.assert_not_called()

    def test_downloads_when_none_cached(self, tmp_path):
        def fake_retrieve(url, dest, reporthook=None):
            Path(dest).write_bytes(b"fake jar content")

        with (
            patch(
                "felderize.install_feldera_sql_compiler._fetch_release",
                return_value=_FAKE_RELEASE,
            ),
            patch(
                "felderize.install_feldera_sql_compiler.urllib.request.urlretrieve",
                side_effect=fake_retrieve,
            ),
        ):
            result = ensure_compiler(search_dir=tmp_path)
        assert result is not None
        assert result.exists()
        assert result.name == "sql2dbsp-jar-with-dependencies-v0.291.0.jar"

    def test_none_when_download_disabled_and_no_cache(self, tmp_path):
        assert ensure_compiler(search_dir=tmp_path, auto_download=False) is None

    def test_returns_none_and_warns_on_download_failure(self, tmp_path, capsys):
        with patch(
            "felderize.install_feldera_sql_compiler.download_compiler",
            side_effect=RuntimeError("network down"),
        ):
            result = ensure_compiler(search_dir=tmp_path)
        assert result is None
        assert "could not auto-download" in capsys.readouterr().err


class TestVersionHelpers:
    def test_parse_version(self):
        assert _parse_version("v0.304.0") == (0, 304, 0)
        assert _parse_version("v1.2.3") == (1, 2, 3)
        assert _parse_version("nonsense") == ()

    @pytest.mark.parametrize(
        "tag, supported",
        [
            ("v0.304.0", True),  # exactly the minimum
            ("v0.305.0", True),  # newer patch line
            ("v0.350.0", True),  # newer minor
            ("v1.0.0", True),  # newer major
            ("v0.303.0", False),  # one below minimum
            ("v0.297.0", False),  # well below
        ],
    )
    def test_is_supported_version(self, tag, supported):
        assert is_supported_version(tag) is supported

    def test_jar_version_extracts_tag(self):
        assert jar_version("sql2dbsp-jar-with-dependencies-v0.304.0.jar") == "v0.304.0"
        assert jar_version("/home/x/.felderize/foo-v1.2.3.jar") == "v1.2.3"

    def test_jar_version_missing(self):
        assert jar_version("some-random.jar") is None
