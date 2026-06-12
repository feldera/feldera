from __future__ import annotations

import pytest

from felderize.config import Config


class TestAutoDownloadCompilerFlag:
    def test_defaults_to_enabled(self, monkeypatch):
        monkeypatch.delenv("FELDERIZE_AUTO_DOWNLOAD", raising=False)
        assert Config.from_env().auto_download_compiler is True

    @pytest.mark.parametrize("value", ["0", "false", "FALSE", "no", "off", "Off"])
    def test_disabled_values(self, monkeypatch, value):
        monkeypatch.setenv("FELDERIZE_AUTO_DOWNLOAD", value)
        assert Config.from_env().auto_download_compiler is False

    @pytest.mark.parametrize("value", ["1", "true", "yes", "on", "anything-else"])
    def test_enabled_values(self, monkeypatch, value):
        monkeypatch.setenv("FELDERIZE_AUTO_DOWNLOAD", value)
        assert Config.from_env().auto_download_compiler is True
