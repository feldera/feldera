"""Tests for LongOperationWarning, which suppresses spam in polling loops."""

import logging
from unittest import mock

from feldera._long_operation_warning import LongOperationWarning


def _make(monotonic, **kwargs):
    """Build a LongOperationWarning with `time.monotonic` patched to `monotonic`."""
    with mock.patch(
        "feldera._long_operation_warning.time.monotonic", side_effect=monotonic
    ):
        return LongOperationWarning(
            logging.getLogger("test"),
            lambda elapsed: f"waiting, elapsed={elapsed}",
            lambda elapsed: f"done, elapsed={elapsed}",
            **kwargs,
        )


def _check_at(long_op, t):
    with mock.patch("feldera._long_operation_warning.time.monotonic", return_value=t):
        long_op.check()


def _done_at(long_op, t):
    with mock.patch("feldera._long_operation_warning.time.monotonic", return_value=t):
        long_op.done()


class TestLongOperationWarning:
    def test_no_warning_before_threshold(self, caplog):
        long_op = _make([0.0], warn_threshold_s=5.0, level=logging.WARNING)
        with caplog.at_level(logging.WARNING):
            _check_at(long_op, 4.9)
        assert caplog.records == []

    def test_warning_after_threshold(self, caplog):
        long_op = _make([0.0], warn_threshold_s=5.0, level=logging.WARNING)
        with caplog.at_level(logging.WARNING):
            _check_at(long_op, 5.0)
        assert len(caplog.records) == 1
        assert "waiting, elapsed=5.0" in caplog.records[0].message

    def test_threshold_doubles_after_each_warning(self, caplog):
        # Without doubling, every one-second tick after 5s would log again;
        # this is exactly the spam LongOperationWarning exists to prevent.
        long_op = _make([0.0], warn_threshold_s=5.0, level=logging.WARNING)
        with caplog.at_level(logging.WARNING):
            _check_at(long_op, 5.0)  # 1st warning, threshold -> 10
            _check_at(long_op, 9.0)  # below new threshold, no warning
            _check_at(long_op, 10.0)  # 2nd warning, threshold -> 20
        assert len(caplog.records) == 2

    def test_done_is_noop_if_never_warned(self, caplog):
        long_op = _make([0.0], warn_threshold_s=5.0, level=logging.WARNING)
        with caplog.at_level(logging.WARNING):
            _check_at(long_op, 1.0)  # below threshold, no warning
            _done_at(long_op, 1.5)
        assert caplog.records == []

    def test_done_logs_if_previously_warned(self, caplog):
        long_op = _make([0.0], warn_threshold_s=5.0, level=logging.WARNING)
        with caplog.at_level(logging.WARNING):
            _check_at(long_op, 5.0)
            _done_at(long_op, 7.0)
        assert len(caplog.records) == 2
        assert "done, elapsed=7.0" in caplog.records[1].message

    def test_done_without_done_message_is_silent(self, caplog):
        with mock.patch(
            "feldera._long_operation_warning.time.monotonic", return_value=0.0
        ):
            long_op = LongOperationWarning(
                logging.getLogger("test"),
                lambda elapsed: f"waiting, elapsed={elapsed}",
                warn_threshold_s=5.0,
                level=logging.WARNING,
            )
        with caplog.at_level(logging.WARNING):
            _check_at(long_op, 5.0)
            _done_at(long_op, 7.0)
        assert len(caplog.records) == 1

    def test_uses_configured_level(self, caplog):
        long_op = _make([0.0], warn_threshold_s=1.0, level=logging.DEBUG)
        with caplog.at_level(logging.DEBUG):
            _check_at(long_op, 1.0)
        assert len(caplog.records) == 1
        assert caplog.records[0].levelno == logging.DEBUG

    def test_default_level_is_visible_at_info(self, caplog):
        # tests/__init__.py configures this project's test suite at INFO by
        # default. A DEBUG default here would silently drop every warning
        # under that configuration -- which is exactly what happened in CI
        # (https://github.com/feldera/feldera/actions/runs/30584868028): a
        # pipeline stuck provisioning for 180s never logged a single "still
        # waiting" line because the messages were below the configured level.
        long_op = _make([0.0], warn_threshold_s=5.0)  # no explicit level
        with caplog.at_level(logging.INFO):
            _check_at(long_op, 5.0)
        assert len(caplog.records) == 1
