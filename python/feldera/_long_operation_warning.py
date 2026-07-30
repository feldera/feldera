import logging
import time
from typing import Callable, Optional


class LongOperationWarning:
    """
    Suppresses per-iteration logging in polling loops.

    Logging on every iteration of a wait loop floods the log when polling
    quickly or waiting a long time. Instead, this logs only after
    `warn_threshold_s` seconds have elapsed, then doubles the threshold
    before warning again, so warnings become progressively less frequent the
    longer the operation runs. If a warning was ever emitted, a later call to
    `done()` logs that the operation finished, so a warning about a stuck
    operation does not go unresolved in the log.

    Mirrors `LongOperationWarning` in crates/adapters/src/util.rs. Defaults to
    WARNING, not DEBUG: these messages only fire rarely (thanks to the
    backoff), so there's no spam to hide, and a DEBUG default would make them
    invisible under most logging configurations -- including this project's
    own test suite, which defaults to INFO.
    """

    def __init__(
        self,
        logger: logging.Logger,
        message: Callable[[float], str],
        done_message: Optional[Callable[[float], str]] = None,
        warn_threshold_s: float = 5.0,
        level: int = logging.WARNING,
    ):
        self._logger = logger
        self._message = message
        self._done_message = done_message
        self._warn_threshold_s = warn_threshold_s
        self._level = level
        self._start = time.monotonic()
        self._warned = False

    def elapsed(self) -> float:
        """Seconds elapsed since this object was created."""
        return time.monotonic() - self._start

    def check(self) -> None:
        """Logs a warning if the current threshold has elapsed, then doubles it."""
        elapsed = self.elapsed()
        if elapsed >= self._warn_threshold_s:
            self._logger.log(self._level, self._message(elapsed))
            self._warn_threshold_s *= 2
            self._warned = True

    def done(self) -> None:
        """Logs completion, but only if `check()` previously logged a warning."""
        if self._warned and self._done_message is not None:
            self._logger.log(self._level, self._done_message(self.elapsed()))
