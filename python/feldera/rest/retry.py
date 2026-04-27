from dataclasses import dataclass, field
from typing import FrozenSet


_DEFAULT_RETRYABLE_STATUS_CODES: FrozenSet[int] = frozenset({408, 429, 502, 503, 504})


@dataclass(frozen=True)
class RetryConfig:
    """
    Configures retry behavior for the Feldera HTTP client.

    Retries are attempted on transient failures: connection/read timeouts and
    the HTTP statuses listed in `retryable_status_codes` (408, 429, 502, 503,
    504 by default).

    Wait strategies:
      - 408, 429, 503, 504 and connection/read timeouts use exponential
        backoff: `min(initial_backoff * (multiplier ** n), max_backoff)`,
        plus a uniform random `[0, jitter)` term, where `n` is the
        zero-based retry index.
      - 502 uses cluster-aware backoff: the client probes
        `/cluster_healthz`; if the cluster is healthy, the 502 is treated as
        spurious and the next retry runs immediately (wait = 0). If the
        cluster reports unhealthy (e.g. an upgrade is in progress), the next
        retry waits `unhealthy_backoff` seconds.
      - A server-supplied `Retry-After` header always overrides the computed
        wait (capped at `max_backoff`).

    :param max_retries: Number of retries to attempt after the initial request.
        A value of `3` means up to `4` total attempts. Must be `>= 0`.
        Default: `3`.
    :param initial_backoff: Base wait in seconds before the first retry.
        Default: `2.0`.
    :param max_backoff: Maximum wait in seconds between retries. The computed
        exponential wait is clamped to this value. Default: `64.0`.
    :param multiplier: Exponential base applied to `initial_backoff` for each
        successive retry. Default: `2.0`.
    :param jitter: Maximum random extra wait in seconds added to each
        exponential backoff (drawn uniformly from `[0, jitter)`). Helps avoid
        thundering-herd retries when many clients fail at once.
        Default: `0.0` (no jitter).
    :param unhealthy_backoff: Flat wait in seconds between 502 retries when
        the cluster reports unhealthy on `/cluster_healthz`. The cluster is
        likely upgrading/restarting, so a flat pause is preferable to an
        exponential ramp. Default: `90.0`.
    :param retryable_status_codes: HTTP status codes that should trigger a
        retry. Default: `{408, 429, 502, 503, 504}`.
    """

    max_retries: int = 3
    initial_backoff: float = 2.0
    max_backoff: float = 64.0
    multiplier: float = 2.0
    jitter: float = 0.0
    unhealthy_backoff: float = 90.0
    retryable_status_codes: FrozenSet[int] = field(
        default_factory=lambda: _DEFAULT_RETRYABLE_STATUS_CODES
    )

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.initial_backoff < 0:
            raise ValueError("initial_backoff must be >= 0")
        if self.max_backoff < 0:
            raise ValueError("max_backoff must be >= 0")
        if self.multiplier <= 0:
            raise ValueError("multiplier must be > 0")
        if self.jitter < 0:
            raise ValueError("jitter must be >= 0")
        if self.unhealthy_backoff < 0:
            raise ValueError("unhealthy_backoff must be >= 0")
        # Coerce to frozenset so callers can pass a set/list without surprises,
        # and so equality comparisons against the default behave intuitively.
        if not isinstance(self.retryable_status_codes, frozenset):
            object.__setattr__(
                self, "retryable_status_codes", frozenset(self.retryable_status_codes)
            )
