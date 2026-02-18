import logging
import math

logger = logging.getLogger(__name__)


def normalize_wait_timeout(
    timeout_s: float | None, default_timeout_s: float, operation: str
) -> float:
    """
    Ensure wait loops always use a finite timeout.
    """
    if timeout_s is None:
        return default_timeout_s
    if not math.isfinite(timeout_s):
        logger.warning(
            "%s called with non-finite timeout %r; defaulting to %.1fs",
            operation,
            timeout_s,
            default_timeout_s,
        )
        return default_timeout_s
    if timeout_s <= 0:
        logger.warning(
            "%s called with non-positive timeout %r; defaulting to %.1fs",
            operation,
            timeout_s,
            default_timeout_s,
        )
        return default_timeout_s
    return timeout_s


def normalize_poll_interval(
    poll_interval_s: float, default_poll_interval_s: float, operation: str
) -> float:
    """
    Ensure wait loops always use a finite positive poll interval.
    """
    if not math.isfinite(poll_interval_s) or poll_interval_s <= 0:
        logger.warning(
            "%s called with invalid poll interval %r; defaulting to %.1fs",
            operation,
            poll_interval_s,
            default_poll_interval_s,
        )
        return default_poll_interval_s
    return poll_interval_s
