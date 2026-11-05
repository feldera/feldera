from requests import Response
import json
import re
from datetime import datetime, timezone
from typing import Any, List, Mapping, Optional
from urllib.parse import urlparse


# RFC 9110 §5.6.7: HTTP-date "IMF-fixdate" format used by `Retry-After`.
_HTTP_DATE_FMT = "%a, %d %b %Y %H:%M:%S GMT"


def _parse_retry_after(value: Optional[str]) -> Optional[float]:
    """
    Parse an HTTP `Retry-After` header into a delay in seconds.

    Returns `None` if missing or unparseable. Per RFC 9110, the value is
    either a non-negative integer of seconds or an IMF-fixdate HTTP-date.
    """
    if not value:
        return None
    try:
        return max(float(value), 0.0)
    except ValueError:
        pass
    try:
        retry_at = datetime.strptime(value, _HTTP_DATE_FMT).replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return max((retry_at - datetime.now(timezone.utc)).total_seconds(), 0.0)


# RFC 6750 §3: `error_description` in a Bearer challenge, as a quoted string.
_ERROR_DESCRIPTION_RE = re.compile(r'error_description="((?:[^"\\]|\\.)*)"')


def _bearer_error_description(value: Optional[str]) -> Optional[str]:
    """
    Extract `error_description` from a `WWW-Authenticate` header.

    The server puts why it refused a credential here, not in the body.
    Returns `None` if the header is missing or has no description.
    """
    if not value:
        return None
    match = _ERROR_DESCRIPTION_RE.search(value)
    if match is None:
        return None
    # Remove the quoted-string escapes: each `\X` becomes `X`.
    return re.sub(r"\\(.)", r"\1", match.group(1))


class FelderaError(Exception):
    """
    Generic class for Feldera error handling
    """

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"FelderaError. Error message: {self.message}"


class FelderaAPIError(FelderaError):
    """Error sent by Feldera API"""

    def __init__(self, error: str, request: Response) -> None:
        self.status_code = request.status_code
        self.error = error
        self.error_code = None
        self.message = None
        self.details = None
        self.retry_after: Optional[float] = _parse_retry_after(
            request.headers.get("Retry-After")
        )

        err_msg = ""

        if request.text:
            try:
                json_data = json.loads(request.text)

                self.error_code = json_data.get("error_code")
                if self.error_code:
                    err_msg += f"\nError Code: {self.error_code}"
                self.message = json_data.get("message")
                if self.message:
                    err_msg += f"\nMessage: {self.message}"
                self.details = json_data.get("details")
                if self.details:
                    err_msg += f"\nDetails: {self.details}"
            except Exception:
                self.message = request.text
                err_msg += request.text

        err_msg += f"\nResponse Status: {request.status_code}"

        if int(request.status_code) == 401:
            parsed = urlparse(request.request.url)

            auth_err = (
                f"\nAuthorization error at '{parsed.scheme}://{parsed.hostname}': "
            )
            auth = request.request.headers.get("Authorization")
            if auth is None:
                err_msg += f"{auth_err}no credential provided"
            else:
                # The credential may be an API key or a JWT/bearer token; do not
                # assume which. The `Reason` below carries the specifics.
                err_msg += f"{auth_err}credential rejected (invalid or expired token / API key)"
            reason = _bearer_error_description(request.headers.get("WWW-Authenticate"))
            if reason:
                err_msg += f"\nReason: {reason}"

        err_msg = err_msg.strip()

        super().__init__(err_msg)


# Compatibility alias: the RFC and some docs refer to this as `FelderaApiError`.
FelderaApiError = FelderaAPIError


class FelderaTimeoutError(FelderaError):
    """Error when Feldera operation takes longer than expected"""

    def __init__(self, err: str) -> None:
        super().__init__(f"Timeout connecting to Feldera: {err}")


class FelderaCommunicationError(FelderaError):
    """Error when connection to Feldera"""

    def __init__(self, err: str) -> None:
        super().__init__(f"Cannot connect to Feldera API: {err}")


class TableReloadNotAllowedError(RuntimeError):
    """
    Raised when bootstrapping would reload a table that is not in
    `reloadable_tables`, or when the SDK cannot tell which tables it reloads.
    The pipeline stays in the AWAITINGAPPROVAL state: call `approve()` to
    bootstrap or `stop(force=True)` to reject.
    """

    def __init__(
        self,
        pipeline_name: str,
        reason: str,
        approval_diff: Optional[Mapping[str, Any]],
        blocked_tables: Optional[List[str]] = None,
    ) -> None:
        """
        :param pipeline_name: The name of the pipeline. Stored as
            `pipeline_name`.
        :param reason: Why the SDK did not approve the bootstrap. Part of the
            error message.
        :param approval_diff: The diff awaiting approval, as the pipeline
            reported it. Stored as `approval_diff`.
        :param blocked_tables: Modified tables that are not in
            `reloadable_tables`. Stored as `blocked_tables`. Empty if the diff
            is unknown.
        """
        self.pipeline_name = pipeline_name
        self.approval_diff = approval_diff
        self.blocked_tables = blocked_tables or []
        super().__init__(
            f"Table reload not allowed in pipeline '{pipeline_name}': {reason}. "
            "The pipeline is AWAITINGAPPROVAL: call approve() to bootstrap or "
            "stop(force=True) to reject."
        )
