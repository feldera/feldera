from requests import Response
import json
from datetime import datetime, timezone
from typing import Optional
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

            auth_err = f"\nAuthorization error: Failed to connect to '{parsed.scheme}://{parsed.hostname}': "
            auth = request.request.headers.get("Authorization")
            if auth is None:
                err_msg += f"{auth_err} API key not set"
            else:
                err_msg += f"{auth_err} invalid API key"

        err_msg = err_msg.strip()

        super().__init__(err_msg)


class FelderaTimeoutError(FelderaError):
    """Error when Feldera operation takes longer than expected"""

    def __init__(self, err: str) -> None:
        super().__init__(f"Timeout connecting to Feldera: {err}")


class FelderaCommunicationError(FelderaError):
    """Error when connection to Feldera"""

    def __init__(self, err: str) -> None:
        super().__init__(f"Cannot connect to Feldera API: {err}")
