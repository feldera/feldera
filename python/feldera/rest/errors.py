from requests import Response
import json
from urllib.parse import urlparse


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
