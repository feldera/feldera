from requests import Response
import json


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

        if request.text:
            try:
                json_data = json.loads(request.text)
                self.message = json_data.get("message")
                self.details = json_data.get("details")
                self.error_code = json_data.get("error_code")
            except:
                self.message = request.text

    def __str__(self) -> str:
        if self.error_code:
            return f"FelderaAPIError: {self.error}\n Error code: {self.error_code}\n Error message: {self.message}\n Details: {self.details}"
        else:
            return f"FelderaAPIError: {self.error}\n {self.message}"


class FelderaTimeoutError(FelderaError):
    """Error when Feldera operation takes longer than expected"""

    def __str__(self) -> str:
        return f"FelderaTimeoutError: {self.message}"


class FelderaCommunicationError(FelderaError):
    """Error when connection to Feldera"""

    def __str__(self) -> str:
        return f"FelderaCommunicationError: {self.message}"
