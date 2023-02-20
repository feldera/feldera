from dbsp_api_client.types import Response
from dbsp_api_client.models.error_response import ErrorResponse

class DBSPServerError(Exception):
    """Exception raised when an HTTP request to the DBSP server fails.
    """

    def __init__(self, response: Response, description: str):
        self.description = description
        self.response = response

        if isinstance(response.parsed, ErrorResponse):
            response_body = response.parsed.message
        else:
            response_body = str(response.parsed)
        message = description + "\nHTTP response code: " + str(response.status_code) + "\nResponse body: " + response_body 
        super().__init__(message)

class CompilationException(Exception):
    """Error returned by the DBSP compiler.

    Attributes:
        message: Compiler error message.
    """
    def __init__(self, status):
        if hasattr(status, 'sql_error'):
            self.message = "SQL error: " + status.sql_error
        elif hasattr(status, 'rust_error'):
            self.message = "Rust compiler error: " + status.rust_error
        else:
            self.message = "Unexpected project status: " + str(status)

        super().__init__(self.message)

class TimeoutException(Exception):
    """Exception raised when a DBSP operation times out.
    """
    pass

# Add a method to the `Response` class to throw an error when the HTTP
# status in the response is not not a success.
def unwrap(self, description = "DBSP request failed"):
    if 200 <= self.status_code <= 202:
        return self.parsed
    else:
        raise DBSPServerError(response = self, description = description)

Response.unwrap = unwrap

