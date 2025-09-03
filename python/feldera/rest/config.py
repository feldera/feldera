from typing import Optional
import os


class Config:
    """
    :class:`.FelderaClient`'s credentials and configuration parameters
    """

    def __init__(
        self,
        url: Optional[str] = None,
        api_key: Optional[str] = None,
        version: Optional[str] = None,
        timeout: Optional[float] = None,
        connection_timeout: Optional[float] = None,
        requests_verify: bool | str = True,
    ) -> None:
        """
        :param url: The url to the Feldera API (ex: https://try.feldera.com)
        :param api_key: The optional API key to access Feldera
        :param version: The version of the API to use
        :param timeout: The timeout for the HTTP requests
        :param connection_timeout: The connection timeout for the HTTP requests
        :param requests_verify: The `verify` parameter passed to the requests
            library. `True` by default.
        """

        BASE_URL = (
            url
            or os.environ.get("FELDERA_HOST")
            or os.environ.get("FELDERA_BASE_URL")
            or "http://localhost:8080"
        )
        self.url: str = BASE_URL
        self.api_key: Optional[str] = os.environ.get("FELDERA_API_KEY", api_key)
        self.version: Optional[str] = version or "v0"
        self.timeout: Optional[float] = timeout
        self.connection_timeout: Optional[float] = connection_timeout

        FELDERA_TLS_INSECURE = True if os.environ.get("FELDERA_TLS_INSECURE") else False
        FELDERA_HTTPS_TLS_CERT = os.environ.get("FELDERA_HTTPS_TLS_CERT")
        requests_verify = not FELDERA_TLS_INSECURE
        if requests_verify and FELDERA_HTTPS_TLS_CERT is not None:
            requests_verify = FELDERA_HTTPS_TLS_CERT

        self.requests_verify: bool | str = requests_verify
