from typing import Optional
import os
from feldera.rest._helpers import requests_verify_from_env
import logging


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
        requests_verify: Optional[bool | str] = None,
    ) -> None:
        """
        :param url: The url to the Feldera API (ex: https://try.feldera.com)
        :param api_key: The optional API key to access Feldera
        :param version: The version of the API to use
        :param timeout: The timeout for the HTTP requests
        :param connection_timeout: The connection timeout for the HTTP requests
        :param requests_verify: The `verify` parameter passed to the requests
            library. `True` by default. Can also be set using environment
            variables `FELDERA_TLS_INSECURE` to disable TLS and
            `FELDERA_HTTPS_TLS_CERT` to set the certificate path. The latter
            takes priority.
        """

        BASE_URL = (
            url
            or os.environ.get("FELDERA_HOST")
            or os.environ.get("FELDERA_BASE_URL")
            or "http://localhost:8080"
        )
        self.url: str = BASE_URL
        self.api_key: Optional[str] = os.environ.get("FELDERA_API_KEY", api_key)
        self.version: str = version or "v0"
        self.timeout: Optional[float] = timeout
        self.connection_timeout: Optional[float] = connection_timeout
        env_verify = requests_verify_from_env()
        self.requests_verify: bool | str = (
            requests_verify if requests_verify is not None else env_verify
        )

        if self.requests_verify is False:
            logging.warning("TLS verification is disabled.")
