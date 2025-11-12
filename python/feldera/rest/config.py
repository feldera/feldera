from typing import Optional
import os
from feldera.rest._helpers import requests_verify_from_env
import logging


class Config:
    """
    :class:`.FelderaClient` configuration, which includes authentication information
    and the address of the Feldera API the client will interact with.
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
        See documentation of the `FelderaClient` constructor for the other arguments.

        :param version: (Optional) Version of the API to use.
            Default: `v0`.
        """
        self.url: str = url or os.environ.get("FELDERA_HOST") or "http://localhost:8080"
        self.api_key: Optional[str] = api_key or os.environ.get("FELDERA_API_KEY")
        self.version: str = version or "v0"
        self.timeout: Optional[float] = timeout
        self.connection_timeout: Optional[float] = connection_timeout
        env_verify = requests_verify_from_env()
        self.requests_verify: bool | str = (
            requests_verify if requests_verify is not None else env_verify
        )

        if self.requests_verify is False:
            logging.warning("Feldera client: TLS verification is disabled!")
