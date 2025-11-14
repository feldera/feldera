import logging
import os
from typing import Optional

from feldera.rest._helpers import requests_verify_from_env


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
        health_recovery_timeout: Optional[int] = None,
    ) -> None:
        """
        See documentation of the `FelderaClient` constructor for the other arguments.

        :param version: (Optional) Version of the API to use.
            Default: `v0`.
        :param health_recovery_timeout: (Optional) Maximum time in seconds to wait for cluster health recovery after a 502 error.
            Default: `300` (5 minutes).
        """
        self.url: str = url or os.environ.get("FELDERA_HOST") or "http://localhost:8080"
        self.api_key: Optional[str] = api_key or os.environ.get("FELDERA_API_KEY")
        self.version: str = version or "v0"
        self.timeout: Optional[float] = timeout
        self.connection_timeout: Optional[float] = connection_timeout
        self.health_recovery_timeout: int = health_recovery_timeout or 300
        env_verify = requests_verify_from_env()
        self.requests_verify: bool | str = (
            requests_verify if requests_verify is not None else env_verify
        )

        if self.requests_verify is False:
            logging.warning("Feldera client: TLS verification is disabled!")
