from typing import Optional


class Config:
    """
    :class:`.FelderaClient`'s credentials and configuration parameters
    """

    def __init__(
        self,
        url: str,
        api_key: Optional[str] = None,
        version: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """
        :param url: The url to the Feldera API (ex: https://try.feldera.com)
        :param api_key: The optional API key to access Feldera
        :param version: The version of the API to use
        :param timeout: The timeout for the HTTP requests
        """

        self.url: str = url
        self.api_key: Optional[str] = api_key
        self.version: Optional[str] = version or "v0"
        self.timeout: Optional[float] = timeout
