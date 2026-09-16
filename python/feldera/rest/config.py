import logging
import os
from typing import Callable, Mapping, Optional, Union

from feldera.rest._helpers import requests_verify_from_env
from feldera.rest.retry import RetryConfig

# Either a static bearer (e.g. `"apikey:..."`, a long-lived JWT) or a
# zero-arg callable resolved per-request — covers OIDC workload-identity
# flows that mint short-lived tokens (Kubernetes projected SA token,
# GitHub Actions OIDC, Tailscale tsidp, ...).
ApiKey = Union[str, Callable[[], str]]


def _validated_headers(
    headers: Optional[Mapping[str, str]],
) -> dict[str, str]:
    """Copy `headers`, rejecting what `requests` cannot send as a header.

    A name or value of the wrong type otherwise fails deep inside the request,
    naming neither the header nor the caller that supplied it.
    """
    if not headers:
        return {}
    validated = {}
    for name, value in headers.items():
        if not isinstance(name, str) or not isinstance(value, str):
            raise TypeError(
                f"header {name!r}: names and values must be str, "
                f"got {type(name).__name__} and {type(value).__name__}"
            )
        if not name.strip():
            raise ValueError("a header name must not be empty")
        validated[name] = value
    return validated


class Config:
    """
    :class:`.FelderaClient` configuration, which includes authentication information
    and the address of the Feldera API the client will interact with.
    """

    def __init__(
        self,
        url: Optional[str] = None,
        api_key: Optional[ApiKey] = None,
        version: Optional[str] = None,
        timeout: Optional[float] = None,
        connection_timeout: Optional[float] = None,
        requests_verify: Optional[bool | str] = None,
        retry_config: Optional[RetryConfig] = None,
        tenant: Optional[str] = None,
        headers: Optional[Mapping[str, str]] = None,
    ) -> None:
        """
        See documentation of the `FelderaClient` constructor for the other arguments.

        :param version: (Optional) Version of the API to use.
            Default: `v0`.
        :param retry_config: (Optional) Retry behavior for transient HTTP failures.
            Default: `RetryConfig()` — 3 retries with exponential backoff starting at 2 seconds.
        :param tenant: (Optional) Tenant to act in, sent as the `Feldera-Tenant`
            header. A platform owner uses this to select any tenant (by name or
            UUID); a regular user, to disambiguate among the tenants their token
            authorizes. Default: the token's own/home tenant.
        :param headers: (Optional) Extra HTTP headers sent with every request.
            A header given here replaces the one the client would otherwise send
            under that name, whatever case either spells it in, except
            `Content-Type`, which each request sets itself.
            Default: no extra headers.
        """
        self.url: str = url or os.environ.get("FELDERA_HOST") or "http://localhost:8080"
        self.api_key: Optional[ApiKey] = api_key or os.environ.get("FELDERA_API_KEY")
        self.version: str = version or "v0"
        self.tenant: Optional[str] = tenant or os.environ.get("FELDERA_TENANT")
        self.timeout: Optional[float] = timeout
        self.connection_timeout: Optional[float] = connection_timeout
        self.retry_config: RetryConfig = retry_config or RetryConfig()
        self.headers: dict[str, str] = _validated_headers(headers)
        env_verify = requests_verify_from_env()
        self.requests_verify: bool | str = (
            requests_verify if requests_verify is not None else env_verify
        )

        if self.requests_verify is False:
            logging.warning("Feldera client: TLS verification is disabled!")
