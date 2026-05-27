from __future__ import annotations

import json
import logging
import random
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    List,
    Mapping,
    Optional,
    Sequence,
    Union,
)

import requests
from requests.packages import urllib3
from tenacity import (
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from feldera.rest.config import Config
from feldera.rest.errors import (
    FelderaAPIError,
    FelderaCommunicationError,
    FelderaTimeoutError,
)

if TYPE_CHECKING:
    from tenacity import RetryCallState


def json_serialize(body: Any) -> str:
    # serialize as string if this object cannot be serialized (e.g. UUID)
    return json.dumps(body, default=str) if body else "" if body == "" else "null"


def _is_502(exc: BaseException) -> bool:
    return isinstance(exc, FelderaAPIError) and exc.status_code == 502


_SENSITIVE_HEADERS = {"authorization", "cookie", "proxy-authorization", "x-api-key"}


def _redact_headers(headers: dict) -> dict:
    return {
        key: "[REDACTED]" if key.lower() in _SENSITIVE_HEADERS else value
        for key, value in headers.items()
    }


class HttpRequests:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.headers = {"User-Agent": "feldera-python-sdk/v1"}
        self.requests_verify = config.requests_verify

        if isinstance(self.requests_verify, bool) and not self.requests_verify:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def _resolve_bearer(self) -> Optional[str]:
        """Return the bearer token to use for this request, or None."""
        key = self.config.api_key
        if key is None:
            return None
        if callable(key):
            token = key()
            if not isinstance(token, str):
                raise TypeError(
                    f"api_key callable returned {type(token).__name__}, expected str"
                )
            return token.strip()
        return key

    def _headers_with_auth(self) -> dict:
        """Headers for the next request, with a freshly-resolved bearer."""
        headers = dict(self.headers)
        token = self._resolve_bearer()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _check_cluster_health(self) -> bool:
        """Check `/cluster_healthz`; return True iff `all_healthy` is reported."""
        try:
            health_path = (
                self.config.url + "/" + self.config.version + "/cluster_healthz"
            )
            response = requests.get(
                health_path,
                timeout=(self.config.connection_timeout, self.config.timeout),
                headers=self.headers,
                verify=self.requests_verify,
            )

            if response.status_code == 200:
                return bool(response.json().get("all_healthy", False))
            logging.warning(
                "Health check returned status %d; instance may be upgrading",
                response.status_code,
            )
            return False
        except Exception as e:
            logging.error("Health check failed: %s", e)
            return False

    def _is_retryable(self, exc: BaseException) -> bool:
        """Define which exceptions are worth retrying."""
        if isinstance(exc, requests.exceptions.Timeout):
            return True
        if isinstance(exc, FelderaAPIError):
            return exc.status_code in self.config.retry_config.retryable_status_codes
        return False

    def _custom_wait(self, retry_state: "RetryCallState") -> float:
        """
        Compute the wait between retries. Branches by exception type:
          - `Retry-After` header (if any) wins, capped at `max_backoff`.
          - 502: probe `/cluster_healthz`. If the cluster is healthy the 502
            is treated as spurious — return 0 so the retry runs immediately.
            Otherwise return the configured `unhealthy_backoff` (a flat wait
            while an upgrade or restart completes).
          - Everything else: exponential backoff plus optional jitter.
        """
        cfg = self.config.retry_config
        exc = retry_state.outcome.exception() if retry_state.outcome else None

        retry_after = getattr(exc, "retry_after", None)
        if retry_after is not None:
            return min(float(retry_after), cfg.max_backoff)

        if _is_502(exc):
            if self._check_cluster_health():
                logging.info("Cluster healthy — treating 502 as spurious")
                return 0.0
            logging.info(
                "Cluster unhealthy; backing off %.1fs before retrying 502",
                cfg.unhealthy_backoff,
            )
            return cfg.unhealthy_backoff

        backoff = wait_exponential(
            multiplier=cfg.initial_backoff,
            exp_base=cfg.multiplier,
            max=cfg.max_backoff,
        )(retry_state)
        if cfg.jitter > 0:
            backoff += random.uniform(0, cfg.jitter)
        return backoff

    def _do_single_request(
        self,
        http_method: Callable,
        request_path: str,
        data: Any,
        params: Optional[Mapping[str, Any]],
        stream: bool,
        headers: Optional[dict] = None,
    ) -> Any:
        response = http_method(
            request_path,
            data=data,
            timeout=(self.config.connection_timeout, self.config.timeout),
            headers=headers if headers is not None else self.headers,
            params=params,
            stream=stream,
            verify=self.requests_verify,
        )
        resp = self.__validate(response, stream=stream)
        logging.debug("got response: %s", str(resp))
        return resp

    def send_request(
        self,
        http_method: Callable,
        path: str,
        body: Optional[
            Union[Mapping[str, Any], Sequence[Mapping[str, Any]], List[str], str]
        ] = None,
        content_type: str = "application/json",
        params: Optional[Mapping[str, Any]] = None,
        stream: bool = False,
        serialize: bool = True,
    ) -> Any:
        """
        :param http_method: The HTTP method to use. Takes the equivalent `requests.*` module. (Example: `requests.get`)
        :param path: The path to send the request to.
        :param body: The HTTP request body.
        :param content_type: The value for `Content-Type` HTTP header. "application/json" by default.
        :param params: The query parameters part of this request.
        :param stream: True if the response is expected to be a HTTP stream.
        :param serialize: True if the body needs to be serialized to JSON.

        Send an HTTP request, retrying transient failures per the client's
        `RetryConfig`.

        Retry policy:
        - Status codes in `retry_config.retryable_status_codes` (default
          408, 429, 502, 503, 504) and connection/read timeouts retry.
        - 502 probes `/cluster_healthz` to distinguish a spurious gateway
          error (cluster healthy → retry immediately) from a real outage
          (cluster unhealthy → wait `unhealthy_backoff` seconds before
          retrying).
        - Other retryable failures use exponential backoff with optional
          jitter; a server-supplied `Retry-After` header overrides it
          (capped at `max_backoff`).
        - All other errors are raised immediately.
        """
        self.headers["Content-Type"] = content_type
        request_path = self.config.url + "/" + self.config.version + path

        # Serialize the body once, not per retry. None / bytes / `serialize=False`
        # all pass through unchanged.
        if body is None or isinstance(body, bytes) or not serialize:
            data = body
        else:
            data = json_serialize(body)

        headers = self._headers_with_auth()
        headers["Content-Type"] = content_type

        logging.debug(
            "sending %s request to: %s with headers: %s, and params: %s",
            http_method.__name__,
            request_path,
            _redact_headers(headers),
            str(params),
        )

        cfg = self.config.retry_config
        retryer = Retrying(
            retry=retry_if_exception(self._is_retryable),
            wait=self._custom_wait,
            stop=stop_after_attempt(cfg.max_retries + 1),
            reraise=True,
        )

        try:
            for attempt in retryer:
                with attempt:
                    return self._do_single_request(
                        http_method, request_path, data, params, stream, headers
                    )
        except FelderaAPIError as err:
            # On 401, if the bearer is a callable, re-resolve once and retry.
            # Covers tokens that expire mid-flight in long-running scripts
            # without forcing every caller to wrap calls in their own retry.
            # One-shot is enforced by scope: this except runs at most once
            # per `send_request` call.
            if err.status_code == 401 and callable(self.config.api_key):
                logging.info(
                    "401 from %s; re-resolving api_key callable and retrying once",
                    request_path,
                )
                headers = self._headers_with_auth()
                headers["Content-Type"] = content_type
                return self._do_single_request(
                    http_method, request_path, data, params, stream, headers
                )
            raise
        except requests.exceptions.Timeout as err:
            raise FelderaTimeoutError(str(err)) from err
        except requests.exceptions.ConnectionError as err:
            raise FelderaCommunicationError(str(err)) from err

    def get(
        self,
        path: str,
        params: Optional[Mapping[str, Any]] = None,
        stream: bool = False,
    ) -> Any:
        return self.send_request(requests.get, path, params=params, stream=stream)

    def post(
        self,
        path: str,
        body: Optional[
            Union[Mapping[str, Any], Sequence[Mapping[str, Any]], List[str], str]
        ] = None,
        content_type: str = "application/json",
        params: Optional[Mapping[str, Any]] = None,
        stream: bool = False,
        serialize: bool = True,
    ) -> Any:
        return self.send_request(
            requests.post,
            path,
            body,
            content_type,
            params,
            stream=stream,
            serialize=serialize,
        )

    def patch(
        self,
        path: str,
        body: Optional[
            Union[Mapping[str, Any], Sequence[Mapping[str, Any]], List[str], str]
        ] = None,
        content_type: str = "application/json",
        params: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        return self.send_request(requests.patch, path, body, content_type, params)

    def put(
        self,
        path: str,
        body: Optional[
            Union[Mapping[str, Any], Sequence[Mapping[str, Any]], List[str], str]
        ] = None,
        content_type: str = "application/json",
        params: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        return self.send_request(requests.put, path, body, content_type, params)

    def delete(
        self,
        path: str,
        body: Optional[
            Union[Mapping[str, Any], Sequence[Mapping[str, Any]], List[str]]
        ] = None,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        return self.send_request(requests.delete, path, body, params=params)

    @staticmethod
    def __to_json(request: requests.Response) -> Any:
        if request.content == b"":
            return request
        return request.json()

    @staticmethod
    def __validate(request: requests.Response, stream=False) -> Any:
        try:
            request.raise_for_status()

            if request is None:
                # This shouldn't ever be the case, but we've seen it happen
                return FelderaCommunicationError(
                    "Failed to Communicate with Feldera Received None as Response",
                )
            if stream:
                return request
            if request.headers.get("content-type") == "text/plain":
                return request.text
            elif request.headers.get("content-type") == "application/octet-stream":
                return request.content

            resp = HttpRequests.__to_json(request)
            return resp
        except requests.exceptions.HTTPError as err:
            raise FelderaAPIError(str(err), request) from err
