from __future__ import annotations

import json
import logging
import random
import time
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
    stop_after_delay,
    wait_exponential,
)

from feldera.rest._jwt import seconds_since_expiry
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

# How often to re-resolve while waiting for a refresher to replace an expired
# token. Short enough that the wait costs little beyond the refresher's own
# schedule, long enough not to spin on a file.
_EXPIRED_TOKEN_POLL_SECONDS = 5.0

# Bound at import so a test can give this module a fake clock without also
# replacing the one tenacity naps on.
_monotonic = time.monotonic
_sleep = time.sleep

_BEARER_PREFIX = "Bearer "


def _bearer_of(headers: Mapping[str, str]) -> Optional[str]:
    """The token `headers` will actually present, or None if they present none."""
    value = headers.get("Authorization", "")
    return value[len(_BEARER_PREFIX) :] if value.startswith(_BEARER_PREFIX) else None


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
        """Headers for the next request, with a freshly-resolved bearer and the
        selected tenant (if any)."""
        headers = dict(self.headers)
        token = self._resolve_bearer()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if self.config.tenant:
            headers["Feldera-Tenant"] = self.config.tenant
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
                headers=self._headers_with_auth(),
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

    def _is_retryable(self, exc: BaseException, idempotent: bool) -> bool:
        """Define which exceptions are worth retrying.

        `idempotent` gates `ConnectionError` (e.g. a connection reset mid-poll):
        safe to retry for GET, where a lost response can't have caused a
        server-side side effect, but not for POST/PUT/PATCH/DELETE, where the
        original request may already have been applied.
        """
        if isinstance(exc, requests.exceptions.Timeout):
            return True
        if idempotent and isinstance(exc, requests.exceptions.ConnectionError):
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

    def _reauthenticated_headers(self, sent: dict, request_path: str) -> dict:
        """Headers for the one retry of a request the instance answered 401.

        A credential that refreshes in the background can be replaced between
        the moment a request is built and the moment it is rejected, so
        re-resolving covers that race for free. Where re-resolving still yields
        an expired token, only that token is worth waiting on: its refresher is
        late and will replace it, whereas anything else is a credential the
        instance genuinely refuses, and waiting on that would turn a wrong API
        key into a hang.
        """
        headers = self._headers_with_auth()
        previously_sent = sent.get("Authorization")
        # Judge the token these headers carry, never a second resolve of the
        # credential: a refresher landing between the two reads would clear the
        # wait while the retry still went out with the expired token.
        expired_for = seconds_since_expiry(_bearer_of(headers))
        budget = self.config.retry_config.expired_token_wait_seconds

        if expired_for is None or budget <= 0:
            if headers.get("Authorization") != previously_sent:
                logging.info(
                    "401 from %s; the credential had already been replaced, retrying",
                    request_path,
                )
            else:
                logging.info(
                    "401 from %s; re-resolving api_key callable and retrying once",
                    request_path,
                )
            return headers

        logging.warning(
            "401 from %s: the bearer expired %.0fs ago, so whatever refreshes "
            "it is late; waiting up to %.0fs for a live one",
            request_path,
            expired_for,
            budget,
        )
        deadline = _monotonic() + budget
        while True:
            remaining = deadline - _monotonic()
            if remaining <= 0:
                break
            _sleep(min(_EXPIRED_TOKEN_POLL_SECONDS, remaining))
            headers = self._headers_with_auth()
            # A refresher minting from a skewed clock can replace one dead
            # token with another, so a token that merely differs is not yet
            # worth spending the single retry on.
            if seconds_since_expiry(_bearer_of(headers)) is None:
                logging.info(
                    "the bearer was refreshed after %.0fs; retrying %s",
                    budget - (deadline - _monotonic()),
                    request_path,
                )
                return headers

        logging.error(
            "the bearer was still expired after %.0fs; retrying with it so the "
            "instance reports the failure",
            budget,
        )
        return headers

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
        idempotent: Optional[bool] = None,
    ) -> Any:
        """
        :param http_method: The HTTP method to use. Takes the equivalent `requests.*` module. (Example: `requests.get`)
        :param path: The path to send the request to.
        :param body: The HTTP request body.
        :param content_type: The value for `Content-Type` HTTP header. "application/json" by default.
        :param params: The query parameters part of this request.
        :param stream: True if the response is expected to be a HTTP stream.
        :param serialize: True if the body needs to be serialized to JSON.
        :param idempotent: Overrides the by-method idempotency assumption
            (only GET is assumed idempotent). Callers whose endpoint is
            idempotent by API contract, such as a desired-state setter,
            pass True so a dropped connection retries.

        Send an HTTP request, retrying transient failures per the client's
        `RetryConfig`.

        Retry policy:
        - Status codes in `retry_config.retryable_status_codes` (default
          408, 429, 502, 503, 504) and connection/read timeouts retry.
        - For GET, and for any request marked `idempotent=True`, a
          `ConnectionError` (e.g. connection reset mid-request) also
          retries: a lost response can't change the outcome of an
          idempotent request. Other POST/PUT/PATCH/DELETE requests do not
          retry it, since the original request may already have been
          applied.
        - A DELETE marked `idempotent=True` that receives 404 on a retry
          attempt reports success: the resource vanished between attempts,
          so an earlier attempt was applied and the postcondition holds.
        - 502 probes `/cluster_healthz` to distinguish a spurious gateway
          error (cluster healthy → retry immediately) from a real outage
          (cluster unhealthy → wait `unhealthy_backoff` seconds before
          retrying).
        - Other retryable failures use exponential backoff with optional
          jitter; a server-supplied `Retry-After` header overrides it
          (capped at `max_backoff`).
        - Retrying stops after `max_retries` retries, or, when
          `retry_config.deadline_seconds` is set, once that wall-clock
          budget is spent (the attempt count is then unbounded).
        - All other errors are raised immediately.
        """
        is_idempotent = (
            (http_method is requests.get) if idempotent is None else idempotent
        )
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
        # A wall-clock deadline (when configured) replaces the attempt cap:
        # transient outages last for a duration, not a number of requests.
        stop = (
            stop_after_delay(cfg.deadline_seconds)
            if cfg.deadline_seconds is not None
            else stop_after_attempt(cfg.max_retries + 1)
        )
        retryer = Retrying(
            retry=retry_if_exception(
                lambda exc: self._is_retryable(exc, is_idempotent)
            ),
            wait=self._custom_wait,
            stop=stop,
            reraise=True,
        )

        try:
            for attempt in retryer:
                with attempt:
                    try:
                        return self._do_single_request(
                            http_method, request_path, data, params, stream, headers
                        )
                    except FelderaAPIError as err:
                        if (
                            is_idempotent
                            and http_method is requests.delete
                            and err.status_code == 404
                            and attempt.retry_state.attempt_number > 1
                        ):
                            # The resource vanished between attempts: an
                            # earlier attempt was applied server-side even
                            # though its response was lost. The postcondition
                            # (resource absent) holds, so report success.
                            return None
                        raise
        except FelderaAPIError as err:
            # On 401, if the bearer is a callable, re-resolve and retry once.
            # Covers tokens that expire mid-flight in long-running scripts
            # without forcing every caller to wrap calls in their own retry.
            # One retry is enforced by scope: this except runs at most once
            # per `send_request` call.
            if err.status_code == 401 and callable(self.config.api_key):
                headers = self._reauthenticated_headers(headers, request_path)
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
        idempotent: Optional[bool] = None,
    ) -> Any:
        return self.send_request(
            requests.post,
            path,
            body,
            content_type,
            params,
            stream=stream,
            serialize=serialize,
            idempotent=idempotent,
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
        idempotent: Optional[bool] = None,
    ) -> Any:
        return self.send_request(
            requests.put, path, body, content_type, params, idempotent=idempotent
        )

    def delete(
        self,
        path: str,
        body: Optional[
            Union[Mapping[str, Any], Sequence[Mapping[str, Any]], List[str]]
        ] = None,
        params: Optional[Mapping[str, Any]] = None,
        idempotent: Optional[bool] = None,
    ) -> Any:
        return self.send_request(
            requests.delete, path, body, params=params, idempotent=idempotent
        )

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
