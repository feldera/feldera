import json
import logging
import time
from typing import Any, Callable, List, Mapping, Optional, Sequence, Union

import requests
from requests.packages import urllib3

from feldera.rest.config import Config
from feldera.rest.errors import (
    FelderaAPIError,
    FelderaCommunicationError,
    FelderaTimeoutError,
)


def json_serialize(body: Any) -> str:
    # serialize as string if this object cannot be serialized (e.g. UUID)
    return json.dumps(body, default=str) if body else "" if body == "" else "null"


class HttpRequests:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.headers = {"User-Agent": "feldera-python-sdk/v1"}
        self.requests_verify = config.requests_verify

        if isinstance(self.requests_verify, bool) and not self.requests_verify:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        if self.config.api_key:
            self.headers["Authorization"] = f"Bearer {self.config.api_key}"

    def _check_cluster_health(self) -> bool:
        """Check cluster health via /cluster_healthz endpoint.

        Returns:
            bool: True if both runner and compiler services are healthy, False otherwise
        """
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
                health_data = response.json()
                all_healthy = health_data.get("all_healthy", False)
                return all_healthy
            else:
                logging.warning(
                    f"Health check returned status {response.status_code}. The instance might be in the process of being upgraded. Waiting to see if it recovers."
                )
                return False
        except Exception as e:
            logging.error(f"Health check failed: {e}")
            return False

    def _wait_for_health_recovery(self, max_wait_seconds: int = 300) -> bool:
        """
        Wait for cluster to become healthy after detecting upgrade/restart.

        Args:
            max_wait_seconds: Maximum time to wait for health recovery (default 5 minutes)

        Returns:
            bool: True if cluster became healthy within timeout, False otherwise
        """
        start_time = time.time()
        check_interval = 5

        logging.info(
            f"Waiting for cluster health recovery (max {max_wait_seconds}s)..."
        )

        while time.time() - start_time < max_wait_seconds:
            if self._check_cluster_health():
                elapsed = time.time() - start_time
                logging.info(f"Instance health recovered after {elapsed:.1f}s")
                return True

            time.sleep(check_interval)
            elapsed = time.time() - start_time
            logging.debug(
                f"Still waiting for health recovery ({elapsed:.1f}s elapsed)..."
            )

        logging.warning(f"Instance did not recover within {max_wait_seconds}s timeout")
        return False

    def _handle_502_with_health_check(
        self, path: str, attempt: int, max_retries: int
    ) -> bool:
        """
        Handles 502 errors with health monitoring.

        Args:
            path: The request path that failed
            attempt: Current attempt number (0-based)
            max_retries: Maximum number of retries allowed

        Returns:
            bool: True if should retry, False if should give up
        """
        if attempt >= max_retries:
            return False

        logging.warning(
            f"HTTP 502 received for {path} (attempt {attempt + 1}/{max_retries + 1}), checking cluster health..."
        )

        # Do a short backoff and retry
        time.sleep(min(2 << attempt, 64))

        # First, check if cluster is currently healthy
        if self._check_cluster_health():
            # Instance appears healthy, this might be a spurious 502
            logging.info(
                f"Instance appears healthy, treating 502 for {path} as spurious - retrying"
            )
            return True
        else:
            # Instance is unhealthy, likely an upgrade/restart scenario
            logging.info(
                f"Instance unhealthy for request to {path}, likely upgrade in progress - waiting for recovery..."
            )

            # Wait for cluster to recover (up to 5 minutes)
            recovery_timeout = self.config.health_recovery_timeout
            if self._wait_for_health_recovery(max_wait_seconds=recovery_timeout):
                logging.info(f"Instance health recovered, can now retry {path}...")
                return True
            else:
                # Health didn't recover within timeout
                logging.error(
                    f"Instance health did not recover within {recovery_timeout}s timeout for {path}, giving up"
                )
                return False

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
        max_retries: int = 3,
    ) -> Any:
        """
        Send HTTP request with intelligent retry logic.

        Handles different error conditions with appropriate retry strategies:
        - 502 errors: Checks cluster health and waits for recovery during upgrades
        - 503/408 errors: Standard retry with backoff
        - Timeout errors: Standard retry with backoff

        For 502 errors, the method distinguishes between:
        1. Spurious 502s (cluster healthy): Quick retry with short backoff
        2. Upgrade scenarios (cluster unhealthy): Waits up to health_recovery_timeout
           for cluster to become healthy, then retries

        :param http_method: The HTTP method to use. Takes the equivalent `requests.*` module. (Example: `requests.get`)
        :param path: The path to send the request to.
        :param body: The HTTP request body.
        :param content_type: The value for `Content-Type` HTTP header. "application/json" by default.
        :param params: The query parameters part of this request.
        :param stream: True if the response is expected to be a HTTP stream.
        :param serialize: True if the body needs to be serialized to JSON.
        :param max_retries: Maximum number of retry attempts.
        """
        self.headers["Content-Type"] = content_type

        prev_resp: requests.Response | None = None

        try:
            conn_timeout = self.config.connection_timeout
            timeout = self.config.timeout
            headers = self.headers

            request_path = self.config.url + "/" + self.config.version + path

            logging.debug(
                "sending %s request to: %s with headers: %s, and params: %s",
                http_method.__name__,
                request_path,
                str(headers),
                str(params),
            )

            for attempt in range(max_retries):
                if http_method.__name__ == "get":
                    request = http_method(
                        request_path,
                        timeout=(conn_timeout, timeout),
                        headers=headers,
                        params=params,
                        stream=stream,
                        verify=self.requests_verify,
                    )
                elif isinstance(body, bytes):
                    request = http_method(
                        request_path,
                        timeout=(conn_timeout, timeout),
                        headers=headers,
                        data=body,
                        params=params,
                        stream=stream,
                        verify=self.requests_verify,
                    )
                else:
                    request = http_method(
                        request_path,
                        timeout=(conn_timeout, timeout),
                        headers=headers,
                        data=json_serialize(body) if serialize else body,
                        params=params,
                        stream=stream,
                        verify=self.requests_verify,
                    )

                prev_resp = request

                try:
                    resp = self.__validate(request, stream=stream)
                    logging.debug("got response: %s", str(resp))
                    return resp
                except FelderaAPIError as err:
                    # Handle 502 with intelligent health monitoring
                    if err.status_code == 502:
                        if self._handle_502_with_health_check(
                            path, attempt, max_retries
                        ):
                            continue
                        else:
                            raise
                    # Handle other retryable errors
                    elif err.status_code in [408, 503, 504]:
                        if attempt < max_retries:
                            logging.warning(
                                "HTTP %d received for %s, retrying (%d/%d)...",
                                err.status_code,
                                path,
                                attempt + 1,
                                max_retries,
                            )
                            time.sleep(min(2 << attempt, 64))
                            continue
                    raise  # re-raise for all other errors or if out of retries
                except requests.exceptions.Timeout as err:
                    if attempt < max_retries:
                        logging.warning(
                            "HTTP Connection Timeout for %s, retrying (%d/%d)...",
                            path,
                            attempt + 1,
                            max_retries,
                        )
                        time.sleep(2)
                        continue
                    raise FelderaTimeoutError(str(err)) from err

        except requests.exceptions.ConnectionError as err:
            raise FelderaCommunicationError(str(err)) from err

        raise FelderaAPIError(
            "Max retries exceeded, couldn't successfully connect to Feldera", prev_resp
        )

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
