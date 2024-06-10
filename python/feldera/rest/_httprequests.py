import logging

from feldera.rest.config import Config

from feldera.rest.errors import FelderaAPIError, FelderaTimeoutError, FelderaCommunicationError

import json
import requests
from typing import Callable, Optional, Any, Union, Mapping, Sequence, List


def json_serialize(body: Any) -> str:
    return json.dumps(body) if body else "" if body == "" else "null"


class HttpRequests:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.headers = {
            "User-Agent": "feldera-python-sdk/v1"
        }
        if self.config.api_key:
            self.headers["Authorization"] = f"Bearer {self.config.api_key}"

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
        """
        self.headers["Content-Type"] = content_type

        try:
            timeout = self.config.timeout
            headers = self.headers

            request_path = self.config.url + "/" + self.config.version + path

            logging.debug(
                "sending %s request to: %s with headers: %s, and params: %s",
                http_method.__name__, request_path, str(headers), str(params)
            )

            if http_method.__name__ == "get":
                request = http_method(
                    request_path,
                    timeout=timeout,
                    headers=headers,
                    params=params,
                )
            elif isinstance(body, bytes):
                request = http_method(
                    request_path,
                    timeout=timeout,
                    headers=headers,
                    data=body,
                    params=params,
                    stream=stream,
                )
            else:
                request = http_method(
                    request_path,
                    timeout=timeout,
                    headers=headers,
                    data=json_serialize(body) if serialize else body,
                    params=params,
                    stream=stream,
                )
                if stream:
                    return request
            resp = self.__validate(request)
            logging.debug("got response: %s", str(resp))
            return resp

        except requests.exceptions.Timeout as err:
            raise FelderaTimeoutError(str(err)) from err
        except requests.exceptions.ConnectionError as err:
            raise FelderaCommunicationError(str(err)) from err

    def get(
            self,
            path: str,
            params: Optional[Mapping[str, Any]] = None
    ) -> Any:
        return self.send_request(requests.get, path, params)

    def post(
            self,
            path: str,
            body: Optional[
                Union[Mapping[str, Any], Sequence[Mapping[str, Any]], List[str], str]
            ] = None,
            content_type: Optional[str] = "application/json",
            params: Optional[Mapping[str, Any]] = None,
            stream: bool = False,
            serialize: bool = True,
    ) -> Any:
        return self.send_request(
            requests.post,
            path,
            body,
            content_type,
            params, stream=stream,
            serialize=serialize
        )

    def patch(
            self,
            path: str,
            body: Optional[
                Union[Mapping[str, Any], Sequence[Mapping[str, Any]], List[str], str]
            ] = None,
            content_type: Optional[str] = "application/json",
            params: Optional[Mapping[str, Any]] = None
    ) -> Any:
        return self.send_request(requests.patch, path, body, content_type, params)

    def put(
            self,
            path: str,
            body: Optional[
                Union[Mapping[str, Any], Sequence[Mapping[str, Any]], List[str], str]
            ] = None,
            content_type: Optional[str] = "application/json",
            params: Optional[Mapping[str, Any]] = None
    ) -> Any:
        return self.send_request(requests.put, path, body, content_type, params)

    def delete(
            self,
            path: str,
            body: Optional[Union[Mapping[str, Any], Sequence[Mapping[str, Any]], List[str]]] = None,
            params: Optional[Mapping[str, Any]] = None
    ) -> Any:
        return self.send_request(requests.delete, path, body, params=params)

    @staticmethod
    def __to_json(request: requests.Response) -> Any:
        if request.content == b"":
            return request
        return request.json()

    @staticmethod
    def __validate(request: requests.Response) -> Any:
        try:
            request.raise_for_status()
            resp = HttpRequests.__to_json(request)
            return resp
        except requests.exceptions.HTTPError as err:
            raise FelderaAPIError(str(err), request) from err
