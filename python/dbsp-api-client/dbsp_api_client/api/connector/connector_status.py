from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import Client
from ...models.connector_descr import ConnectorDescr
from ...models.error_response import ErrorResponse
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    client: Client,
    id: Union[Unset, None, str] = UNSET,
    name: Union[Unset, None, str] = UNSET,
) -> Dict[str, Any]:
    url = "{}/connector".format(client.base_url)

    headers: Dict[str, str] = client.get_headers()
    cookies: Dict[str, Any] = client.get_cookies()

    params: Dict[str, Any] = {}
    params["id"] = id

    params["name"] = name

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    return {
        "method": "get",
        "url": url,
        "headers": headers,
        "cookies": cookies,
        "timeout": client.get_timeout(),
        "follow_redirects": client.follow_redirects,
        "params": params,
    }


def _parse_response(*, client: Client, response: httpx.Response) -> Optional[Union[ConnectorDescr, ErrorResponse]]:
    if response.status_code == HTTPStatus.OK:
        response_200 = ConnectorDescr.from_dict(response.json())

        return response_200
    if response.status_code == HTTPStatus.BAD_REQUEST:
        response_400 = ErrorResponse.from_dict(response.json())

        return response_400
    if response.status_code == HTTPStatus.NOT_FOUND:
        response_404 = ErrorResponse.from_dict(response.json())

        return response_404
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(*, client: Client, response: httpx.Response) -> Response[Union[ConnectorDescr, ErrorResponse]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Client,
    id: Union[Unset, None, str] = UNSET,
    name: Union[Unset, None, str] = UNSET,
) -> Response[Union[ConnectorDescr, ErrorResponse]]:
    """Returns connector descriptor.

     Returns connector descriptor.

    Args:
        id (Union[Unset, None, str]):
        name (Union[Unset, None, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ConnectorDescr, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        client=client,
        id=id,
        name=name,
    )

    response = httpx.request(
        verify=client.verify_ssl,
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Client,
    id: Union[Unset, None, str] = UNSET,
    name: Union[Unset, None, str] = UNSET,
) -> Optional[Union[ConnectorDescr, ErrorResponse]]:
    """Returns connector descriptor.

     Returns connector descriptor.

    Args:
        id (Union[Unset, None, str]):
        name (Union[Unset, None, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ConnectorDescr, ErrorResponse]
    """

    return sync_detailed(
        client=client,
        id=id,
        name=name,
    ).parsed


async def asyncio_detailed(
    *,
    client: Client,
    id: Union[Unset, None, str] = UNSET,
    name: Union[Unset, None, str] = UNSET,
) -> Response[Union[ConnectorDescr, ErrorResponse]]:
    """Returns connector descriptor.

     Returns connector descriptor.

    Args:
        id (Union[Unset, None, str]):
        name (Union[Unset, None, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ConnectorDescr, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        client=client,
        id=id,
        name=name,
    )

    async with httpx.AsyncClient(verify=client.verify_ssl) as _client:
        response = await _client.request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Client,
    id: Union[Unset, None, str] = UNSET,
    name: Union[Unset, None, str] = UNSET,
) -> Optional[Union[ConnectorDescr, ErrorResponse]]:
    """Returns connector descriptor.

     Returns connector descriptor.

    Args:
        id (Union[Unset, None, str]):
        name (Union[Unset, None, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ConnectorDescr, ErrorResponse]
    """

    return (
        await asyncio_detailed(
            client=client,
            id=id,
            name=name,
        )
    ).parsed
