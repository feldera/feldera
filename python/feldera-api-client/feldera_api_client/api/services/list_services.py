from http import HTTPStatus
from typing import Any, Dict, List, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error_response import ErrorResponse
from ...models.service_descr import ServiceDescr
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    id: Union[Unset, None, str] = UNSET,
    name: Union[Unset, None, str] = UNSET,
    config_type: Union[Unset, None, str] = UNSET,
) -> Dict[str, Any]:
    pass

    params: Dict[str, Any] = {}
    params["id"] = id

    params["name"] = name

    params["config_type"] = config_type

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    return {
        "method": "get",
        "url": "/v0/services",
        "params": params,
    }


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[ErrorResponse, List["ServiceDescr"]]]:
    if response.status_code == HTTPStatus.OK:
        response_200 = []
        _response_200 = response.json()
        for response_200_item_data in _response_200:
            response_200_item = ServiceDescr.from_dict(response_200_item_data)

            response_200.append(response_200_item)

        return response_200
    if response.status_code == HTTPStatus.NOT_FOUND:
        response_404 = ErrorResponse.from_dict(response.json())

        return response_404
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[ErrorResponse, List["ServiceDescr"]]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    id: Union[Unset, None, str] = UNSET,
    name: Union[Unset, None, str] = UNSET,
    config_type: Union[Unset, None, str] = UNSET,
) -> Response[Union[ErrorResponse, List["ServiceDescr"]]]:
    """Fetch services, optionally filtered by name or ID.

     Fetch services, optionally filtered by name or ID.

    Args:
        id (Union[Unset, None, str]):
        name (Union[Unset, None, str]):
        config_type (Union[Unset, None, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, List['ServiceDescr']]]
    """

    kwargs = _get_kwargs(
        id=id,
        name=name,
        config_type=config_type,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient,
    id: Union[Unset, None, str] = UNSET,
    name: Union[Unset, None, str] = UNSET,
    config_type: Union[Unset, None, str] = UNSET,
) -> Optional[Union[ErrorResponse, List["ServiceDescr"]]]:
    """Fetch services, optionally filtered by name or ID.

     Fetch services, optionally filtered by name or ID.

    Args:
        id (Union[Unset, None, str]):
        name (Union[Unset, None, str]):
        config_type (Union[Unset, None, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, List['ServiceDescr']]
    """

    return sync_detailed(
        client=client,
        id=id,
        name=name,
        config_type=config_type,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    id: Union[Unset, None, str] = UNSET,
    name: Union[Unset, None, str] = UNSET,
    config_type: Union[Unset, None, str] = UNSET,
) -> Response[Union[ErrorResponse, List["ServiceDescr"]]]:
    """Fetch services, optionally filtered by name or ID.

     Fetch services, optionally filtered by name or ID.

    Args:
        id (Union[Unset, None, str]):
        name (Union[Unset, None, str]):
        config_type (Union[Unset, None, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, List['ServiceDescr']]]
    """

    kwargs = _get_kwargs(
        id=id,
        name=name,
        config_type=config_type,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    id: Union[Unset, None, str] = UNSET,
    name: Union[Unset, None, str] = UNSET,
    config_type: Union[Unset, None, str] = UNSET,
) -> Optional[Union[ErrorResponse, List["ServiceDescr"]]]:
    """Fetch services, optionally filtered by name or ID.

     Fetch services, optionally filtered by name or ID.

    Args:
        id (Union[Unset, None, str]):
        name (Union[Unset, None, str]):
        config_type (Union[Unset, None, str]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, List['ServiceDescr']]
    """

    return (
        await asyncio_detailed(
            client=client,
            id=id,
            name=name,
            config_type=config_type,
        )
    ).parsed
