from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.connector_descr import ConnectorDescr
from ...models.error_response import ErrorResponse
from ...types import Response


def _get_kwargs(
    connector_name: str,
) -> Dict[str, Any]:
    pass

    return {
        "method": "get",
        "url": "/v0/connectors/{connector_name}".format(
            connector_name=connector_name,
        ),
    }


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[ConnectorDescr, ErrorResponse]]:
    if response.status_code == HTTPStatus.OK:
        response_200 = ConnectorDescr.from_dict(response.json())

        return response_200
    if response.status_code == HTTPStatus.BAD_REQUEST:
        response_400 = ErrorResponse.from_dict(response.json())

        return response_400
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[ConnectorDescr, ErrorResponse]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    connector_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[Union[ConnectorDescr, ErrorResponse]]:
    """Fetch a connector by ID.

     Fetch a connector by ID.

    Args:
        connector_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ConnectorDescr, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        connector_name=connector_name,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    connector_name: str,
    *,
    client: AuthenticatedClient,
) -> Optional[Union[ConnectorDescr, ErrorResponse]]:
    """Fetch a connector by ID.

     Fetch a connector by ID.

    Args:
        connector_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ConnectorDescr, ErrorResponse]
    """

    return sync_detailed(
        connector_name=connector_name,
        client=client,
    ).parsed


async def asyncio_detailed(
    connector_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[Union[ConnectorDescr, ErrorResponse]]:
    """Fetch a connector by ID.

     Fetch a connector by ID.

    Args:
        connector_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ConnectorDescr, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        connector_name=connector_name,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    connector_name: str,
    *,
    client: AuthenticatedClient,
) -> Optional[Union[ConnectorDescr, ErrorResponse]]:
    """Fetch a connector by ID.

     Fetch a connector by ID.

    Args:
        connector_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ConnectorDescr, ErrorResponse]
    """

    return (
        await asyncio_detailed(
            connector_name=connector_name,
            client=client,
        )
    ).parsed
