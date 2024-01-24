from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.create_or_replace_connector_response import CreateOrReplaceConnectorResponse
from ...models.create_or_replace_program_request import CreateOrReplaceProgramRequest
from ...models.error_response import ErrorResponse
from ...types import Response


def _get_kwargs(
    connector_name: str,
    *,
    json_body: CreateOrReplaceProgramRequest,
) -> Dict[str, Any]:
    pass

    json_json_body = json_body.to_dict()

    return {
        "method": "put",
        "url": "/v0/connectors/{connector_name}".format(
            connector_name=connector_name,
        ),
        "json": json_json_body,
    }


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[CreateOrReplaceConnectorResponse, ErrorResponse]]:
    if response.status_code == HTTPStatus.OK:
        response_200 = CreateOrReplaceConnectorResponse.from_dict(response.json())

        return response_200
    if response.status_code == HTTPStatus.CREATED:
        response_201 = CreateOrReplaceConnectorResponse.from_dict(response.json())

        return response_201
    if response.status_code == HTTPStatus.CONFLICT:
        response_409 = ErrorResponse.from_dict(response.json())

        return response_409
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[CreateOrReplaceConnectorResponse, ErrorResponse]]:
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
    json_body: CreateOrReplaceProgramRequest,
) -> Response[Union[CreateOrReplaceConnectorResponse, ErrorResponse]]:
    """Create or replace a connector.

     Create or replace a connector.

    Args:
        connector_name (str):
        json_body (CreateOrReplaceProgramRequest): Request to create or replace a program.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[CreateOrReplaceConnectorResponse, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        connector_name=connector_name,
        json_body=json_body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    connector_name: str,
    *,
    client: AuthenticatedClient,
    json_body: CreateOrReplaceProgramRequest,
) -> Optional[Union[CreateOrReplaceConnectorResponse, ErrorResponse]]:
    """Create or replace a connector.

     Create or replace a connector.

    Args:
        connector_name (str):
        json_body (CreateOrReplaceProgramRequest): Request to create or replace a program.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[CreateOrReplaceConnectorResponse, ErrorResponse]
    """

    return sync_detailed(
        connector_name=connector_name,
        client=client,
        json_body=json_body,
    ).parsed


async def asyncio_detailed(
    connector_name: str,
    *,
    client: AuthenticatedClient,
    json_body: CreateOrReplaceProgramRequest,
) -> Response[Union[CreateOrReplaceConnectorResponse, ErrorResponse]]:
    """Create or replace a connector.

     Create or replace a connector.

    Args:
        connector_name (str):
        json_body (CreateOrReplaceProgramRequest): Request to create or replace a program.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[CreateOrReplaceConnectorResponse, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        connector_name=connector_name,
        json_body=json_body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    connector_name: str,
    *,
    client: AuthenticatedClient,
    json_body: CreateOrReplaceProgramRequest,
) -> Optional[Union[CreateOrReplaceConnectorResponse, ErrorResponse]]:
    """Create or replace a connector.

     Create or replace a connector.

    Args:
        connector_name (str):
        json_body (CreateOrReplaceProgramRequest): Request to create or replace a program.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[CreateOrReplaceConnectorResponse, ErrorResponse]
    """

    return (
        await asyncio_detailed(
            connector_name=connector_name,
            client=client,
            json_body=json_body,
        )
    ).parsed
