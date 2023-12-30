from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error_response import ErrorResponse
from ...models.update_connector_request import UpdateConnectorRequest
from ...models.update_connector_response import UpdateConnectorResponse
from ...types import Response


def _get_kwargs(
    connector_name: str,
    *,
    json_body: UpdateConnectorRequest,
) -> Dict[str, Any]:
    pass

    json_json_body = json_body.to_dict()

    return {
        "method": "patch",
        "url": "/v0/connectors/{connector_name}".format(
            connector_name=connector_name,
        ),
        "json": json_json_body,
    }


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[ErrorResponse, UpdateConnectorResponse]]:
    if response.status_code == HTTPStatus.OK:
        response_200 = UpdateConnectorResponse.from_dict(response.json())

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
) -> Response[Union[ErrorResponse, UpdateConnectorResponse]]:
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
    json_body: UpdateConnectorRequest,
) -> Response[Union[ErrorResponse, UpdateConnectorResponse]]:
    """Change a connector's name, description or configuration.

     Change a connector's name, description or configuration.

    Args:
        connector_name (str):
        json_body (UpdateConnectorRequest): Request to update an existing data-connector.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, UpdateConnectorResponse]]
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
    json_body: UpdateConnectorRequest,
) -> Optional[Union[ErrorResponse, UpdateConnectorResponse]]:
    """Change a connector's name, description or configuration.

     Change a connector's name, description or configuration.

    Args:
        connector_name (str):
        json_body (UpdateConnectorRequest): Request to update an existing data-connector.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, UpdateConnectorResponse]
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
    json_body: UpdateConnectorRequest,
) -> Response[Union[ErrorResponse, UpdateConnectorResponse]]:
    """Change a connector's name, description or configuration.

     Change a connector's name, description or configuration.

    Args:
        connector_name (str):
        json_body (UpdateConnectorRequest): Request to update an existing data-connector.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, UpdateConnectorResponse]]
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
    json_body: UpdateConnectorRequest,
) -> Optional[Union[ErrorResponse, UpdateConnectorResponse]]:
    """Change a connector's name, description or configuration.

     Change a connector's name, description or configuration.

    Args:
        connector_name (str):
        json_body (UpdateConnectorRequest): Request to update an existing data-connector.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, UpdateConnectorResponse]
    """

    return (
        await asyncio_detailed(
            connector_name=connector_name,
            client=client,
            json_body=json_body,
        )
    ).parsed
