from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error_response import ErrorResponse
from ...models.update_service_request import UpdateServiceRequest
from ...models.update_service_response import UpdateServiceResponse
from ...types import Response


def _get_kwargs(
    service_id: str,
    *,
    json_body: UpdateServiceRequest,
) -> Dict[str, Any]:
    pass

    json_json_body = json_body.to_dict()

    return {
        "method": "patch",
        "url": "/v0/services/{service_id}".format(
            service_id=service_id,
        ),
        "json": json_json_body,
    }


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[ErrorResponse, UpdateServiceResponse]]:
    if response.status_code == HTTPStatus.OK:
        response_200 = UpdateServiceResponse.from_dict(response.json())

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
) -> Response[Union[ErrorResponse, UpdateServiceResponse]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    service_id: str,
    *,
    client: AuthenticatedClient,
    json_body: UpdateServiceRequest,
) -> Response[Union[ErrorResponse, UpdateServiceResponse]]:
    """Change a service's description or configuration.

     Change a service's description or configuration.

    Args:
        service_id (str):
        json_body (UpdateServiceRequest): Request to update an existing service.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, UpdateServiceResponse]]
    """

    kwargs = _get_kwargs(
        service_id=service_id,
        json_body=json_body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    service_id: str,
    *,
    client: AuthenticatedClient,
    json_body: UpdateServiceRequest,
) -> Optional[Union[ErrorResponse, UpdateServiceResponse]]:
    """Change a service's description or configuration.

     Change a service's description or configuration.

    Args:
        service_id (str):
        json_body (UpdateServiceRequest): Request to update an existing service.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, UpdateServiceResponse]
    """

    return sync_detailed(
        service_id=service_id,
        client=client,
        json_body=json_body,
    ).parsed


async def asyncio_detailed(
    service_id: str,
    *,
    client: AuthenticatedClient,
    json_body: UpdateServiceRequest,
) -> Response[Union[ErrorResponse, UpdateServiceResponse]]:
    """Change a service's description or configuration.

     Change a service's description or configuration.

    Args:
        service_id (str):
        json_body (UpdateServiceRequest): Request to update an existing service.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, UpdateServiceResponse]]
    """

    kwargs = _get_kwargs(
        service_id=service_id,
        json_body=json_body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    service_id: str,
    *,
    client: AuthenticatedClient,
    json_body: UpdateServiceRequest,
) -> Optional[Union[ErrorResponse, UpdateServiceResponse]]:
    """Change a service's description or configuration.

     Change a service's description or configuration.

    Args:
        service_id (str):
        json_body (UpdateServiceRequest): Request to update an existing service.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, UpdateServiceResponse]
    """

    return (
        await asyncio_detailed(
            service_id=service_id,
            client=client,
            json_body=json_body,
        )
    ).parsed
