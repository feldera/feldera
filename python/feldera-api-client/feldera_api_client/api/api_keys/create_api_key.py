from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_key_descr import ApiKeyDescr
from ...models.error_response import ErrorResponse
from ...models.new_api_key_request import NewApiKeyRequest
from ...types import Response


def _get_kwargs(
    *,
    json_body: NewApiKeyRequest,
) -> Dict[str, Any]:
    pass

    json_json_body = json_body.to_dict()

    return {
        "method": "post",
        "url": "/api_keys",
        "json": json_json_body,
    }


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[ApiKeyDescr, ErrorResponse]]:
    if response.status_code == HTTPStatus.OK:
        response_200 = ApiKeyDescr.from_dict(response.json())

        return response_200
    if response.status_code == HTTPStatus.CONFLICT:
        response_409 = ErrorResponse.from_dict(response.json())

        return response_409
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[ApiKeyDescr, ErrorResponse]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    json_body: NewApiKeyRequest,
) -> Response[Union[ApiKeyDescr, ErrorResponse]]:
    """Create an API key

     Create an API key

    Args:
        json_body (NewApiKeyRequest): Request to create a new API key.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ApiKeyDescr, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        json_body=json_body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient,
    json_body: NewApiKeyRequest,
) -> Optional[Union[ApiKeyDescr, ErrorResponse]]:
    """Create an API key

     Create an API key

    Args:
        json_body (NewApiKeyRequest): Request to create a new API key.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ApiKeyDescr, ErrorResponse]
    """

    return sync_detailed(
        client=client,
        json_body=json_body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    json_body: NewApiKeyRequest,
) -> Response[Union[ApiKeyDescr, ErrorResponse]]:
    """Create an API key

     Create an API key

    Args:
        json_body (NewApiKeyRequest): Request to create a new API key.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ApiKeyDescr, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        json_body=json_body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    json_body: NewApiKeyRequest,
) -> Optional[Union[ApiKeyDescr, ErrorResponse]]:
    """Create an API key

     Create an API key

    Args:
        json_body (NewApiKeyRequest): Request to create a new API key.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ApiKeyDescr, ErrorResponse]
    """

    return (
        await asyncio_detailed(
            client=client,
            json_body=json_body,
        )
    ).parsed
