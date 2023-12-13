from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.api_key_descr import ApiKeyDescr
from ...models.error_response import ErrorResponse
from ...types import Response


def _get_kwargs(
    api_key_name: str,
) -> Dict[str, Any]:
    pass

    return {
        "method": "get",
        "url": "/v0/api_keys/{api_key_name}".format(
            api_key_name=api_key_name,
        ),
    }


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[ApiKeyDescr, ErrorResponse]]:
    if response.status_code == HTTPStatus.OK:
        response_200 = ApiKeyDescr.from_dict(response.json())

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
) -> Response[Union[ApiKeyDescr, ErrorResponse]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    api_key_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[Union[ApiKeyDescr, ErrorResponse]]:
    """Get an API key description

     Get an API key description

    Args:
        api_key_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ApiKeyDescr, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        api_key_name=api_key_name,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    api_key_name: str,
    *,
    client: AuthenticatedClient,
) -> Optional[Union[ApiKeyDescr, ErrorResponse]]:
    """Get an API key description

     Get an API key description

    Args:
        api_key_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ApiKeyDescr, ErrorResponse]
    """

    return sync_detailed(
        api_key_name=api_key_name,
        client=client,
    ).parsed


async def asyncio_detailed(
    api_key_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[Union[ApiKeyDescr, ErrorResponse]]:
    """Get an API key description

     Get an API key description

    Args:
        api_key_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ApiKeyDescr, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        api_key_name=api_key_name,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    api_key_name: str,
    *,
    client: AuthenticatedClient,
) -> Optional[Union[ApiKeyDescr, ErrorResponse]]:
    """Get an API key description

     Get an API key description

    Args:
        api_key_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ApiKeyDescr, ErrorResponse]
    """

    return (
        await asyncio_detailed(
            api_key_name=api_key_name,
            client=client,
        )
    ).parsed
