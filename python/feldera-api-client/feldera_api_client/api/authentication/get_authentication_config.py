from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.auth_provider_type_0 import AuthProviderType0
from ...models.auth_provider_type_1 import AuthProviderType1
from ...models.error_response import ErrorResponse
from ...types import Response


def _get_kwargs() -> Dict[str, Any]:
    pass

    return {
        "method": "get",
        "url": "/config/authentication",
    }


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[ErrorResponse, Union["AuthProviderType0", "AuthProviderType1"]]]:
    if response.status_code == HTTPStatus.OK:

        def _parse_response_200(data: object) -> Union["AuthProviderType0", "AuthProviderType1"]:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_auth_provider_type_0 = AuthProviderType0.from_dict(data)

                return componentsschemas_auth_provider_type_0
            except:  # noqa: E722
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_auth_provider_type_1 = AuthProviderType1.from_dict(data)

            return componentsschemas_auth_provider_type_1

        response_200 = _parse_response_200(response.json())

        return response_200
    if response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
        response_500 = ErrorResponse.from_dict(response.json())

        return response_500
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[ErrorResponse, Union["AuthProviderType0", "AuthProviderType1"]]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[Union[ErrorResponse, Union["AuthProviderType0", "AuthProviderType1"]]]:
    """Get authentication provider configuration

     Get authentication provider configuration

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, Union['AuthProviderType0', 'AuthProviderType1']]]
    """

    kwargs = _get_kwargs()

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[Union[ErrorResponse, Union["AuthProviderType0", "AuthProviderType1"]]]:
    """Get authentication provider configuration

     Get authentication provider configuration

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, Union['AuthProviderType0', 'AuthProviderType1']]
    """

    return sync_detailed(
        client=client,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[Union[ErrorResponse, Union["AuthProviderType0", "AuthProviderType1"]]]:
    """Get authentication provider configuration

     Get authentication provider configuration

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, Union['AuthProviderType0', 'AuthProviderType1']]]
    """

    kwargs = _get_kwargs()

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[Union[ErrorResponse, Union["AuthProviderType0", "AuthProviderType1"]]]:
    """Get authentication provider configuration

     Get authentication provider configuration

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, Union['AuthProviderType0', 'AuthProviderType1']]
    """

    return (
        await asyncio_detailed(
            client=client,
        )
    ).parsed
