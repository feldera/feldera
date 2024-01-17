from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.create_or_replace_program_request import CreateOrReplaceProgramRequest
from ...models.create_or_replace_program_response import CreateOrReplaceProgramResponse
from ...models.error_response import ErrorResponse
from ...types import Response


def _get_kwargs(
    program_name: str,
    *,
    json_body: CreateOrReplaceProgramRequest,
) -> Dict[str, Any]:
    pass

    json_json_body = json_body.to_dict()

    return {
        "method": "put",
        "url": "/v0/programs/{program_name}".format(
            program_name=program_name,
        ),
        "json": json_json_body,
    }


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[CreateOrReplaceProgramResponse, ErrorResponse]]:
    if response.status_code == HTTPStatus.OK:
        response_200 = CreateOrReplaceProgramResponse.from_dict(response.json())

        return response_200
    if response.status_code == HTTPStatus.CREATED:
        response_201 = CreateOrReplaceProgramResponse.from_dict(response.json())

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
) -> Response[Union[CreateOrReplaceProgramResponse, ErrorResponse]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    program_name: str,
    *,
    client: AuthenticatedClient,
    json_body: CreateOrReplaceProgramRequest,
) -> Response[Union[CreateOrReplaceProgramResponse, ErrorResponse]]:
    """Create or replace a program.

     Create or replace a program.

    Args:
        program_name (str):
        json_body (CreateOrReplaceProgramRequest): Request to create or replace a Feldera program.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[CreateOrReplaceProgramResponse, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        program_name=program_name,
        json_body=json_body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    program_name: str,
    *,
    client: AuthenticatedClient,
    json_body: CreateOrReplaceProgramRequest,
) -> Optional[Union[CreateOrReplaceProgramResponse, ErrorResponse]]:
    """Create or replace a program.

     Create or replace a program.

    Args:
        program_name (str):
        json_body (CreateOrReplaceProgramRequest): Request to create or replace a Feldera program.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[CreateOrReplaceProgramResponse, ErrorResponse]
    """

    return sync_detailed(
        program_name=program_name,
        client=client,
        json_body=json_body,
    ).parsed


async def asyncio_detailed(
    program_name: str,
    *,
    client: AuthenticatedClient,
    json_body: CreateOrReplaceProgramRequest,
) -> Response[Union[CreateOrReplaceProgramResponse, ErrorResponse]]:
    """Create or replace a program.

     Create or replace a program.

    Args:
        program_name (str):
        json_body (CreateOrReplaceProgramRequest): Request to create or replace a Feldera program.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[CreateOrReplaceProgramResponse, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        program_name=program_name,
        json_body=json_body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    program_name: str,
    *,
    client: AuthenticatedClient,
    json_body: CreateOrReplaceProgramRequest,
) -> Optional[Union[CreateOrReplaceProgramResponse, ErrorResponse]]:
    """Create or replace a program.

     Create or replace a program.

    Args:
        program_name (str):
        json_body (CreateOrReplaceProgramRequest): Request to create or replace a Feldera program.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[CreateOrReplaceProgramResponse, ErrorResponse]
    """

    return (
        await asyncio_detailed(
            program_name=program_name,
            client=client,
            json_body=json_body,
        )
    ).parsed
