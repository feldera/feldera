from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import Client
from ...models.error_response import ErrorResponse
from ...models.new_program_request import NewProgramRequest
from ...models.new_program_response import NewProgramResponse
from ...types import Response


def _get_kwargs(
    *,
    client: Client,
    json_body: NewProgramRequest,
) -> Dict[str, Any]:
    url = "{}/programs".format(client.base_url)

    headers: Dict[str, str] = client.get_headers()
    cookies: Dict[str, Any] = client.get_cookies()

    json_json_body = json_body.to_dict()

    return {
        "method": "post",
        "url": url,
        "headers": headers,
        "cookies": cookies,
        "timeout": client.get_timeout(),
        "follow_redirects": client.follow_redirects,
        "json": json_json_body,
    }


def _parse_response(*, client: Client, response: httpx.Response) -> Optional[Union[ErrorResponse, NewProgramResponse]]:
    if response.status_code == HTTPStatus.CREATED:
        response_201 = NewProgramResponse.from_dict(response.json())

        return response_201
    if response.status_code == HTTPStatus.CONFLICT:
        response_409 = ErrorResponse.from_dict(response.json())

        return response_409
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(*, client: Client, response: httpx.Response) -> Response[Union[ErrorResponse, NewProgramResponse]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Client,
    json_body: NewProgramRequest,
) -> Response[Union[ErrorResponse, NewProgramResponse]]:
    """Create a new program.

     Create a new program.

    If the `overwrite_existing` flag is set in the request and a program with
    the same name already exists, all pipelines associated with that program and
    the program itself will be deleted.

    Args:
        json_body (NewProgramRequest): Request to create a new DBSP program.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, NewProgramResponse]]
    """

    kwargs = _get_kwargs(
        client=client,
        json_body=json_body,
    )

    response = httpx.request(
        verify=client.verify_ssl,
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Client,
    json_body: NewProgramRequest,
) -> Optional[Union[ErrorResponse, NewProgramResponse]]:
    """Create a new program.

     Create a new program.

    If the `overwrite_existing` flag is set in the request and a program with
    the same name already exists, all pipelines associated with that program and
    the program itself will be deleted.

    Args:
        json_body (NewProgramRequest): Request to create a new DBSP program.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, NewProgramResponse]
    """

    return sync_detailed(
        client=client,
        json_body=json_body,
    ).parsed


async def asyncio_detailed(
    *,
    client: Client,
    json_body: NewProgramRequest,
) -> Response[Union[ErrorResponse, NewProgramResponse]]:
    """Create a new program.

     Create a new program.

    If the `overwrite_existing` flag is set in the request and a program with
    the same name already exists, all pipelines associated with that program and
    the program itself will be deleted.

    Args:
        json_body (NewProgramRequest): Request to create a new DBSP program.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, NewProgramResponse]]
    """

    kwargs = _get_kwargs(
        client=client,
        json_body=json_body,
    )

    async with httpx.AsyncClient(verify=client.verify_ssl) as _client:
        response = await _client.request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Client,
    json_body: NewProgramRequest,
) -> Optional[Union[ErrorResponse, NewProgramResponse]]:
    """Create a new program.

     Create a new program.

    If the `overwrite_existing` flag is set in the request and a program with
    the same name already exists, all pipelines associated with that program and
    the program itself will be deleted.

    Args:
        json_body (NewProgramRequest): Request to create a new DBSP program.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, NewProgramResponse]
    """

    return (
        await asyncio_detailed(
            client=client,
            json_body=json_body,
        )
    ).parsed
