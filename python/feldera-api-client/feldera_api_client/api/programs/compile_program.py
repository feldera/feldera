from http import HTTPStatus
from typing import Any, Dict, Optional, Union, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.compile_program_request import CompileProgramRequest
from ...models.error_response import ErrorResponse
from ...types import Response


def _get_kwargs(
    program_id: str,
    *,
    json_body: CompileProgramRequest,
) -> Dict[str, Any]:
    pass

    json_json_body = json_body.to_dict()

    return {
        "method": "post",
        "url": "/v0/programs/{program_id}/compile".format(
            program_id=program_id,
        ),
        "json": json_json_body,
    }


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[Any, ErrorResponse]]:
    if response.status_code == HTTPStatus.ACCEPTED:
        response_202 = cast(Any, None)
        return response_202
    if response.status_code == HTTPStatus.NOT_FOUND:
        response_404 = ErrorResponse.from_dict(response.json())

        return response_404
    if response.status_code == HTTPStatus.CONFLICT:
        response_409 = ErrorResponse.from_dict(response.json())

        return response_409
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[Any, ErrorResponse]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    program_id: str,
    *,
    client: AuthenticatedClient,
    json_body: CompileProgramRequest,
) -> Response[Union[Any, ErrorResponse]]:
    """Mark a program for compilation.

     Mark a program for compilation.

    The client can track a program's compilation status by pollling the
    `/program/{program_id}` or `/programs` endpoints, and
    then checking the `status` field of the program object

    Args:
        program_id (str):
        json_body (CompileProgramRequest): Request to queue a program for compilation.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        program_id=program_id,
        json_body=json_body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    program_id: str,
    *,
    client: AuthenticatedClient,
    json_body: CompileProgramRequest,
) -> Optional[Union[Any, ErrorResponse]]:
    """Mark a program for compilation.

     Mark a program for compilation.

    The client can track a program's compilation status by pollling the
    `/program/{program_id}` or `/programs` endpoints, and
    then checking the `status` field of the program object

    Args:
        program_id (str):
        json_body (CompileProgramRequest): Request to queue a program for compilation.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Any, ErrorResponse]
    """

    return sync_detailed(
        program_id=program_id,
        client=client,
        json_body=json_body,
    ).parsed


async def asyncio_detailed(
    program_id: str,
    *,
    client: AuthenticatedClient,
    json_body: CompileProgramRequest,
) -> Response[Union[Any, ErrorResponse]]:
    """Mark a program for compilation.

     Mark a program for compilation.

    The client can track a program's compilation status by pollling the
    `/program/{program_id}` or `/programs` endpoints, and
    then checking the `status` field of the program object

    Args:
        program_id (str):
        json_body (CompileProgramRequest): Request to queue a program for compilation.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        program_id=program_id,
        json_body=json_body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    program_id: str,
    *,
    client: AuthenticatedClient,
    json_body: CompileProgramRequest,
) -> Optional[Union[Any, ErrorResponse]]:
    """Mark a program for compilation.

     Mark a program for compilation.

    The client can track a program's compilation status by pollling the
    `/program/{program_id}` or `/programs` endpoints, and
    then checking the `status` field of the program object

    Args:
        program_id (str):
        json_body (CompileProgramRequest): Request to queue a program for compilation.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Any, ErrorResponse]
    """

    return (
        await asyncio_detailed(
            program_id=program_id,
            client=client,
            json_body=json_body,
        )
    ).parsed
