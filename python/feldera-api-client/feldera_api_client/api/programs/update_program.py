from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error_response import ErrorResponse
from ...models.update_program_request import UpdateProgramRequest
from ...models.update_program_response import UpdateProgramResponse
from ...types import Response


def _get_kwargs(
    program_name: str,
    *,
    json_body: UpdateProgramRequest,
) -> Dict[str, Any]:
    pass

    json_json_body = json_body.to_dict()

    return {
        "method": "patch",
        "url": "/v0/programs/{program_name}".format(
            program_name=program_name,
        ),
        "json": json_json_body,
    }


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[ErrorResponse, UpdateProgramResponse]]:
    if response.status_code == HTTPStatus.OK:
        response_200 = UpdateProgramResponse.from_dict(response.json())

        return response_200
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
) -> Response[Union[ErrorResponse, UpdateProgramResponse]]:
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
    json_body: UpdateProgramRequest,
) -> Response[Union[ErrorResponse, UpdateProgramResponse]]:
    """Change one or more of a program's code, description or name.

     Change one or more of a program's code, description or name.

    If a program's code changes, any ongoing compilation gets cancelled,
    the program status is reset to `None`, and the program version
    is incremented by 1.

    Changing only the program's name or description does not affect its
    version or the compilation process.

    Args:
        program_name (str):
        json_body (UpdateProgramRequest): Update program request.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, UpdateProgramResponse]]
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
    json_body: UpdateProgramRequest,
) -> Optional[Union[ErrorResponse, UpdateProgramResponse]]:
    """Change one or more of a program's code, description or name.

     Change one or more of a program's code, description or name.

    If a program's code changes, any ongoing compilation gets cancelled,
    the program status is reset to `None`, and the program version
    is incremented by 1.

    Changing only the program's name or description does not affect its
    version or the compilation process.

    Args:
        program_name (str):
        json_body (UpdateProgramRequest): Update program request.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, UpdateProgramResponse]
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
    json_body: UpdateProgramRequest,
) -> Response[Union[ErrorResponse, UpdateProgramResponse]]:
    """Change one or more of a program's code, description or name.

     Change one or more of a program's code, description or name.

    If a program's code changes, any ongoing compilation gets cancelled,
    the program status is reset to `None`, and the program version
    is incremented by 1.

    Changing only the program's name or description does not affect its
    version or the compilation process.

    Args:
        program_name (str):
        json_body (UpdateProgramRequest): Update program request.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, UpdateProgramResponse]]
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
    json_body: UpdateProgramRequest,
) -> Optional[Union[ErrorResponse, UpdateProgramResponse]]:
    """Change one or more of a program's code, description or name.

     Change one or more of a program's code, description or name.

    If a program's code changes, any ongoing compilation gets cancelled,
    the program status is reset to `None`, and the program version
    is incremented by 1.

    Changing only the program's name or description does not affect its
    version or the compilation process.

    Args:
        program_name (str):
        json_body (UpdateProgramRequest): Update program request.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, UpdateProgramResponse]
    """

    return (
        await asyncio_detailed(
            program_name=program_name,
            client=client,
            json_body=json_body,
        )
    ).parsed
