from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import Client
from ...models.error_response import ErrorResponse
from ...models.program_code_response import ProgramCodeResponse
from ...types import Response


def _get_kwargs(
    program_id: str,
    *,
    client: Client,
) -> Dict[str, Any]:
    url = "{}/program/{program_id}/code".format(client.base_url, program_id=program_id)

    headers: Dict[str, str] = client.get_headers()
    cookies: Dict[str, Any] = client.get_cookies()

    return {
        "method": "get",
        "url": url,
        "headers": headers,
        "cookies": cookies,
        "timeout": client.get_timeout(),
        "follow_redirects": client.follow_redirects,
    }


def _parse_response(*, client: Client, response: httpx.Response) -> Optional[Union[ErrorResponse, ProgramCodeResponse]]:
    if response.status_code == HTTPStatus.OK:
        response_200 = ProgramCodeResponse.from_dict(response.json())

        return response_200
    if response.status_code == HTTPStatus.BAD_REQUEST:
        response_400 = ErrorResponse.from_dict(response.json())

        return response_400
    if response.status_code == HTTPStatus.NOT_FOUND:
        response_404 = ErrorResponse.from_dict(response.json())

        return response_404
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(*, client: Client, response: httpx.Response) -> Response[Union[ErrorResponse, ProgramCodeResponse]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    program_id: str,
    *,
    client: Client,
) -> Response[Union[ErrorResponse, ProgramCodeResponse]]:
    """Returns the latest SQL source code of the program along with its meta-data.

     Returns the latest SQL source code of the program along with its meta-data.

    Args:
        program_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, ProgramCodeResponse]]
    """

    kwargs = _get_kwargs(
        program_id=program_id,
        client=client,
    )

    response = httpx.request(
        verify=client.verify_ssl,
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    program_id: str,
    *,
    client: Client,
) -> Optional[Union[ErrorResponse, ProgramCodeResponse]]:
    """Returns the latest SQL source code of the program along with its meta-data.

     Returns the latest SQL source code of the program along with its meta-data.

    Args:
        program_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, ProgramCodeResponse]
    """

    return sync_detailed(
        program_id=program_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    program_id: str,
    *,
    client: Client,
) -> Response[Union[ErrorResponse, ProgramCodeResponse]]:
    """Returns the latest SQL source code of the program along with its meta-data.

     Returns the latest SQL source code of the program along with its meta-data.

    Args:
        program_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, ProgramCodeResponse]]
    """

    kwargs = _get_kwargs(
        program_id=program_id,
        client=client,
    )

    async with httpx.AsyncClient(verify=client.verify_ssl) as _client:
        response = await _client.request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    program_id: str,
    *,
    client: Client,
) -> Optional[Union[ErrorResponse, ProgramCodeResponse]]:
    """Returns the latest SQL source code of the program along with its meta-data.

     Returns the latest SQL source code of the program along with its meta-data.

    Args:
        program_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, ProgramCodeResponse]
    """

    return (
        await asyncio_detailed(
            program_id=program_id,
            client=client,
        )
    ).parsed
