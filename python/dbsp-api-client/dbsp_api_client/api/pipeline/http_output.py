from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import Client
from ...models.chunk import Chunk
from ...models.error_response import ErrorResponse
from ...types import UNSET, Response


def _get_kwargs(
    pipeline_id: str,
    table_name: str,
    *,
    client: Client,
    format_: str,
) -> Dict[str, Any]:
    url = "{}/v0/pipelines/{pipeline_id}/egress/{table_name}".format(
        client.base_url, pipeline_id=pipeline_id, table_name=table_name
    )

    headers: Dict[str, str] = client.get_headers()
    cookies: Dict[str, Any] = client.get_cookies()

    params: Dict[str, Any] = {}
    params["format"] = format_

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    return {
        "method": "get",
        "url": url,
        "headers": headers,
        "cookies": cookies,
        "timeout": client.get_timeout(),
        "follow_redirects": client.follow_redirects,
        "params": params,
    }


def _parse_response(*, client: Client, response: httpx.Response) -> Optional[Union[Chunk, ErrorResponse]]:
    if response.status_code == HTTPStatus.OK:
        response_200 = Chunk.from_dict(response.json())

        return response_200
    if response.status_code == HTTPStatus.BAD_REQUEST:
        response_400 = ErrorResponse.from_dict(response.json())

        return response_400
    if response.status_code == HTTPStatus.NOT_FOUND:
        response_404 = ErrorResponse.from_dict(response.json())

        return response_404
    if response.status_code == HTTPStatus.GONE:
        response_410 = ErrorResponse.from_dict(response.json())

        return response_410
    if response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
        response_500 = ErrorResponse.from_dict(response.json())

        return response_500
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(*, client: Client, response: httpx.Response) -> Response[Union[Chunk, ErrorResponse]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    pipeline_id: str,
    table_name: str,
    *,
    client: Client,
    format_: str,
) -> Response[Union[Chunk, ErrorResponse]]:
    """Subscribe to a stream of updates to a SQL view or table.

     Subscribe to a stream of updates to a SQL view or table.

    The pipeline responds with a continuous stream of changes to the specified
    table or view, encoded using the format specified in the `?format=` parameter.
    Updates are split into `Chunk`'s.

    The pipeline continuous sending updates until the client closes the connection or the
    pipeline is shut down.

    Args:
        pipeline_id (str):
        table_name (str):
        format_ (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Chunk, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        pipeline_id=pipeline_id,
        table_name=table_name,
        client=client,
        format_=format_,
    )

    response = httpx.request(
        verify=client.verify_ssl,
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    pipeline_id: str,
    table_name: str,
    *,
    client: Client,
    format_: str,
) -> Optional[Union[Chunk, ErrorResponse]]:
    """Subscribe to a stream of updates to a SQL view or table.

     Subscribe to a stream of updates to a SQL view or table.

    The pipeline responds with a continuous stream of changes to the specified
    table or view, encoded using the format specified in the `?format=` parameter.
    Updates are split into `Chunk`'s.

    The pipeline continuous sending updates until the client closes the connection or the
    pipeline is shut down.

    Args:
        pipeline_id (str):
        table_name (str):
        format_ (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Chunk, ErrorResponse]
    """

    return sync_detailed(
        pipeline_id=pipeline_id,
        table_name=table_name,
        client=client,
        format_=format_,
    ).parsed


async def asyncio_detailed(
    pipeline_id: str,
    table_name: str,
    *,
    client: Client,
    format_: str,
) -> Response[Union[Chunk, ErrorResponse]]:
    """Subscribe to a stream of updates to a SQL view or table.

     Subscribe to a stream of updates to a SQL view or table.

    The pipeline responds with a continuous stream of changes to the specified
    table or view, encoded using the format specified in the `?format=` parameter.
    Updates are split into `Chunk`'s.

    The pipeline continuous sending updates until the client closes the connection or the
    pipeline is shut down.

    Args:
        pipeline_id (str):
        table_name (str):
        format_ (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Chunk, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        pipeline_id=pipeline_id,
        table_name=table_name,
        client=client,
        format_=format_,
    )

    async with httpx.AsyncClient(verify=client.verify_ssl) as _client:
        response = await _client.request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    pipeline_id: str,
    table_name: str,
    *,
    client: Client,
    format_: str,
) -> Optional[Union[Chunk, ErrorResponse]]:
    """Subscribe to a stream of updates to a SQL view or table.

     Subscribe to a stream of updates to a SQL view or table.

    The pipeline responds with a continuous stream of changes to the specified
    table or view, encoded using the format specified in the `?format=` parameter.
    Updates are split into `Chunk`'s.

    The pipeline continuous sending updates until the client closes the connection or the
    pipeline is shut down.

    Args:
        pipeline_id (str):
        table_name (str):
        format_ (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Chunk, ErrorResponse]
    """

    return (
        await asyncio_detailed(
            pipeline_id=pipeline_id,
            table_name=table_name,
            client=client,
            format_=format_,
        )
    ).parsed
