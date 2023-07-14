from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import Client
from ...models.error_response import ErrorResponse
from ...models.update_pipeline_request import UpdatePipelineRequest
from ...models.update_pipeline_response import UpdatePipelineResponse
from ...types import Response


def _get_kwargs(
    *,
    client: Client,
    json_body: UpdatePipelineRequest,
) -> Dict[str, Any]:
    url = "{}/pipelines".format(client.base_url)

    headers: Dict[str, str] = client.get_headers()
    cookies: Dict[str, Any] = client.get_cookies()

    json_json_body = json_body.to_dict()

    return {
        "method": "patch",
        "url": url,
        "headers": headers,
        "cookies": cookies,
        "timeout": client.get_timeout(),
        "follow_redirects": client.follow_redirects,
        "json": json_json_body,
    }


def _parse_response(
    *, client: Client, response: httpx.Response
) -> Optional[Union[ErrorResponse, UpdatePipelineResponse]]:
    if response.status_code == HTTPStatus.OK:
        response_200 = UpdatePipelineResponse.from_dict(response.json())

        return response_200
    if response.status_code == HTTPStatus.NOT_FOUND:
        response_404 = ErrorResponse.from_dict(response.json())

        return response_404
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Client, response: httpx.Response
) -> Response[Union[ErrorResponse, UpdatePipelineResponse]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Client,
    json_body: UpdatePipelineRequest,
) -> Response[Union[ErrorResponse, UpdatePipelineResponse]]:
    """Update existing pipeline configuration.

     Update existing pipeline configuration.

    Updates pipeline configuration. On success, increments pipeline version by 1.

    Args:
        json_body (UpdatePipelineRequest): Request to update an existing program configuration.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, UpdatePipelineResponse]]
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
    json_body: UpdatePipelineRequest,
) -> Optional[Union[ErrorResponse, UpdatePipelineResponse]]:
    """Update existing pipeline configuration.

     Update existing pipeline configuration.

    Updates pipeline configuration. On success, increments pipeline version by 1.

    Args:
        json_body (UpdatePipelineRequest): Request to update an existing program configuration.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, UpdatePipelineResponse]
    """

    return sync_detailed(
        client=client,
        json_body=json_body,
    ).parsed


async def asyncio_detailed(
    *,
    client: Client,
    json_body: UpdatePipelineRequest,
) -> Response[Union[ErrorResponse, UpdatePipelineResponse]]:
    """Update existing pipeline configuration.

     Update existing pipeline configuration.

    Updates pipeline configuration. On success, increments pipeline version by 1.

    Args:
        json_body (UpdatePipelineRequest): Request to update an existing program configuration.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, UpdatePipelineResponse]]
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
    json_body: UpdatePipelineRequest,
) -> Optional[Union[ErrorResponse, UpdatePipelineResponse]]:
    """Update existing pipeline configuration.

     Update existing pipeline configuration.

    Updates pipeline configuration. On success, increments pipeline version by 1.

    Args:
        json_body (UpdatePipelineRequest): Request to update an existing program configuration.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, UpdatePipelineResponse]
    """

    return (
        await asyncio_detailed(
            client=client,
            json_body=json_body,
        )
    ).parsed
