from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error_response import ErrorResponse
from ...models.new_pipeline_request import NewPipelineRequest
from ...models.new_pipeline_response import NewPipelineResponse
from ...types import Response


def _get_kwargs(
    *,
    json_body: NewPipelineRequest,
) -> Dict[str, Any]:
    pass

    json_json_body = json_body.to_dict()

    return {
        "method": "post",
        "url": "/v0/pipelines",
        "json": json_json_body,
    }


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[ErrorResponse, NewPipelineResponse]]:
    if response.status_code == HTTPStatus.OK:
        response_200 = NewPipelineResponse.from_dict(response.json())

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
) -> Response[Union[ErrorResponse, NewPipelineResponse]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    json_body: NewPipelineRequest,
) -> Response[Union[ErrorResponse, NewPipelineResponse]]:
    """Create a new pipeline.

     Create a new pipeline.

    Args:
        json_body (NewPipelineRequest): Request to create a new pipeline.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, NewPipelineResponse]]
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
    json_body: NewPipelineRequest,
) -> Optional[Union[ErrorResponse, NewPipelineResponse]]:
    """Create a new pipeline.

     Create a new pipeline.

    Args:
        json_body (NewPipelineRequest): Request to create a new pipeline.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, NewPipelineResponse]
    """

    return sync_detailed(
        client=client,
        json_body=json_body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    json_body: NewPipelineRequest,
) -> Response[Union[ErrorResponse, NewPipelineResponse]]:
    """Create a new pipeline.

     Create a new pipeline.

    Args:
        json_body (NewPipelineRequest): Request to create a new pipeline.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, NewPipelineResponse]]
    """

    kwargs = _get_kwargs(
        json_body=json_body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    json_body: NewPipelineRequest,
) -> Optional[Union[ErrorResponse, NewPipelineResponse]]:
    """Create a new pipeline.

     Create a new pipeline.

    Args:
        json_body (NewPipelineRequest): Request to create a new pipeline.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, NewPipelineResponse]
    """

    return (
        await asyncio_detailed(
            client=client,
            json_body=json_body,
        )
    ).parsed
