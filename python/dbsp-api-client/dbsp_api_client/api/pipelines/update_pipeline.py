from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error_response import ErrorResponse
from ...models.update_pipeline_request import UpdatePipelineRequest
from ...models.update_pipeline_response import UpdatePipelineResponse
from ...types import Response


def _get_kwargs(
    pipeline_id: str,
    *,
    json_body: UpdatePipelineRequest,
) -> Dict[str, Any]:
    pass

    json_json_body = json_body.to_dict()

    return {
        "method": "patch",
        "url": "/pipelines/{pipeline_id}".format(
            pipeline_id=pipeline_id,
        ),
        "json": json_json_body,
    }


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
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
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[ErrorResponse, UpdatePipelineResponse]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    pipeline_id: str,
    *,
    client: Union[AuthenticatedClient, Client],
    json_body: UpdatePipelineRequest,
) -> Response[Union[ErrorResponse, UpdatePipelineResponse]]:
    """Change a pipeline's name, description, code, configuration, or connectors.

     Change a pipeline's name, description, code, configuration, or connectors.
    On success, increments the pipeline's version by 1.

    Args:
        pipeline_id (str):
        json_body (UpdatePipelineRequest): Request to update an existing pipeline.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, UpdatePipelineResponse]]
    """

    kwargs = _get_kwargs(
        pipeline_id=pipeline_id,
        json_body=json_body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    pipeline_id: str,
    *,
    client: Union[AuthenticatedClient, Client],
    json_body: UpdatePipelineRequest,
) -> Optional[Union[ErrorResponse, UpdatePipelineResponse]]:
    """Change a pipeline's name, description, code, configuration, or connectors.

     Change a pipeline's name, description, code, configuration, or connectors.
    On success, increments the pipeline's version by 1.

    Args:
        pipeline_id (str):
        json_body (UpdatePipelineRequest): Request to update an existing pipeline.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, UpdatePipelineResponse]
    """

    return sync_detailed(
        pipeline_id=pipeline_id,
        client=client,
        json_body=json_body,
    ).parsed


async def asyncio_detailed(
    pipeline_id: str,
    *,
    client: Union[AuthenticatedClient, Client],
    json_body: UpdatePipelineRequest,
) -> Response[Union[ErrorResponse, UpdatePipelineResponse]]:
    """Change a pipeline's name, description, code, configuration, or connectors.

     Change a pipeline's name, description, code, configuration, or connectors.
    On success, increments the pipeline's version by 1.

    Args:
        pipeline_id (str):
        json_body (UpdatePipelineRequest): Request to update an existing pipeline.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, UpdatePipelineResponse]]
    """

    kwargs = _get_kwargs(
        pipeline_id=pipeline_id,
        json_body=json_body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    pipeline_id: str,
    *,
    client: Union[AuthenticatedClient, Client],
    json_body: UpdatePipelineRequest,
) -> Optional[Union[ErrorResponse, UpdatePipelineResponse]]:
    """Change a pipeline's name, description, code, configuration, or connectors.

     Change a pipeline's name, description, code, configuration, or connectors.
    On success, increments the pipeline's version by 1.

    Args:
        pipeline_id (str):
        json_body (UpdatePipelineRequest): Request to update an existing pipeline.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, UpdatePipelineResponse]
    """

    return (
        await asyncio_detailed(
            pipeline_id=pipeline_id,
            client=client,
            json_body=json_body,
        )
    ).parsed
