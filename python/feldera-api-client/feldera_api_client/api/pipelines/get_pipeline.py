from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error_response import ErrorResponse
from ...models.pipeline import Pipeline
from ...types import Response


def _get_kwargs(
    pipeline_name: str,
) -> Dict[str, Any]:
    pass

    return {
        "method": "get",
        "url": "/v0/pipelines/{pipeline_name}".format(
            pipeline_name=pipeline_name,
        ),
    }


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[ErrorResponse, Pipeline]]:
    if response.status_code == HTTPStatus.OK:
        response_200 = Pipeline.from_dict(response.json())

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
) -> Response[Union[ErrorResponse, Pipeline]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    pipeline_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[Union[ErrorResponse, Pipeline]]:
    """Fetch a pipeline by ID.

     Fetch a pipeline by ID.

    Args:
        pipeline_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, Pipeline]]
    """

    kwargs = _get_kwargs(
        pipeline_name=pipeline_name,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    pipeline_name: str,
    *,
    client: AuthenticatedClient,
) -> Optional[Union[ErrorResponse, Pipeline]]:
    """Fetch a pipeline by ID.

     Fetch a pipeline by ID.

    Args:
        pipeline_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, Pipeline]
    """

    return sync_detailed(
        pipeline_name=pipeline_name,
        client=client,
    ).parsed


async def asyncio_detailed(
    pipeline_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[Union[ErrorResponse, Pipeline]]:
    """Fetch a pipeline by ID.

     Fetch a pipeline by ID.

    Args:
        pipeline_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, Pipeline]]
    """

    kwargs = _get_kwargs(
        pipeline_name=pipeline_name,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    pipeline_name: str,
    *,
    client: AuthenticatedClient,
) -> Optional[Union[ErrorResponse, Pipeline]]:
    """Fetch a pipeline by ID.

     Fetch a pipeline by ID.

    Args:
        pipeline_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, Pipeline]
    """

    return (
        await asyncio_detailed(
            pipeline_name=pipeline_name,
            client=client,
        )
    ).parsed
