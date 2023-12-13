from http import HTTPStatus
from typing import Any, Dict, Optional, Union, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error_response import ErrorResponse
from ...types import Response


def _get_kwargs(
    pipeline_id: str,
) -> Dict[str, Any]:
    pass

    return {
        "method": "get",
        "url": "/v0/pipelines/{pipeline_id}/validate".format(
            pipeline_id=pipeline_id,
        ),
    }


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[ErrorResponse, str]]:
    if response.status_code == HTTPStatus.OK:
        response_200 = cast(str, response.json())
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


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[ErrorResponse, str]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    pipeline_id: str,
    *,
    client: AuthenticatedClient,
) -> Response[Union[ErrorResponse, str]]:
    """Validate a pipeline.

     Validate a pipeline.

    Checks whether a pipeline is configured correctly. This includes
    checking whether the pipeline references a valid compiled program,
    whether the connectors reference valid tables/views in the program,
    and more.

    Args:
        pipeline_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, str]]
    """

    kwargs = _get_kwargs(
        pipeline_id=pipeline_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    pipeline_id: str,
    *,
    client: AuthenticatedClient,
) -> Optional[Union[ErrorResponse, str]]:
    """Validate a pipeline.

     Validate a pipeline.

    Checks whether a pipeline is configured correctly. This includes
    checking whether the pipeline references a valid compiled program,
    whether the connectors reference valid tables/views in the program,
    and more.

    Args:
        pipeline_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, str]
    """

    return sync_detailed(
        pipeline_id=pipeline_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    pipeline_id: str,
    *,
    client: AuthenticatedClient,
) -> Response[Union[ErrorResponse, str]]:
    """Validate a pipeline.

     Validate a pipeline.

    Checks whether a pipeline is configured correctly. This includes
    checking whether the pipeline references a valid compiled program,
    whether the connectors reference valid tables/views in the program,
    and more.

    Args:
        pipeline_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ErrorResponse, str]]
    """

    kwargs = _get_kwargs(
        pipeline_id=pipeline_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    pipeline_id: str,
    *,
    client: AuthenticatedClient,
) -> Optional[Union[ErrorResponse, str]]:
    """Validate a pipeline.

     Validate a pipeline.

    Checks whether a pipeline is configured correctly. This includes
    checking whether the pipeline references a valid compiled program,
    whether the connectors reference valid tables/views in the program,
    and more.

    Args:
        pipeline_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ErrorResponse, str]
    """

    return (
        await asyncio_detailed(
            pipeline_id=pipeline_id,
            client=client,
        )
    ).parsed
