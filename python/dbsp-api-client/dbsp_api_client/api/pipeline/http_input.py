from http import HTTPStatus
from typing import Any, Dict, Optional, Union, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error_response import ErrorResponse
from ...types import UNSET, Response


def _get_kwargs(
    pipeline_id: str,
    table_name: str,
    *,
    format_: str,
) -> Dict[str, Any]:
    pass

    params: Dict[str, Any] = {}
    params["format"] = format_

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    return {
        "method": "post",
        "url": "/pipelines/{pipeline_id}/ingress/{table_name}".format(
            pipeline_id=pipeline_id,
            table_name=table_name,
        ),
        "params": params,
    }


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[Any, ErrorResponse]]:
    if response.status_code == HTTPStatus.OK:
        response_200 = cast(Any, None)
        return response_200
    if response.status_code == HTTPStatus.BAD_REQUEST:
        response_400 = ErrorResponse.from_dict(response.json())

        return response_400
    if response.status_code == HTTPStatus.NOT_FOUND:
        response_404 = ErrorResponse.from_dict(response.json())

        return response_404
    if response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY:
        response_422 = ErrorResponse.from_dict(response.json())

        return response_422
    if response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
        response_500 = ErrorResponse.from_dict(response.json())

        return response_500
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
    pipeline_id: str,
    table_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    format_: str,
) -> Response[Union[Any, ErrorResponse]]:
    """Push data to a SQL table.

     Push data to a SQL table.

    The client sends data encoded using the format specified in the `?format=`
    parameter as a body of the request.  The contents of the data must match
    the SQL table schema specified in `table_name`

    The pipeline ingests data as it arrives without waiting for the end of
    the request.  Successful HTTP response indicates that all data has been
    ingested successfully.

    Args:
        pipeline_id (str):
        table_name (str):
        format_ (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        pipeline_id=pipeline_id,
        table_name=table_name,
        format_=format_,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    pipeline_id: str,
    table_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    format_: str,
) -> Optional[Union[Any, ErrorResponse]]:
    """Push data to a SQL table.

     Push data to a SQL table.

    The client sends data encoded using the format specified in the `?format=`
    parameter as a body of the request.  The contents of the data must match
    the SQL table schema specified in `table_name`

    The pipeline ingests data as it arrives without waiting for the end of
    the request.  Successful HTTP response indicates that all data has been
    ingested successfully.

    Args:
        pipeline_id (str):
        table_name (str):
        format_ (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Any, ErrorResponse]
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
    client: Union[AuthenticatedClient, Client],
    format_: str,
) -> Response[Union[Any, ErrorResponse]]:
    """Push data to a SQL table.

     Push data to a SQL table.

    The client sends data encoded using the format specified in the `?format=`
    parameter as a body of the request.  The contents of the data must match
    the SQL table schema specified in `table_name`

    The pipeline ingests data as it arrives without waiting for the end of
    the request.  Successful HTTP response indicates that all data has been
    ingested successfully.

    Args:
        pipeline_id (str):
        table_name (str):
        format_ (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        pipeline_id=pipeline_id,
        table_name=table_name,
        format_=format_,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    pipeline_id: str,
    table_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    format_: str,
) -> Optional[Union[Any, ErrorResponse]]:
    """Push data to a SQL table.

     Push data to a SQL table.

    The client sends data encoded using the format specified in the `?format=`
    parameter as a body of the request.  The contents of the data must match
    the SQL table schema specified in `table_name`

    The pipeline ingests data as it arrives without waiting for the end of
    the request.  Successful HTTP response indicates that all data has been
    ingested successfully.

    Args:
        pipeline_id (str):
        table_name (str):
        format_ (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Any, ErrorResponse]
    """

    return (
        await asyncio_detailed(
            pipeline_id=pipeline_id,
            table_name=table_name,
            client=client,
            format_=format_,
        )
    ).parsed
