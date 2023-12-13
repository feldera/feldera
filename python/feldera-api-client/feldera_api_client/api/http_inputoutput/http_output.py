from http import HTTPStatus
from typing import Any, Dict, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.chunk import Chunk
from ...models.egress_mode import EgressMode
from ...models.error_response import ErrorResponse
from ...models.neighborhood_query import NeighborhoodQuery
from ...models.output_query import OutputQuery
from ...types import UNSET, Response, Unset


def _get_kwargs(
    pipeline_id: str,
    table_name: str,
    *,
    json_body: Optional[NeighborhoodQuery],
    format_: str,
    query: Union[Unset, None, OutputQuery] = UNSET,
    mode: Union[Unset, None, EgressMode] = UNSET,
    quantiles: Union[Unset, None, int] = UNSET,
    array: Union[Unset, None, bool] = UNSET,
) -> Dict[str, Any]:
    pass

    params: Dict[str, Any] = {}
    params["format"] = format_

    json_query: Union[Unset, None, str] = UNSET
    if not isinstance(query, Unset):
        json_query = query.value if query else None

    params["query"] = json_query

    json_mode: Union[Unset, None, str] = UNSET
    if not isinstance(mode, Unset):
        json_mode = mode.value if mode else None

    params["mode"] = json_mode

    params["quantiles"] = quantiles

    params["array"] = array

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    json_json_body = json_body.to_dict() if json_body else None

    return {
        "method": "post",
        "url": "/v0/pipelines/{pipeline_id}/egress/{table_name}".format(
            pipeline_id=pipeline_id,
            table_name=table_name,
        ),
        "json": json_json_body,
        "params": params,
    }


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[Chunk, ErrorResponse]]:
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


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[Chunk, ErrorResponse]]:
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
    client: AuthenticatedClient,
    json_body: Optional[NeighborhoodQuery],
    format_: str,
    query: Union[Unset, None, OutputQuery] = UNSET,
    mode: Union[Unset, None, EgressMode] = UNSET,
    quantiles: Union[Unset, None, int] = UNSET,
    array: Union[Unset, None, bool] = UNSET,
) -> Response[Union[Chunk, ErrorResponse]]:
    """Subscribe to a stream of updates from a SQL view or table.

     Subscribe to a stream of updates from a SQL view or table.

    The pipeline responds with a continuous stream of changes to the specified
    table or view, encoded using the format specified in the `?format=`
    parameter. Updates are split into `Chunk`s.

    The pipeline continues sending updates until the client closes the
    connection or the pipeline is shut down.

    This API is a POST instead of a GET, because when performing neighborhood
    queries (query='neighborhood'), the call expects a request body which
    contains, among other things, a full row to execute a neighborhood search
    around. A row can be quite large and is not appropriate as a query
    parameter.

    Args:
        pipeline_id (str):
        table_name (str):
        format_ (str):
        query (Union[Unset, None, OutputQuery]): A query over an output stream.

            We currently do not support ad hoc queries.  Instead the client can use
            three pre-defined queries to inspect the contents of a table or view.
        mode (Union[Unset, None, EgressMode]):
        quantiles (Union[Unset, None, int]):
        array (Union[Unset, None, bool]):
        json_body (Optional[NeighborhoodQuery]): A request to output a specific neighborhood of a
            table or view.
            The neighborhood is defined in terms of its central point (`anchor`)
            and the number of rows preceding and following the anchor to output.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Chunk, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        pipeline_id=pipeline_id,
        table_name=table_name,
        json_body=json_body,
        format_=format_,
        query=query,
        mode=mode,
        quantiles=quantiles,
        array=array,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    pipeline_id: str,
    table_name: str,
    *,
    client: AuthenticatedClient,
    json_body: Optional[NeighborhoodQuery],
    format_: str,
    query: Union[Unset, None, OutputQuery] = UNSET,
    mode: Union[Unset, None, EgressMode] = UNSET,
    quantiles: Union[Unset, None, int] = UNSET,
    array: Union[Unset, None, bool] = UNSET,
) -> Optional[Union[Chunk, ErrorResponse]]:
    """Subscribe to a stream of updates from a SQL view or table.

     Subscribe to a stream of updates from a SQL view or table.

    The pipeline responds with a continuous stream of changes to the specified
    table or view, encoded using the format specified in the `?format=`
    parameter. Updates are split into `Chunk`s.

    The pipeline continues sending updates until the client closes the
    connection or the pipeline is shut down.

    This API is a POST instead of a GET, because when performing neighborhood
    queries (query='neighborhood'), the call expects a request body which
    contains, among other things, a full row to execute a neighborhood search
    around. A row can be quite large and is not appropriate as a query
    parameter.

    Args:
        pipeline_id (str):
        table_name (str):
        format_ (str):
        query (Union[Unset, None, OutputQuery]): A query over an output stream.

            We currently do not support ad hoc queries.  Instead the client can use
            three pre-defined queries to inspect the contents of a table or view.
        mode (Union[Unset, None, EgressMode]):
        quantiles (Union[Unset, None, int]):
        array (Union[Unset, None, bool]):
        json_body (Optional[NeighborhoodQuery]): A request to output a specific neighborhood of a
            table or view.
            The neighborhood is defined in terms of its central point (`anchor`)
            and the number of rows preceding and following the anchor to output.

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
        json_body=json_body,
        format_=format_,
        query=query,
        mode=mode,
        quantiles=quantiles,
        array=array,
    ).parsed


async def asyncio_detailed(
    pipeline_id: str,
    table_name: str,
    *,
    client: AuthenticatedClient,
    json_body: Optional[NeighborhoodQuery],
    format_: str,
    query: Union[Unset, None, OutputQuery] = UNSET,
    mode: Union[Unset, None, EgressMode] = UNSET,
    quantiles: Union[Unset, None, int] = UNSET,
    array: Union[Unset, None, bool] = UNSET,
) -> Response[Union[Chunk, ErrorResponse]]:
    """Subscribe to a stream of updates from a SQL view or table.

     Subscribe to a stream of updates from a SQL view or table.

    The pipeline responds with a continuous stream of changes to the specified
    table or view, encoded using the format specified in the `?format=`
    parameter. Updates are split into `Chunk`s.

    The pipeline continues sending updates until the client closes the
    connection or the pipeline is shut down.

    This API is a POST instead of a GET, because when performing neighborhood
    queries (query='neighborhood'), the call expects a request body which
    contains, among other things, a full row to execute a neighborhood search
    around. A row can be quite large and is not appropriate as a query
    parameter.

    Args:
        pipeline_id (str):
        table_name (str):
        format_ (str):
        query (Union[Unset, None, OutputQuery]): A query over an output stream.

            We currently do not support ad hoc queries.  Instead the client can use
            three pre-defined queries to inspect the contents of a table or view.
        mode (Union[Unset, None, EgressMode]):
        quantiles (Union[Unset, None, int]):
        array (Union[Unset, None, bool]):
        json_body (Optional[NeighborhoodQuery]): A request to output a specific neighborhood of a
            table or view.
            The neighborhood is defined in terms of its central point (`anchor`)
            and the number of rows preceding and following the anchor to output.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Chunk, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        pipeline_id=pipeline_id,
        table_name=table_name,
        json_body=json_body,
        format_=format_,
        query=query,
        mode=mode,
        quantiles=quantiles,
        array=array,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    pipeline_id: str,
    table_name: str,
    *,
    client: AuthenticatedClient,
    json_body: Optional[NeighborhoodQuery],
    format_: str,
    query: Union[Unset, None, OutputQuery] = UNSET,
    mode: Union[Unset, None, EgressMode] = UNSET,
    quantiles: Union[Unset, None, int] = UNSET,
    array: Union[Unset, None, bool] = UNSET,
) -> Optional[Union[Chunk, ErrorResponse]]:
    """Subscribe to a stream of updates from a SQL view or table.

     Subscribe to a stream of updates from a SQL view or table.

    The pipeline responds with a continuous stream of changes to the specified
    table or view, encoded using the format specified in the `?format=`
    parameter. Updates are split into `Chunk`s.

    The pipeline continues sending updates until the client closes the
    connection or the pipeline is shut down.

    This API is a POST instead of a GET, because when performing neighborhood
    queries (query='neighborhood'), the call expects a request body which
    contains, among other things, a full row to execute a neighborhood search
    around. A row can be quite large and is not appropriate as a query
    parameter.

    Args:
        pipeline_id (str):
        table_name (str):
        format_ (str):
        query (Union[Unset, None, OutputQuery]): A query over an output stream.

            We currently do not support ad hoc queries.  Instead the client can use
            three pre-defined queries to inspect the contents of a table or view.
        mode (Union[Unset, None, EgressMode]):
        quantiles (Union[Unset, None, int]):
        array (Union[Unset, None, bool]):
        json_body (Optional[NeighborhoodQuery]): A request to output a specific neighborhood of a
            table or view.
            The neighborhood is defined in terms of its central point (`anchor`)
            and the number of rows preceding and following the anchor to output.

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
            json_body=json_body,
            format_=format_,
            query=query,
            mode=mode,
            quantiles=quantiles,
            array=array,
        )
    ).parsed
