from http import HTTPStatus
from typing import Any, Dict, Optional, Union, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error_response import ErrorResponse
from ...models.json_update_format import JsonUpdateFormat
from ...types import UNSET, Response, Unset


def _get_kwargs(
    pipeline_id: str,
    table_name: str,
    *,
    force: bool,
    format_: str,
    array: Union[Unset, None, bool] = UNSET,
    update_format: Union[Unset, None, JsonUpdateFormat] = UNSET,
) -> Dict[str, Any]:
    pass

    params: Dict[str, Any] = {}
    params["force"] = force

    params["format"] = format_

    params["array"] = array

    json_update_format: Union[Unset, None, str] = UNSET
    if not isinstance(update_format, Unset):
        json_update_format = update_format.value if update_format else None

    params["update_format"] = json_update_format

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
    client: AuthenticatedClient,
    force: bool,
    format_: str,
    array: Union[Unset, None, bool] = UNSET,
    update_format: Union[Unset, None, JsonUpdateFormat] = UNSET,
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
        force (bool):
        format_ (str):
        array (Union[Unset, None, bool]):
        update_format (Union[Unset, None, JsonUpdateFormat]): Supported JSON data change event
            formats.

            Each element in a JSON-formatted input stream specifies
            an update to one or more records in an input table.  We support
            several different ways to represent such updates.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        pipeline_id=pipeline_id,
        table_name=table_name,
        force=force,
        format_=format_,
        array=array,
        update_format=update_format,
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
    force: bool,
    format_: str,
    array: Union[Unset, None, bool] = UNSET,
    update_format: Union[Unset, None, JsonUpdateFormat] = UNSET,
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
        force (bool):
        format_ (str):
        array (Union[Unset, None, bool]):
        update_format (Union[Unset, None, JsonUpdateFormat]): Supported JSON data change event
            formats.

            Each element in a JSON-formatted input stream specifies
            an update to one or more records in an input table.  We support
            several different ways to represent such updates.

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
        force=force,
        format_=format_,
        array=array,
        update_format=update_format,
    ).parsed


async def asyncio_detailed(
    pipeline_id: str,
    table_name: str,
    *,
    client: AuthenticatedClient,
    force: bool,
    format_: str,
    array: Union[Unset, None, bool] = UNSET,
    update_format: Union[Unset, None, JsonUpdateFormat] = UNSET,
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
        force (bool):
        format_ (str):
        array (Union[Unset, None, bool]):
        update_format (Union[Unset, None, JsonUpdateFormat]): Supported JSON data change event
            formats.

            Each element in a JSON-formatted input stream specifies
            an update to one or more records in an input table.  We support
            several different ways to represent such updates.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, ErrorResponse]]
    """

    kwargs = _get_kwargs(
        pipeline_id=pipeline_id,
        table_name=table_name,
        force=force,
        format_=format_,
        array=array,
        update_format=update_format,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    pipeline_id: str,
    table_name: str,
    *,
    client: AuthenticatedClient,
    force: bool,
    format_: str,
    array: Union[Unset, None, bool] = UNSET,
    update_format: Union[Unset, None, JsonUpdateFormat] = UNSET,
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
        force (bool):
        format_ (str):
        array (Union[Unset, None, bool]):
        update_format (Union[Unset, None, JsonUpdateFormat]): Supported JSON data change event
            formats.

            Each element in a JSON-formatted input stream specifies
            an update to one or more records in an input table.  We support
            several different ways to represent such updates.

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
            force=force,
            format_=format_,
            array=array,
            update_format=update_format,
        )
    ).parsed
