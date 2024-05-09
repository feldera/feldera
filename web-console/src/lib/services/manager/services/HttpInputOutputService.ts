/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { Chunk } from '../models/Chunk'
import type { EgressMode } from '../models/EgressMode'
import type { JsonUpdateFormat } from '../models/JsonUpdateFormat'
import type { NeighborhoodQuery } from '../models/NeighborhoodQuery'
import type { OutputQuery } from '../models/OutputQuery'
import type { CancelablePromise } from '../core/CancelablePromise'
import { OpenAPI } from '../core/OpenAPI'
import { request as __request } from '../core/request'
export class HttpInputOutputService {
  /**
   * Subscribe to a stream of updates from a SQL view or table.
   * The pipeline responds with a continuous stream of changes to the specified
   * table or view, encoded using the format specified in the `?format=`
   * parameter. Updates are split into `Chunk`s.
   *
   * The pipeline continues sending updates until the client closes the
   * connection or the pipeline is shut down.
   *
   * This API is a POST instead of a GET, because when performing neighborhood
   * queries (query='neighborhood'), the call expects a request body which
   * contains, among other things, a full row to execute a neighborhood search
   * around. A row can be quite large and is not appropriate as a query
   * parameter.
   * @param pipelineName Unique pipeline name
   * @param tableName SQL table name. Unquoted SQL names have to be capitalized. Quoted SQL names have to exactly match the case from the SQL program.
   * @param format Output data format, e.g., 'csv' or 'json'.
   * @param query Query to execute on the table. Must be one of 'table', 'neighborhood', or 'quantiles'. The default value is 'table'
   * @param mode Output mode. Must be one of 'watch' or 'snapshot'. The default value is 'watch'
   * @param quantiles For 'quantiles' queries: the number of quantiles to output. The default value is 100.
   * @param array Set to `true` to group updates in this stream into JSON arrays (used in conjunction with `format=json`). The default value is `false`
   * @param requestBody When the `query` parameter is set to 'neighborhood', the body of the request must contain a neighborhood specification.
   * @returns Chunk Connection to the endpoint successfully established. The body of the response contains a stream of data chunks.
   * @throws ApiError
   */
  public static httpOutput(
    pipelineName: string,
    tableName: string,
    format: string,
    query?: OutputQuery | null,
    mode?: EgressMode | null,
    quantiles?: number | null,
    array?: boolean | null,
    requestBody?: NeighborhoodQuery | null
  ): CancelablePromise<Chunk> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/v0/pipelines/{pipeline_name}/egress/{table_name}',
      path: {
        pipeline_name: pipelineName,
        table_name: tableName
      },
      query: {
        format: format,
        query: query,
        mode: mode,
        quantiles: quantiles,
        array: array
      },
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        400: `Unknown data format specified in the '?format=' argument.`,
        404: `Specified table or view does not exist.`,
        410: `Pipeline is not currently running because it has been shutdown or not yet started.`,
        500: `Request failed.`
      }
    })
  }
  /**
   * Push data to a SQL table.
   * The client sends data encoded using the format specified in the `?format=`
   * parameter as a body of the request.  The contents of the data must match
   * the SQL table schema specified in `table_name`
   *
   * The pipeline ingests data as it arrives without waiting for the end of
   * the request.  Successful HTTP response indicates that all data has been
   * ingested successfully.
   * @param pipelineName Unique pipeline name
   * @param tableName SQL table name. Unquoted SQL names have to be capitalized. Quoted SQL names have to exactly match the case from the SQL program.
   * @param force When `true`, push data to the pipeline even if the pipeline is paused. The default value is `false`
   * @param format Input data format, e.g., 'csv' or 'json'.
   * @param requestBody Contains the new input data in CSV.
   * @param array Set to `true` if updates in this stream are packaged into JSON arrays (used in conjunction with `format=json`). The default values is `false`.
   * @param updateFormat JSON data change event format (used in conjunction with `format=json`).  The default value is 'insert_delete'.
   * @returns any Data successfully delivered to the pipeline.
   * @throws ApiError
   */
  public static httpInput(
    pipelineName: string,
    tableName: string,
    force: boolean,
    format: string,
    requestBody: string,
    array?: boolean | null,
    updateFormat?: JsonUpdateFormat | null
  ): CancelablePromise<any> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/v0/pipelines/{pipeline_name}/ingress/{table_name}',
      path: {
        pipeline_name: pipelineName,
        table_name: tableName
      },
      query: {
        force: force,
        format: format,
        array: array,
        update_format: updateFormat
      },
      body: requestBody,
      mediaType: 'text/csv',
      errors: {
        400: `Error parsing input data.`,
        404: `Pipeline is not currently running because it has been shutdown or not yet started.`,
        500: `Request failed.`
      }
    })
  }
}
