/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { Chunk } from '../models/Chunk'
import type { EgressMode } from '../models/EgressMode'
import type { NeighborhoodQuery } from '../models/NeighborhoodQuery'
import type { NewPipelineRequest } from '../models/NewPipelineRequest'
import type { NewPipelineResponse } from '../models/NewPipelineResponse'
import type { OutputQuery } from '../models/OutputQuery'
import type { Pipeline } from '../models/Pipeline'
import type { PipelineConfig } from '../models/PipelineConfig'
import type { PipelineRevision } from '../models/PipelineRevision'
import type { UpdatePipelineRequest } from '../models/UpdatePipelineRequest'
import type { UpdatePipelineResponse } from '../models/UpdatePipelineResponse'

import type { CancelablePromise } from '../core/CancelablePromise'
import { OpenAPI } from '../core/OpenAPI'
import { request as __request } from '../core/request'

export class PipelinesService {
  /**
   * Fetch pipelines, optionally filtered by name or ID.
   * Fetch pipelines, optionally filtered by name or ID.
   * @param id Unique pipeline id.
   * @param name Unique pipeline name.
   * @returns Pipeline Pipeline list retrieved successfully.
   * @throws ApiError
   */
  public static listPipelines(id?: string | null, name?: string | null): CancelablePromise<Array<Pipeline>> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/pipelines',
      query: {
        id: id,
        name: name
      }
    })
  }

  /**
   * Create a new pipeline.
   * Create a new pipeline.
   * @param requestBody
   * @returns NewPipelineResponse Pipeline successfully created.
   * @throws ApiError
   */
  public static newPipeline(requestBody: NewPipelineRequest): CancelablePromise<NewPipelineResponse> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/pipelines',
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        404: `Specified program id or connector ids do not exist.`
      }
    })
  }

  /**
   * Fetch a pipeline by ID.
   * Fetch a pipeline by ID.
   * @param pipelineId Unique pipeline identifier
   * @returns Pipeline Pipeline descriptor retrieved successfully.
   * @throws ApiError
   */
  public static getPipeline(pipelineId: string): CancelablePromise<Pipeline> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/pipelines/{pipeline_id}',
      path: {
        pipeline_id: pipelineId
      },
      errors: {
        404: `Specified pipeline ID does not exist.`
      }
    })
  }

  /**
   * Delete a pipeline. The pipeline must be in the shutdown state.
   * Delete a pipeline. The pipeline must be in the shutdown state.
   * @param pipelineId Unique pipeline identifier
   * @returns any Pipeline successfully deleted.
   * @throws ApiError
   */
  public static pipelineDelete(pipelineId: string): CancelablePromise<any> {
    return __request(OpenAPI, {
      method: 'DELETE',
      url: '/pipelines/{pipeline_id}',
      path: {
        pipeline_id: pipelineId
      },
      errors: {
        400: `Pipeline ID is invalid or pipeline is already running.`,
        404: `Specified pipeline id does not exist.`
      }
    })
  }

  /**
   * Change a pipeline's name, description, code, configuration, or connectors.
   * Change a pipeline's name, description, code, configuration, or connectors.
   * On success, increments the pipeline's version by 1.
   * @param pipelineId Unique pipeline identifier
   * @param requestBody
   * @returns UpdatePipelineResponse Pipeline successfully updated.
   * @throws ApiError
   */
  public static updatePipeline(
    pipelineId: string,
    requestBody: UpdatePipelineRequest
  ): CancelablePromise<UpdatePipelineResponse> {
    return __request(OpenAPI, {
      method: 'PATCH',
      url: '/pipelines/{pipeline_id}',
      path: {
        pipeline_id: pipelineId
      },
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        404: `Specified pipeline or connector id does not exist.`
      }
    })
  }

  /**
   * Fetch a pipeline's configuration.
   * Fetch a pipeline's configuration.
   *
   * When defining a pipeline, clients have to provide an optional
   * `RuntimeConfig` for the pipelines and references to existing
   * connectors to attach to the pipeline. This endpoint retrieves
   * the *expanded* definition of the pipeline's configuration,
   * which comprises both the `RuntimeConfig` and the complete
   * definitions of the attached connectors.
   * @param pipelineId Unique pipeline identifier
   * @returns PipelineConfig Expanded pipeline configuration retrieved successfully.
   * @throws ApiError
   */
  public static getPipelineConfig(pipelineId: string): CancelablePromise<PipelineConfig> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/pipelines/{pipeline_id}/config',
      path: {
        pipeline_id: pipelineId
      },
      errors: {
        404: `Specified pipeline ID does not exist.`
      }
    })
  }

  /**
   * Return the currently deployed version of the pipeline, if any.
   * Return the currently deployed version of the pipeline, if any.
   * @param pipelineId Unique pipeline identifier
   * @returns any Last deployed version of the pipeline retrieved successfully (returns null if pipeline was never deployed yet).
   * @throws ApiError
   */
  public static pipelineDeployed(pipelineId: string): CancelablePromise<PipelineRevision | null> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/pipelines/{pipeline_id}/deployed',
      path: {
        pipeline_id: pipelineId
      },
      errors: {
        404: `Specified \`pipeline_id\` does not exist.`
      }
    })
  }

  /**
   * Subscribe to a stream of updates from a SQL view or table.
   * Subscribe to a stream of updates from a SQL view or table.
   *
   * The pipeline responds with a continuous stream of changes to the specified
   * table or view, encoded using the format specified in the `?format=`
   * parameter. Updates are split into `Chunk`'s.
   *
   * The pipeline continuous sending updates until the client closes the
   * connection or the pipeline is shut down.
   * @param pipelineId Unique pipeline identifier.
   * @param tableName SQL table or view name.
   * @param format Output data format, e.g., 'csv' or 'json'.
   * @param query Query to execute on the table. Must be one of 'table', 'neighborhood', or 'quantiles'. The default value is 'table'
   * @param mode Output mode. Must be one of 'watch' or 'snapshot'. The default value is 'watch'
   * @param quantiles For 'quantiles' queries: the number of quantiles to output. The default value is 100.
   * @param requestBody When the `query` parameter is set to 'neighborhood', the body of the request must contain a neighborhood specification.
   * @returns Chunk Connection to the endpoint successfully established. The body of the response contains a stream of data chunks.
   * @throws ApiError
   */
  public static httpOutput(
    pipelineId: string,
    tableName: string,
    format: string,
    query?: OutputQuery | null,
    mode?: EgressMode | null,
    quantiles?: number | null,
    requestBody?: NeighborhoodQuery | null
  ): CancelablePromise<Chunk> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/pipelines/{pipeline_id}/egress/{table_name}',
      path: {
        pipeline_id: pipelineId,
        table_name: tableName
      },
      query: {
        format: format,
        query: query,
        mode: mode,
        quantiles: quantiles
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
   * Push data to a SQL table.
   *
   * The client sends data encoded using the format specified in the `?format=`
   * parameter as a body of the request.  The contents of the data must match
   * the SQL table schema specified in `table_name`
   *
   * The pipeline ingests data as it arrives without waiting for the end of
   * the request.  Successful HTTP response indicates that all data has been
   * ingested successfully.
   * @param pipelineId Unique pipeline identifier.
   * @param tableName SQL table name.
   * @param format Input data format, e.g., 'csv' or 'json'.
   * @param requestBody Contains the new input data in CSV.
   * @returns any Data successfully delivered to the pipeline.
   * @throws ApiError
   */
  public static httpInput(
    pipelineId: string,
    tableName: string,
    format: string,
    requestBody: string
  ): CancelablePromise<any> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/pipelines/{pipeline_id}/ingress/{table_name}',
      path: {
        pipeline_id: pipelineId,
        table_name: tableName
      },
      query: {
        format: format
      },
      body: requestBody,
      mediaType: 'text/csv',
      errors: {
        400: `Unknown data format specified in the '?format=' argument.`,
        404: `Pipeline is not currently running because it has been shutdown or not yet started.`,
        422: `Error parsing input data.`,
        500: `Request failed.`
      }
    })
  }

  /**
   * Retrieve pipeline metrics and performance counters.
   * Retrieve pipeline metrics and performance counters.
   * @param pipelineId Unique pipeline identifier
   * @returns any Pipeline metrics retrieved successfully.
   * @throws ApiError
   */
  public static pipelineStats(pipelineId: string): CancelablePromise<Record<string, any>> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/pipelines/{pipeline_id}/stats',
      path: {
        pipeline_id: pipelineId
      },
      errors: {
        400: `Specified pipeline id is not a valid uuid.`,
        404: `Specified pipeline id does not exist.`
      }
    })
  }

  /**
   * Validate a pipeline.
   * Validate a pipeline.
   *
   * Checks whether a pipeline is configured correctly. This includes
   * checking whether the pipeline references a valid compiled program,
   * whether the connectors reference valid tables/views in the program,
   * and more.
   * @param pipelineId Unique pipeline identifier
   * @returns string Validate a Pipeline config.
   * @throws ApiError
   */
  public static pipelineValidate(pipelineId: string): CancelablePromise<string> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/pipelines/{pipeline_id}/validate',
      path: {
        pipeline_id: pipelineId
      },
      errors: {
        400: `Invalid pipeline.`,
        404: `Specified pipeline id does not exist.`
      }
    })
  }

  /**
   * Change the desired state of the pipeline.
   * Change the desired state of the pipeline.
   *
   * This endpoint allows the user to control the execution of the pipeline,
   * by changing its desired state attribute (see the discussion of the desired
   * state model in the [`PipelineStatus`] documentation).
   *
   * The endpoint returns immediately after validating the request and forwarding
   * it to the pipeline. The requested status change completes asynchronously.  On success,
   * the pipeline enters the requested desired state.  On error, the pipeline
   * transitions to the `Failed` state. The user
   * can monitor the current status of the pipeline by polling the `GET /pipeline`
   * endpoint.
   *
   * The following values of the `action` argument are accepted by this endpoint:
   *
   * - 'start': Start processing data.
   * - 'pause': Pause the pipeline.
   * - 'shutdown': Terminate the execution of the pipeline.
   * @param pipelineId Unique pipeline identifier
   * @param action Pipeline action [start, pause, shutdown]
   * @returns any Request accepted.
   * @throws ApiError
   */
  public static pipelineAction(pipelineId: string, action: string): CancelablePromise<any> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/pipelines/{pipeline_id}/{action}',
      path: {
        pipeline_id: pipelineId,
        action: action
      },
      errors: {
        400: `Pipeline desired state is not valid.`,
        404: `Specified pipeline id does not exist.`,
        500: `Timeout waiting for the pipeline to initialize.`
      }
    })
  }
}
