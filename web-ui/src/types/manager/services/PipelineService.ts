/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { NewPipelineRequest } from '../models/NewPipelineRequest'
import type { NewPipelineResponse } from '../models/NewPipelineResponse'
import type { PipelineDescr } from '../models/PipelineDescr'
import type { UpdatePipelineRequest } from '../models/UpdatePipelineRequest'
import type { UpdatePipelineResponse } from '../models/UpdatePipelineResponse'

import type { CancelablePromise } from '../core/CancelablePromise'
import { OpenAPI } from '../core/OpenAPI'
import { request as __request } from '../core/request'

export class PipelineService {
  /**
   * Retrieve pipeline metadata.
   * Retrieve pipeline metadata.
   * @param id Unique pipeline identifier
   * @param name Unique pipeline name
   * @returns PipelineDescr Pipeline descriptor retrieved successfully.
   * @throws ApiError
   */
  public static pipelineStatus(id?: string, name?: string): CancelablePromise<PipelineDescr> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/v0/pipeline',
      query: {
        id: id,
        name: name
      },
      errors: {
        400: `Specified \`pipeline_id\` is not a valid uuid.`,
        404: `Specified \`pipeline_id\` does not exist in the database.`
      }
    })
  }

  /**
   * List pipelines.
   * List pipelines.
   * @returns PipelineDescr Pipeline list retrieved successfully.
   * @throws ApiError
   */
  public static listPipelines(): CancelablePromise<Array<PipelineDescr>> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/v0/pipelines'
    })
  }

  /**
   * Create a new program configuration.
   * Create a new program configuration.
   * @param requestBody
   * @returns NewPipelineResponse Configuration successfully created.
   * @throws ApiError
   */
  public static newPipeline(requestBody: NewPipelineRequest): CancelablePromise<NewPipelineResponse> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/v0/pipelines',
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        404: `Specified \`program_id\` does not exist in the database.`
      }
    })
  }

  /**
   * Update existing program configuration.
   * Update existing program configuration.
   *
   * Updates program config name, description and code and, optionally, config
   * and connectors. On success, increments config version by 1.
   * @param requestBody
   * @returns UpdatePipelineResponse Configuration successfully updated.
   * @throws ApiError
   */
  public static updatePipeline(requestBody: UpdatePipelineRequest): CancelablePromise<UpdatePipelineResponse> {
    return __request(OpenAPI, {
      method: 'PATCH',
      url: '/v0/pipelines',
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        404: `A connector ID in \`connectors\` does not exist in the database.`
      }
    })
  }

  /**
   * Terminate and delete a pipeline.
   * Terminate and delete a pipeline.
   *
   * Shut down the pipeline if it is still running and delete it from
   * the database.
   * @param pipelineId Unique pipeline identifier
   * @returns string Pipeline successfully deleted.
   * @throws ApiError
   */
  public static pipelineDelete(pipelineId: string): CancelablePromise<string> {
    return __request(OpenAPI, {
      method: 'DELETE',
      url: '/v0/pipelines/{pipeline_id}',
      path: {
        pipeline_id: pipelineId
      },
      errors: {
        404: `Specified \`pipeline_id\` does not exist in the database.`,
        500: `Request failed.`
      }
    })
  }

  /**
   * Connect to an HTTP input/output websocket
   * Connect to an HTTP input/output websocket
   * @param pipelineId Unique pipeline identifier
   * @param connectorName Connector name
   * @returns string Pipeline successfully connected to.
   * @throws ApiError
   */
  public static httpInput(pipelineId: string, connectorName: string): CancelablePromise<string> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/v0/pipelines/{pipeline_id}/connector/{connector_name}',
      path: {
        pipeline_id: pipelineId,
        connector_name: connectorName
      },
      errors: {
        404: `Specified \`connector_name\` does not exist for the pipeline.`,
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
      url: '/v0/pipelines/{pipeline_id}/stats',
      path: {
        pipeline_id: pipelineId
      },
      errors: {
        400: `Specified \`pipeline_id\` is not a valid uuid.`,
        404: `Specified \`pipeline_id\` does not exist in the database.`
      }
    })
  }

  /**
   * Perform action on a pipeline.
   * Perform action on a pipeline.
   *
   * - 'deploy': Run a new pipeline. Deploy a pipeline for the specified program
   * and configuration. This is a synchronous endpoint, which sends a response
   * once the pipeline has been initialized.
   * - 'pause': Pause the pipeline.
   * - 'start': Resume the paused pipeline.
   * - 'shutdown': Terminate the execution of a pipeline. Sends a termination
   * request to the pipeline process. Returns immediately, without waiting for
   * the pipeline to terminate (which can take several seconds). The pipeline is
   * not deleted from the database, but its `status` is set to `shutdown`.
   * @param pipelineId Unique pipeline identifier
   * @param action Pipeline action [run, start, pause, shutdown]
   * @returns string Performed a Pipeline action.
   * @throws ApiError
   */
  public static pipelineAction(pipelineId: string, action: string): CancelablePromise<string> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/v0/pipelines/{pipeline_id}/{action}',
      path: {
        pipeline_id: pipelineId,
        action: action
      },
      errors: {
        400: `Specified \`pipeline_id\` is not a valid uuid.`,
        404: `Specified \`pipeline_id\` does not exist in the database.`
      }
    })
  }
}
