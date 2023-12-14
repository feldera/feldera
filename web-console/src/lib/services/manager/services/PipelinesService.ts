/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { NewPipelineRequest } from '../models/NewPipelineRequest'
import type { NewPipelineResponse } from '../models/NewPipelineResponse'
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
      url: '/v0/pipelines',
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
      url: '/v0/pipelines',
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
      url: '/v0/pipelines/{pipeline_id}',
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
      url: '/v0/pipelines/{pipeline_id}',
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
      url: '/v0/pipelines/{pipeline_id}',
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
      url: '/v0/pipelines/{pipeline_id}/config',
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
      url: '/v0/pipelines/{pipeline_id}/deployed',
      path: {
        pipeline_id: pipelineId
      },
      errors: {
        404: `Specified \`pipeline_id\` does not exist.`
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
      url: '/v0/pipelines/{pipeline_id}/validate',
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
   * it to the pipeline. The requested status change completes asynchronously.
   * On success, the pipeline enters the requested desired state.  On error, the
   * pipeline transitions to the `Failed` state. The user
   * can monitor the current status of the pipeline by polling the `GET
   * /pipeline` endpoint.
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
      url: '/v0/pipelines/{pipeline_id}/{action}',
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
