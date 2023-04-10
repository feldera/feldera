/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { NewPipelineRequest } from '../models/NewPipelineRequest'
import type { NewPipelineResponse } from '../models/NewPipelineResponse'
import type { PipelineDescr } from '../models/PipelineDescr'
import type { ShutdownPipelineRequest } from '../models/ShutdownPipelineRequest'

import type { CancelablePromise } from '../core/CancelablePromise'
import { OpenAPI } from '../core/OpenAPI'
import { request as __request } from '../core/request'

export class PipelineService {
  /**
   * List pipelines.
   * List pipelines.
   * @returns PipelineDescr Project pipeline list retrieved successfully.
   * @throws ApiError
   */
  public static listPipelines(): CancelablePromise<Array<PipelineDescr>> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/pipelines'
    })
  }

  /**
   * Launch a new pipeline.
   * Launch a new pipeline.
   *
   * Create a new pipeline for the specified project and configuration.
   * This is a synchronous endpoint, which sends a response once
   * the pipeline has been initialized.
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
        400: `\`config_id\` refers to a config that does not belong to \`project_id\`.`,
        404: `Specified \`project_id\` or \`config_id\` does not exist in the database.`,
        409: `Project or config version in the request doesn't match the latest version in the database.`,
        500: `Pipeline process failed to initialize.`
      }
    })
  }

  /**
   * Terminate the execution of a pipeline.
   * Terminate the execution of a pipeline.
   *
   * Sends a termination request to the pipeline process.
   * Returns immediately, without waiting for the pipeline
   * to terminate (which can take several seconds).
   *
   * The pipeline is not deleted from the database, but its
   * `killed` flag is set to `true`.
   * @param requestBody
   * @returns string Pipeline successfully terminated.
   * @throws ApiError
   */
  public static pipelineShutdown(requestBody: ShutdownPipelineRequest): CancelablePromise<string> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/pipelines/shutdown',
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        404: `Specified \`pipeline_id\` does not exist in the database.`,
        500: `Request failed.`
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
  public static pipelineDelete(pipelineId: number): CancelablePromise<string> {
    return __request(OpenAPI, {
      method: 'DELETE',
      url: '/pipelines/{pipeline_id}',
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
   * Retrieve pipeline metadata.
   * Retrieve pipeline metadata.
   * @param pipelineId Unique pipeline identifier
   * @returns any Pipeline metadata retrieved successfully.
   * @throws ApiError
   */
  public static pipelineMetadata(pipelineId: number): CancelablePromise<any> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/pipelines/{pipeline_id}/metadata',
      path: {
        pipeline_id: pipelineId
      },
      errors: {
        400: `Specified \`pipeline_id\` is not a valid integer.`,
        404: `Specified \`pipeline_id\` does not exist in the database.`
      }
    })
  }

  /**
   * Pause pipeline.
   * Pause pipeline.
   * @param pipelineId Unique pipeline identifier
   * @returns string Pipeline paused.
   * @throws ApiError
   */
  public static pipelinePause(pipelineId: number): CancelablePromise<string> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/pipelines/{pipeline_id}/pause',
      path: {
        pipeline_id: pipelineId
      },
      errors: {
        400: `Specified \`pipeline_id\` is not a valid integer.`,
        404: `Specified \`pipeline_id\` does not exist in the database.`
      }
    })
  }

  /**
   * Start pipeline.
   * Start pipeline.
   * @param pipelineId Unique pipeline identifier
   * @returns string Pipeline started.
   * @throws ApiError
   */
  public static pipelineStart(pipelineId: number): CancelablePromise<string> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/pipelines/{pipeline_id}/start',
      path: {
        pipeline_id: pipelineId
      },
      errors: {
        400: `Specified \`pipeline_id\` is not a valid integer.`,
        404: `Specified \`pipeline_id\` does not exist in the database.`
      }
    })
  }

  /**
   * Retrieve pipeline status and performance counters.
   * Retrieve pipeline status and performance counters.
   * @param pipelineId Unique pipeline identifier
   * @returns any Pipeline status retrieved successfully.
   * @throws ApiError
   */
  public static pipelineStatus(pipelineId: number): CancelablePromise<any> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/pipelines/{pipeline_id}/status',
      path: {
        pipeline_id: pipelineId
      },
      errors: {
        400: `Specified \`pipeline_id\` is not a valid integer.`,
        404: `Specified \`pipeline_id\` does not exist in the database.`
      }
    })
  }
}
