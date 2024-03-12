/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ErrorResponse } from './ErrorResponse'
import type { PipelineId } from './PipelineId'
import type { PipelineStatus } from './PipelineStatus'
/**
 * Runtime state of the pipeine.
 */
export type PipelineRuntimeState = {
  /**
   * Time when the pipeline started executing.
   */
  created: string
  current_status: PipelineStatus
  desired_status: PipelineStatus
  error?: ErrorResponse | null
  /**
   * Location where the pipeline can be reached at runtime.
   * e.g., a TCP port number or a URI.
   */
  location: string
  pipeline_id: PipelineId
  /**
   * Time when the pipeline was assigned its current status
   * of the pipeline.
   */
  status_since: string
}
