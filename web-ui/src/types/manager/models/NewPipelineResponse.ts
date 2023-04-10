/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { PipelineId } from './PipelineId'

/**
 * Response to a pipeline creation request.
 */
export type NewPipelineResponse = {
  pipeline_id: PipelineId
  /**
   * TCP port that the pipeline process listens on.
   */
  port: number
}
