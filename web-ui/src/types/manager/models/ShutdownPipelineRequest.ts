/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { PipelineId } from './PipelineId'

/**
 * Request to terminate a running project pipeline.
 */
export type ShutdownPipelineRequest = {
  pipeline_id: PipelineId
}
