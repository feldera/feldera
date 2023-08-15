/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { PipelineId } from './PipelineId'
import type { Version } from './Version'

/**
 * Response to a pipeline creation request.
 */
export type NewPipelineResponse = {
  pipeline_id: PipelineId
  version: Version
}
