/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { PipelineId } from './PipelineId'
import type { Version } from './Version'

/**
 * Response to a pipeline create or replace request.
 */
export type CreateOrReplacePipelineResponse = {
  pipeline_id: PipelineId
  version: Version
}
