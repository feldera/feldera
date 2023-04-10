/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ConfigId } from './ConfigId'
import type { PipelineId } from './PipelineId'

/**
 * Pipeline descriptor.
 */
export type PipelineDescr = {
  config_id: ConfigId
  created: string
  killed: boolean
  pipeline_id: PipelineId
  port: number
}
