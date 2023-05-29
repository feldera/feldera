/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { AttachedConnector } from './AttachedConnector'
import type { PipelineId } from './PipelineId'
import type { PipelineStatus } from './PipelineStatus'
import type { ProgramId } from './ProgramId'
import type { Version } from './Version'

/**
 * Pipeline descriptor.
 */
export type PipelineDescr = {
  attached_connectors: Array<AttachedConnector>
  config: string
  created?: string
  description: string
  name: string
  pipeline_id: PipelineId
  port: number
  program_id?: ProgramId
  status: PipelineStatus
  version: Version
}
