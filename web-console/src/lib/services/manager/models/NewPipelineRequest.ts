/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { AttachedConnector } from './AttachedConnector'
import type { ProgramId } from './ProgramId'
import type { RuntimeConfig } from './RuntimeConfig'

/**
 * Request to create a new pipeline.
 */
export type NewPipelineRequest = {
  config: RuntimeConfig
  /**
   * Attached connectors.
   */
  connectors?: Array<AttachedConnector> | null
  /**
   * Config description.
   */
  description: string
  /**
   * Config name.
   */
  name: string
  program_id?: ProgramId | null
}
