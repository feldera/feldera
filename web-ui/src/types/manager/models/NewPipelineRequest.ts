/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { AttachedConnector } from './AttachedConnector'
import type { ProgramId } from './ProgramId'

/**
 * Request to create a new program configuration.
 */
export type NewPipelineRequest = {
  /**
   * YAML code for the config.
   */
  config: string
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
