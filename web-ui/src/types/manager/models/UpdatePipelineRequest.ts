/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { AttachedConnector } from './AttachedConnector'
import type { PipelineId } from './PipelineId'
import type { ProgramId } from './ProgramId'

/**
 * Request to update an existing program configuration.
 */
export type UpdatePipelineRequest = {
  /**
   * New config YAML. If absent, existing YAML will be kept unmodified.
   */
  config?: string | null
  /**
   * Attached connectors.
   *
   * - If absent, existing connectors will be kept unmodified.
   *
   * - If present all existing connectors will be replaced with the new
   * specified list.
   */
  connectors?: Array<AttachedConnector> | null
  /**
   * New config description.
   */
  description: string
  /**
   * New config name.
   */
  name: string
  pipeline_id: PipelineId
  program_id?: ProgramId | null
}
