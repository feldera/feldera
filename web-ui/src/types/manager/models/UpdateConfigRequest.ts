/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { AttachedConnector } from './AttachedConnector'
import type { ConfigId } from './ConfigId'
import type { ProjectId } from './ProjectId'

/**
 * Request to update an existing project configuration.
 */
export type UpdateConfigRequest = {
  /**
   * New config YAML. If absent, existing YAML will be kept unmodified.
   */
  config?: string
  config_id: ConfigId
  /**
   * Attached connectors.
   *
   * - If absent, existing connectors will be kept unmodified.
   *
   * - If present all existing connectors will be replaced with the new
   * specified list.
   *
   * The format is a list of tuples of `(connector_id, yaml_config)`.
   */
  connectors?: Array<AttachedConnector>
  /**
   * New config description.
   */
  description: string
  /**
   * New config name.
   */
  name: string
  project_id?: ProjectId
}
