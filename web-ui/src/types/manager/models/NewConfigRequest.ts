/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ProjectId } from './ProjectId'

/**
 * Request to create a new project configuration.
 */
export type NewConfigRequest = {
  /**
   * YAML code for the config.
   */
  config: string
  /**
   * Config description.
   */
  description: string
  /**
   * Config name.
   */
  name: string
  project_id?: ProjectId
}
