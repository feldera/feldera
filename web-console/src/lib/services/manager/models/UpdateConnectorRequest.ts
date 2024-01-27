/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ConnectorConfig } from './ConnectorConfig'

/**
 * Request to update an existing connector.
 */
export type UpdateConnectorRequest = {
  config?: ConnectorConfig | null
  /**
   * New connector description. If absent, existing name will be kept
   * unmodified.
   */
  description?: string | null
  /**
   * New connector name. If absent, existing name will be kept unmodified.
   */
  name?: string | null
}
