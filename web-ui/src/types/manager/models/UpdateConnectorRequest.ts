/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ConnectorConfig } from './ConnectorConfig'

/**
 * Request to update an existing data-connector.
 */
export type UpdateConnectorRequest = {
  config?: ConnectorConfig | null
  /**
   * New connector description.
   */
  description: string
  /**
   * New connector name.
   */
  name: string
}
