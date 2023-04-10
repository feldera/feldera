/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ConnectorType } from './ConnectorType'

/**
 * Request to create a new connector.
 */
export type NewConnectorRequest = {
  /**
   * connector config.
   */
  config: string
  /**
   * connector description.
   */
  description: string
  /**
   * connector name.
   */
  name: string
  typ: ConnectorType
}
