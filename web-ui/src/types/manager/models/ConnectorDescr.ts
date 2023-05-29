/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ConnectorId } from './ConnectorId'

/**
 * Connector descriptor.
 */
export type ConnectorDescr = {
  config: string
  connector_id: ConnectorId
  description: string
  name: string
}
