/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ConnectorId } from './ConnectorId'
import type { Direction } from './Direction'

/**
 * Format to add attached connectors during a config update.
 */
export type AttachedConnector = {
  /**
   * The YAML config for this attached connector.
   */
  config: string
  connector_id: ConnectorId
  direction: Direction
  /**
   * A unique identifier for this attachement.
   */
  uuid: string
}
