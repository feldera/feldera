/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ConnectorId } from './ConnectorId'

/**
 * Format to add attached connectors during a config update.
 */
export type AttachedConnector = {
  connector_id: ConnectorId
  /**
   * Is this an input or an output?
   */
  is_input: boolean
  /**
   * A unique identifier for this attachement.
   */
  name: string
  /**
   * The table or view this connector is attached to.
   */
  relation_name: string
}
