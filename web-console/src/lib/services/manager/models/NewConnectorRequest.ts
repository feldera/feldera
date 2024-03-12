/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ConnectorConfig } from './ConnectorConfig'
/**
 * Request to create a new connector.
 */
export type NewConnectorRequest = {
  config: ConnectorConfig
  /**
   * Connector description.
   */
  description: string
  /**
   * Connector name.
   */
  name: string
}
