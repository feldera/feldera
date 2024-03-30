/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ConnectorConfig } from './ConnectorConfig'
/**
 * Request to create or replace a connector.
 */
export type CreateOrReplaceConnectorRequest = {
  config: ConnectorConfig
  /**
   * New connector description.
   */
  description: string
}
