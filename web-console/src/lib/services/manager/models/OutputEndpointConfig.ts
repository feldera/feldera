/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ConnectorConfig } from './ConnectorConfig'
/**
 * Describes an output connector configuration
 */
export type OutputEndpointConfig = ConnectorConfig & {
  /**
   * The name of the output stream of the circuit that this endpoint is
   * connected to.
   */
  stream: string
}
