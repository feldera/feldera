/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ConnectorConfig } from './ConnectorConfig'
/**
 * Describes an input connector configuration
 */
export type InputEndpointConfig = ConnectorConfig & {
  /**
   * The name of the input stream of the circuit that this endpoint is
   * connected to.
   */
  stream: string
}
