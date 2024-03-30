/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { FormatConfig } from './FormatConfig'
import type { TransportConfig } from './TransportConfig'
/**
 * A data connector's configuration
 */
export type ConnectorConfig = {
  format: FormatConfig
  /**
   * Backpressure threshold.
   *
   * Maximal amount of records buffered by the endpoint before the endpoint
   * is paused by the backpressure mechanism.  Note that this is not a
   * hard bound: there can be a small delay between the backpressure
   * mechanism is triggered and the endpoint is paused, during which more
   * data may be received.
   *
   * The default is 1 million.
   */
  max_buffered_records?: number
  transport: TransportConfig
}
