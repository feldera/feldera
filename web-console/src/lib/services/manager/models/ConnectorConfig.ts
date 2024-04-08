/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { FormatConfig } from './FormatConfig'
import type { OutputBufferConfig } from './OutputBufferConfig'
import type { TransportConfig } from './TransportConfig'
/**
 * A data connector's configuration
 */
export type ConnectorConfig = OutputBufferConfig & {
  format: FormatConfig
  /**
   * Backpressure threshold.
   *
   * Maximal number of records queued by the endpoint before the endpoint
   * is paused by the backpressure mechanism.
   *
   * For input endpoints, this setting bounds the number of records that have
   * been received from the input transport but haven't yet been consumed by
   * the circuit since the circuit, since the circuit is still busy processing
   * previous inputs.
   *
   * For output endpoints, this setting bounds the number of records that have
   * been produced by the circuit but not yet sent via the output transport endpoint
   * nor stored in the output buffer (see `enable_output_buffer`).
   *
   * Note that this is not a hard bound: there can be a small delay between
   * the backpressure mechanism is triggered and the endpoint is paused, during
   * which more data may be queued.
   *
   * The default is 1 million.
   */
  max_queued_records?: number
  transport: TransportConfig
}
