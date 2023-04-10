/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { FormatConfig } from './FormatConfig'
import type { TransportConfig } from './TransportConfig'

export type OutputEndpointConfig = {
  format: FormatConfig
  /**
   * Backpressure threshold.
   *
   * The default is 1 million.
   */
  max_buffered_records?: number
  /**
   * The name of the output stream of the circuit that this endpoint is
   * connected to.
   */
  stream: string
  transport: TransportConfig
}
