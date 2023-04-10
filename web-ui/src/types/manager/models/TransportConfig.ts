/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

/**
 * Transport endpoint configuration.
 */
export type TransportConfig = {
  /**
   * Transport-specific endpoint configuration passed to
   * [`OutputTransport::new_endpoint`](`crate::OutputTransport::new_endpoint`)
   * and
   * [`InputTransport::new_endpoint`](`crate::InputTransport::new_endpoint`).
   */
  config?: any
  /**
   * Data transport name, e.g., "file", "kafka", "kinesis", etc.
   */
  name: string
}
