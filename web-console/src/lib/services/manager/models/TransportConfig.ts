/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * Transport endpoint configuration.
 */
export type TransportConfig = {
  /**
   * Transport-specific endpoint configuration passed to
   * `crate::OutputTransport::new_endpoint`
   * and `crate::InputTransport::new_endpoint`.
   */
  config?: Record<string, any>
  /**
   * Data transport name, e.g., `file`, `kafka`, `kinesis`
   */
  name: string
}
