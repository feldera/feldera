/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * Data format specification used to parse raw data received from the
 * endpoint or to encode data sent to the endpoint.
 */
export type FormatConfig = {
  /**
   * Format-specific parser or encoder configuration.
   */
  config?: Record<string, any>
  /**
   * Format name, e.g., "csv", "json", "bincode", etc.
   */
  name: string
}
