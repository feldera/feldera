/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * Configuration for reading data from an HTTP or HTTPS URL with
 * `UrlInputTransport`.
 */
export type UrlInputConfig = {
  /**
   * URL.
   */
  path: string
  /**
   * Timeout before disconnection when paused.
   *
   * If the pipeline is paused, or if the input adapter reads data faster
   * than the pipeline can process it, then the controller will pause the
   * input adapter. If the input adapter stays paused longer than this
   * timeout, it will drop the network connection to the server. It will
   * automatically reconnect when the input adapter starts running again.
   */
  pause_timeout?: number
}
