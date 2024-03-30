/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * Configuration for reading data from a file with `FileInputTransport`
 */
export type FileInputConfig = {
  /**
   * Read buffer size.
   *
   * Default: when this parameter is not specified, a platform-specific
   * default is used.
   */
  buffer_size_bytes?: number | null
  /**
   * Enable file following.
   *
   * When `false`, the endpoint outputs an `InputConsumer::eoi`
   * message and stops upon reaching the end of file.  When `true`, the
   * endpoint will keep watching the file and outputting any new content
   * appended to it.
   */
  follow?: boolean
  /**
   * File path.
   */
  path: string
}
