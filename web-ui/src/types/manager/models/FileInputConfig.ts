/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

export type FileInputConfig = {
  /**
   * Read buffer size.
   *
   * Default: when this parameter is not specified, a platform-specific
   * default is used.
   */
  buffer_size_bytes?: number
  /**
   * Enable file following.
   *
   * When `false`, the endpoint outputs an [`eoi`](`InputConsumer::eoi`)
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
