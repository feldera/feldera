/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * A set of updates to a SQL table or view.
 *
 * The `sequence_number` field stores the offset of the chunk relative to the
 * start of the stream and can be used to implement reliable delivery.
 * The payload is stored in the `bin_data`, `text_data`, or `json_data` field
 * depending on the data format used.
 */
export type Chunk = {
  /**
   * Base64 encoded binary payload, e.g., bincode.
   */
  bin_data?: Blob | null
  /**
   * JSON payload.
   */
  json_data?: Record<string, any> | null
  sequence_number: number
  /**
   * Text payload, e.g., CSV.
   */
  text_data?: string | null
}
