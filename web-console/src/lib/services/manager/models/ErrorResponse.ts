/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * Information returned by REST API endpoints on error.
 */
export type ErrorResponse = {
  /**
   * Detailed error metadata.
   * The contents of this field is determined by `error_code`.
   */
  details: Record<string, any>
  /**
   * Error code is a string that specifies this error type.
   */
  error_code: string
  /**
   * Human-readable error message.
   */
  message: string
}
