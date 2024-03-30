/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * A SQL compiler error.
 *
 * The SQL compiler returns a list of errors in the following JSON format if
 * it's invoked with the `-je` option.
 *
 * ```ignore
 * [ {
 * "startLineNumber" : 14,
 * "startColumn" : 13,
 * "endLineNumber" : 14,
 * "endColumn" : 13,
 * "warning" : false,
 * "errorType" : "Error parsing SQL",
 * "message" : "Encountered \"<EOF>\" at line 14, column 13."
 * } ]
 * ```
 */
export type SqlCompilerMessage = {
  endColumn: number
  endLineNumber: number
  errorType: string
  message: string
  startColumn: number
  startLineNumber: number
  warning: boolean
}
