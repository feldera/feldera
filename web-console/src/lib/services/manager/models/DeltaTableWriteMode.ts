/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * Delta table write mode.
 *
 * Determines how the Delta table connector handles an existing table at the target location.
 */
export enum DeltaTableWriteMode {
  APPEND = 'append',
  TRUNCATE = 'truncate',
  ERROR_IF_EXISTS = 'error_if_exists'
}
