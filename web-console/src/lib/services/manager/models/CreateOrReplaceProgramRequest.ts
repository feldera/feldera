/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ProgramConfig } from './ProgramConfig'
/**
 * Request to create or replace a program.
 */
export type CreateOrReplaceProgramRequest = {
  /**
   * SQL code of the program.
   */
  code: string
  config?: ProgramConfig
  /**
   * Program description.
   */
  description: string
}
