/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ProgramConfig } from './ProgramConfig'
/**
 * Request to create a new program.
 */
export type NewProgramRequest = {
  /**
   * SQL code of the program.
   */
  code: string
  config?: ProgramConfig
  /**
   * Program description.
   */
  description: string
  /**
   * Program name.
   */
  name: string
}
