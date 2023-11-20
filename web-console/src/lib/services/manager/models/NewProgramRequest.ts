/* generated using openapi-typescript-codegen -- do not edit */
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
   * Compile the program in JIT mode.
   */
  jit_mode?: boolean
  /**
   * Program name.
   */
  name: string
}
