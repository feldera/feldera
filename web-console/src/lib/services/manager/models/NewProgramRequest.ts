/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

/**
 * Request to create a new DBSP program.
 */
export type NewProgramRequest = {
  /**
   * SQL code of the program.
   */
  code: string
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
