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
   * Program name.
   */
  name: string
}
