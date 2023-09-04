/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

/**
 * Update program request.
 */
export type UpdateProgramRequest = {
  /**
   * New SQL code for the program or `None` to keep existing program
   * code unmodified.
   */
  code?: string | null
  /**
   * New description for the program.
   */
  description?: string
  /**
   * New name for the program.
   */
  name: string
}
