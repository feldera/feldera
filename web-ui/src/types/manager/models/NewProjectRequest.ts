/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

/**
 * Request to create a new DBSP project.
 */
export type NewProjectRequest = {
  /**
   * SQL code of the project.
   */
  code: string
  /**
   * Project description.
   */
  description: string
  /**
   * Project name.
   */
  name: string
  /**
   * Overwrite existing project with the same name, if any.
   */
  overwrite_existing?: boolean
}
