/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ProjectId } from './ProjectId'

/**
 * Update project request.
 */
export type UpdateProjectRequest = {
  /**
   * New SQL code for the project or `None` to keep existing project
   * code unmodified.
   */
  code?: string
  /**
   * New description for the project.
   */
  description?: string
  /**
   * New name for the project.
   */
  name: string
  project_id: ProjectId
}
