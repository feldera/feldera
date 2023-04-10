/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ProjectDescr } from './ProjectDescr'

/**
 * Response to a project code request.
 */
export type ProjectCodeResponse = {
  /**
   * Project code.
   */
  code: string
  project: ProjectDescr
}
