/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ProgramId } from './ProgramId'
import type { Version } from './Version'

/**
 * Response to a new program request.
 */
export type NewProgramResponse = {
  program_id: ProgramId
  version: Version
}
