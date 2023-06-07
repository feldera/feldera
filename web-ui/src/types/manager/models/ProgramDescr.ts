/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ProgramId } from './ProgramId'
import type { ProgramStatus } from './ProgramStatus'
import type { Version } from './Version'

/**
 * Program descriptor.
 */
export type ProgramDescr = {
  /**
   * Program description.
   */
  description: string
  /**
   * Program name (doesn't have to be unique).
   */
  name: string
  program_id: ProgramId
  /**
   * A JSON description of the SQL tables and view declarations including
   * field names and types.
   *
   * The schema is set/updated whenever the `status` field reaches >=
   * `ProgramStatus::CompilingRust`.
   *
   * # Example
   *
   * The given SQL program:
   *
   * ```no_run
   * CREATE TABLE USERS ( name varchar );
   * CREATE VIEW OUTPUT_USERS as SELECT * FROM USERS;
   * ```
   *
   * Would lead the following JSON string in `schema`:
   *
   * ```no_run
   * {
   * "inputs": [{
   * "name": "USERS",
   * "fields": [{ "name": "NAME", "type": "VARCHAR", "nullable": true }]
   * }],
   * "outputs": [{
   * "name": "OUTPUT_USERS",
   * "fields": [{ "name": "NAME", "type": "VARCHAR", "nullable": true }]
   * }]
   * }
   * ```
   */
  schema?: string | null
  status: ProgramStatus
  version: Version
}
