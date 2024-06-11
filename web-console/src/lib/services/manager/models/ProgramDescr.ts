/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ProgramConfig } from './ProgramConfig'
import type { ProgramId } from './ProgramId'
import type { ProgramSchema } from './ProgramSchema'
import type { ProgramStatus } from './ProgramStatus'
import type { Version } from './Version'
/**
 * Program descriptor.
 */
export type ProgramDescr = {
  /**
   * SQL code
   */
  code?: string | null
  config: ProgramConfig
  /**
   * Program description.
   */
  description: string
  /**
   * Program name (doesn't have to be unique).
   */
  name: string
  program_id: ProgramId
  schema?: ProgramSchema | null
  status: ProgramStatus
  version: Version
}
