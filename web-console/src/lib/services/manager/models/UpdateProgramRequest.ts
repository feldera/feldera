/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ProgramConfig } from './ProgramConfig'
import type { Version } from './Version'
/**
 * Request to update an existing program.
 */
export type UpdateProgramRequest = {
  /**
   * New SQL code for the program. If absent, existing program code will be
   * kept unmodified.
   */
  code?: string | null
  config?: ProgramConfig | null
  /**
   * New program description. If absent, existing description will be kept
   * unmodified.
   */
  description?: string | null
  guard?: Version | null
  /**
   * New program name. If absent, existing name will be kept unmodified.
   */
  name?: string | null
}
