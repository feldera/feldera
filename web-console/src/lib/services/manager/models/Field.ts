/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ColumnType } from './ColumnType'

/**
 * A SQL field.
 *
 * Matches the Calcite JSON format.
 */
export type Field = {
  case_sensitive?: boolean
  columntype: ColumnType
  name: string
}
