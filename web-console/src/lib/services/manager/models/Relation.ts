/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { Field } from './Field'
/**
 * A SQL table or view. It has a name and a list of fields.
 *
 * Matches the Calcite JSON format.
 */
export type Relation = {
  case_sensitive?: boolean
  fields: Array<Field>
  name: string
}
