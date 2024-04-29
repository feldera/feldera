/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { Field } from './Field'
import type { SqlType } from './SqlType'

/**
 * A SQL column type description.
 *
 * Matches the Calcite JSON format.
 */
export type ColumnType = {
  component?: ColumnType | null
  /**
   * The fields of the type (if available).
   *
   * For example this would specify the fields of a `CREATE TYPE` construct.
   *
   * ```sql
   * CREATE TYPE person_typ AS (
   * firstname       VARCHAR(30),
   * lastname        VARCHAR(30),
   * address         ADDRESS_TYP
   * );
   * ```
   *
   * Would lead to the following `fields` value:
   *
   * ```sql
   * [
   * ColumnType { name: "firstname, ... },
   * ColumnType { name: "lastname", ... },
   * ColumnType { name: "address", fields: [ ... ] }
   * ]
   * ```
   */
  fields?: Array<Field> | null
  /**
   * Does the type accept NULL values?
   */
  nullable: boolean
  /**
   * Precision of the type.
   *
   * # Examples
   * - `VARCHAR` sets precision to `-1`.
   * - `VARCHAR(255)` sets precision to `255`.
   * - `BIGINT`, `DATE`, `FLOAT`, `DOUBLE`, `GEOMETRY`, etc. sets precision
   * to None
   * - `TIME`, `TIMESTAMP` set precision to `0`.
   */
  precision?: number | null
  /**
   * The scale of the type.
   *
   * # Example
   * - `DECIMAL(1,2)` sets scale to `2`.
   */
  scale?: number | null
  type: SqlType
}
