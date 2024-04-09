/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { IntervalUnit } from './IntervalUnit'
/**
 * The available SQL types as specified in `CREATE` statements.
 */
export type SqlType =
  | 'BOOLEAN'
  | 'TINYINT'
  | 'SMALLINT'
  | 'INTEGER'
  | 'BIGINT'
  | 'REAL'
  | 'DOUBLE'
  | 'DECIMAL'
  | 'CHAR'
  | 'VARCHAR'
  | 'BINARY'
  | 'VARBINARY'
  | 'TIME'
  | 'DATE'
  | 'TIMESTAMP'
  | {
      Interval: IntervalUnit
    }
  | 'ARRAY'
  | 'STRUCT'
  | 'NULL'
