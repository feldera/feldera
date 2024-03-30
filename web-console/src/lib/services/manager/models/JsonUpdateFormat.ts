/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * Supported JSON data change event formats.
 *
 * Each element in a JSON-formatted input stream specifies
 * an update to one or more records in an input table.  We support
 * several different ways to represent such updates.
 */
export enum JsonUpdateFormat {
  INSERT_DELETE = 'insert_delete',
  WEIGHTED = 'weighted',
  DEBEZIUM = 'debezium',
  SNOWFLAKE = 'snowflake',
  RAW = 'raw'
}
