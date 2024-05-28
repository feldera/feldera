/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * Delta table read mode.
 *
 * Three options are available:
 *
 * * `snapshot` - read a snapshot of the table and stop.
 *
 * * `follow` - continuously ingest changes to the table, starting from a specified version
 * or timestamp.
 *
 * * `snapshot_and_follow` - read a snapshot of the table before switching to continuous ingestion
 * mode.
 */
export enum DeltaTableIngestMode {
  SNAPSHOT = 'snapshot',
  FOLLOW = 'follow',
  SNAPSHOT_AND_FOLLOW = 'snapshot_and_follow'
}
