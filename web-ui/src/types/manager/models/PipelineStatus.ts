/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

/**
 * Lifecycle of a pipeline.
 */
export enum PipelineStatus {
  SHUTDOWN = 'Shutdown',
  DEPLOYED = 'Deployed',
  RUNNING = 'Running',
  PAUSED = 'Paused',
  FAILED = 'Failed'
}
