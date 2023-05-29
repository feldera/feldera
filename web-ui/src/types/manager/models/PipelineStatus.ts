/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

/**
 * Lifecycle of a pipeline.
 */
export enum PipelineStatus {
  SHUTDOWN = 'Shutdown',
  DEPLOYED = 'Deployed',
  FAILED_TO_DEPLOY = 'FailedToDeploy',
  RUNNING = 'Running',
  PAUSED = 'Paused',
  FAILED = 'Failed'
}
