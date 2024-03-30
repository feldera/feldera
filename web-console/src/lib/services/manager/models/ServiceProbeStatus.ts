/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * Service probe status.
 *
 * State transition diagram:
 * ```text
 * Pending
 * │
 * │ (Prober server picks up the probe)
 * │
 * ▼
 * ⌛Running ───► Failure
 * │
 * ▼
 * Success
 * ```
 */
export enum ServiceProbeStatus {
  PENDING = 'pending',
  RUNNING = 'running',
  SUCCESS = 'success',
  FAILURE = 'failure'
}
