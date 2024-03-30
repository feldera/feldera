/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * Range of possible errors that can occur during a service probe.
 * These are shared across all services.
 */
export type ServiceProbeError =
  | 'timeout_exceeded'
  | {
      unsupported_request: {
        probe_type: string
        service_type: string
      }
    }
  | {
      other: string
    }
