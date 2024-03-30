/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ServiceProbeError } from './ServiceProbeError'
import type { ServiceProbeResult } from './ServiceProbeResult'
/**
 * Response being either success or error.
 */
export type ServiceProbeResponse =
  | {
      success: ServiceProbeResult
    }
  | {
      error: ServiceProbeError
    }
