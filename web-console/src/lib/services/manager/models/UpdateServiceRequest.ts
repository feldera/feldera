/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ServiceConfig } from './ServiceConfig'
/**
 * Request to update an existing service.
 */
export type UpdateServiceRequest = {
  config?: ServiceConfig | null
  /**
   * New service description. If absent, existing name will be kept
   * unmodified.
   */
  description?: string | null
  /**
   * New service name. If absent, existing name will be kept unmodified.
   */
  name?: string | null
}
