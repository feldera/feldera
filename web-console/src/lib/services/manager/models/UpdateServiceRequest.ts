/* generated using openapi-typescript-codegen -- do no edit */
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
   * New service description.
   */
  description: string
}
