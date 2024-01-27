/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ServiceConfig } from './ServiceConfig'

/**
 * Request to create or replace a service.
 */
export type CreateOrReplaceServiceRequest = {
  config: ServiceConfig
  /**
   * Service description.
   */
  description: string
}
