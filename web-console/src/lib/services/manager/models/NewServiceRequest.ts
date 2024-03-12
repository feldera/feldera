/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ServiceConfig } from './ServiceConfig'
/**
 * Request to create a new service.
 */
export type NewServiceRequest = {
  config: ServiceConfig
  /**
   * Service description.
   */
  description: string
  /**
   * Service name.
   */
  name: string
}
