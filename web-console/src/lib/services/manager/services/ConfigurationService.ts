/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { Demo } from '../models/Demo'
import type { CancelablePromise } from '../core/CancelablePromise'
import { OpenAPI } from '../core/OpenAPI'
import { request as __request } from '../core/request'
export class ConfigurationService {
  /**
   * Get the list of demos.
   * @returns Demo List of demos.
   * @throws ApiError
   */
  public static getDemos(): CancelablePromise<Array<Demo>> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/v0/config/demos',
      errors: {
        500: `Failed to read demos from the demos directory.`
      }
    })
  }
}
