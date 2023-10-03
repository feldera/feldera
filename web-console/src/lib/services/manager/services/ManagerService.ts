/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { Provider } from '../models/Provider'

import type { CancelablePromise } from '../core/CancelablePromise'
import { OpenAPI } from '../core/OpenAPI'
import { request as __request } from '../core/request'

export class ManagerService {
  /**
   * Get authentication provider configuration
   * Get authentication provider configuration
   * @returns Provider The response body contains Authentication Provider configuration.
   * @throws ApiError
   */
  public static getAuthenticationConfig(): CancelablePromise<Provider> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/config/authentication',
      errors: {
        500: `Request failed.`
      }
    })
  }
}
