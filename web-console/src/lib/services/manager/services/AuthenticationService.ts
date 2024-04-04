/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { AuthProvider } from '../models/AuthProvider'
import type { CancelablePromise } from '../core/CancelablePromise'
import { OpenAPI } from '../core/OpenAPI'
import { request as __request } from '../core/request'
export class AuthenticationService {
  /**
   * Get authentication provider configuration
   * Get authentication provider configuration
   * @returns AuthProvider The response body contains Authentication Provider configuration, or is empty if no auth is configured.
   * @throws ApiError
   */
  public static getAuthenticationConfig(): CancelablePromise<AuthProvider> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/config/authentication',
      errors: {
        500: `Request failed.`
      }
    })
  }
}
