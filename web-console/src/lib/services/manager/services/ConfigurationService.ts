/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { CancelablePromise } from '../core/CancelablePromise'
import { OpenAPI } from '../core/OpenAPI'
import { request as __request } from '../core/request'
export class ConfigurationService {
  /**
   * Get the list of demo URLs.
   * @returns string URLs to JSON objects that describe a set of demos
   * @throws ApiError
   */
  public static getDemos(): CancelablePromise<Array<string>> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/v0/config/demos'
    })
  }
}
