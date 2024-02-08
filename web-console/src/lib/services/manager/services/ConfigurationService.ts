/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { CancelablePromise } from '../core/CancelablePromise'
import { OpenAPI } from '../core/OpenAPI'
import { request as __request } from '../core/request'

export class ConfigurationService {
  /**
   * Get the list of canned demos (title, description and URL).
   * Get the list of canned demos (title, description and URL).
   * @returns string A list of canned demo URLs to the demo JSON object
   * @throws ApiError
   */
  public static getDemos(): CancelablePromise<Array<string>> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/v0/config/demos'
    })
  }
}
