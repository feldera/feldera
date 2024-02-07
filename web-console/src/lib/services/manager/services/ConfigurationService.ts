/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { CannedDemo } from '../models/CannedDemo'

import type { CancelablePromise } from '../core/CancelablePromise'
import { OpenAPI } from '../core/OpenAPI'
import { request as __request } from '../core/request'

export class ConfigurationService {
  /**
   * Get the list of canned demos (title, description and URL).
   * Get the list of canned demos (title, description and URL).
   * @returns CannedDemo List of canned demos with for each the title, description and URL to the demo JSON object
   * @throws ApiError
   */
  public static getDemos(): CancelablePromise<Array<CannedDemo>> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/v0/config/demos'
    })
  }
}
