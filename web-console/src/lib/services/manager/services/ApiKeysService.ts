/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ApiKeyDescr } from '../models/ApiKeyDescr'
import type { NewApiKeyRequest } from '../models/NewApiKeyRequest'
import type { NewApiKeyResponse } from '../models/NewApiKeyResponse'
import type { CancelablePromise } from '../core/CancelablePromise'
import { OpenAPI } from '../core/OpenAPI'
import { request as __request } from '../core/request'
export class ApiKeysService {
  /**
   * List all API keys
   * List all API keys
   * @param name API key name
   * @returns ApiKeyDescr API keys retrieved successfully
   * @throws ApiError
   */
  public static listApiKeys(name?: string | null): CancelablePromise<Array<ApiKeyDescr>> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/v0/api_keys',
      query: {
        name: name
      },
      errors: {
        404: `Specified API key name does not exist.`
      }
    })
  }
  /**
   * Create an API key
   * Create an API key
   * @param requestBody
   * @returns NewApiKeyResponse API key created successfully.
   * @throws ApiError
   */
  public static createApiKey(requestBody: NewApiKeyRequest): CancelablePromise<NewApiKeyResponse> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/v0/api_keys',
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        409: `An api key with this name already exists.`
      }
    })
  }
  /**
   * Get an API key description
   * Get an API key description
   * @param apiKeyName Unique API key name
   * @returns ApiKeyDescr API key retrieved successfully
   * @throws ApiError
   */
  public static getApiKey(apiKeyName: string): CancelablePromise<ApiKeyDescr> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/v0/api_keys/{api_key_name}',
      path: {
        api_key_name: apiKeyName
      },
      errors: {
        404: `Specified API key name does not exist.`
      }
    })
  }
  /**
   * Delete an API key
   * Delete an API key
   * @param apiKeyName Unique API key name
   * @returns any API key deleted successfully
   * @throws ApiError
   */
  public static deleteApiKey(apiKeyName: string): CancelablePromise<any> {
    return __request(OpenAPI, {
      method: 'DELETE',
      url: '/v0/api_keys/{api_key_name}',
      path: {
        api_key_name: apiKeyName
      },
      errors: {
        404: `Specified API key name does not exist.`
      }
    })
  }
}
