/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { NewServiceRequest } from '../models/NewServiceRequest'
import type { NewServiceResponse } from '../models/NewServiceResponse'
import type { ServiceDescr } from '../models/ServiceDescr'
import type { UpdateServiceRequest } from '../models/UpdateServiceRequest'
import type { UpdateServiceResponse } from '../models/UpdateServiceResponse'

import type { CancelablePromise } from '../core/CancelablePromise'
import { OpenAPI } from '../core/OpenAPI'
import { request as __request } from '../core/request'

export class ServicesService {
  /**
   * Fetch services, optionally filtered by name or ID
   * Fetch services, optionally filtered by name or ID
   * @param id Unique service identifier.
   * @param name Unique service name.
   * @returns ServiceDescr List of services retrieved successfully
   * @throws ApiError
   */
  public static listServices(id?: string | null, name?: string | null): CancelablePromise<Array<ServiceDescr>> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/v0/services',
      query: {
        id: id,
        name: name
      },
      errors: {
        404: `Specified service name or ID does not exist`
      }
    })
  }

  /**
   * Create a new service.
   * Create a new service.
   * @param requestBody
   * @returns NewServiceResponse Service successfully created.
   * @throws ApiError
   */
  public static newService(requestBody: NewServiceRequest): CancelablePromise<NewServiceResponse> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/v0/services',
      body: requestBody,
      mediaType: 'application/json'
    })
  }

  /**
   * Fetch a service by ID.
   * Fetch a service by ID.
   * @param serviceId Unique service identifier
   * @returns ServiceDescr Service retrieved successfully.
   * @throws ApiError
   */
  public static getService(serviceId: string): CancelablePromise<ServiceDescr> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/v0/services/{service_id}',
      path: {
        service_id: serviceId
      },
      errors: {
        400: `Specified service id is not a valid uuid.`
      }
    })
  }

  /**
   * Delete an existing service.
   * Delete an existing service.
   * @param serviceId Unique service identifier
   * @returns any Service successfully deleted.
   * @throws ApiError
   */
  public static deleteService(serviceId: string): CancelablePromise<any> {
    return __request(OpenAPI, {
      method: 'DELETE',
      url: '/v0/services/{service_id}',
      path: {
        service_id: serviceId
      },
      errors: {
        400: `Specified service id is not a valid uuid.`,
        404: `Specified service id does not exist.`
      }
    })
  }

  /**
   * Change a service's description or configuration.
   * Change a service's description or configuration.
   * @param serviceId Unique service identifier
   * @param requestBody
   * @returns UpdateServiceResponse Service successfully updated.
   * @throws ApiError
   */
  public static updateService(
    serviceId: string,
    requestBody: UpdateServiceRequest
  ): CancelablePromise<UpdateServiceResponse> {
    return __request(OpenAPI, {
      method: 'PATCH',
      url: '/v0/services/{service_id}',
      path: {
        service_id: serviceId
      },
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        404: `Specified service id does not exist.`
      }
    })
  }
}
