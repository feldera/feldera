/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { CreateServiceProbeResponse } from '../models/CreateServiceProbeResponse'
import type { NewServiceRequest } from '../models/NewServiceRequest'
import type { NewServiceResponse } from '../models/NewServiceResponse'
import type { ServiceDescr } from '../models/ServiceDescr'
import type { ServiceProbeDescr } from '../models/ServiceProbeDescr'
import type { ServiceProbeRequest } from '../models/ServiceProbeRequest'
import type { ServiceProbeType } from '../models/ServiceProbeType'
import type { UpdateServiceRequest } from '../models/UpdateServiceRequest'
import type { UpdateServiceResponse } from '../models/UpdateServiceResponse'
import type { CancelablePromise } from '../core/CancelablePromise'
import { OpenAPI } from '../core/OpenAPI'
import { request as __request } from '../core/request'
export class ServicesService {
  /**
   * Fetch services, optionally filtered by name, ID or configuration type.
   * Fetch services, optionally filtered by name, ID or configuration type.
   * @param id If provided, will filter based on exact match of the service identifier.
   * @param name If provided, will filter based on exact match of the service name.
   * @param configType If provided, will filter based on exact match of the configuration type.
   * @returns ServiceDescr List of services retrieved successfully
   * @throws ApiError
   */
  public static listServices(
    id?: string | null,
    name?: string | null,
    configType?: string | null
  ): CancelablePromise<Array<ServiceDescr>> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/v0/services',
      query: {
        id: id,
        name: name,
        config_type: configType
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
   * @returns NewServiceResponse Service successfully created
   * @throws ApiError
   */
  public static newService(requestBody: NewServiceRequest): CancelablePromise<NewServiceResponse> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/v0/services',
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        409: `A service with this name already exists in the database`
      }
    })
  }
  /**
   * Fetch a service by name.
   * Fetch a service by name.
   * @param serviceName Unique service name
   * @returns ServiceDescr Service retrieved successfully
   * @throws ApiError
   */
  public static getService(serviceName: string): CancelablePromise<ServiceDescr> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/v0/services/{service_name}',
      path: {
        service_name: serviceName
      },
      errors: {
        404: `Specified service name does not exist`
      }
    })
  }
  /**
   * Delete an existing service.
   * Delete an existing service.
   * @param serviceName Unique service name
   * @returns any Service successfully deleted
   * @throws ApiError
   */
  public static deleteService(serviceName: string): CancelablePromise<any> {
    return __request(OpenAPI, {
      method: 'DELETE',
      url: '/v0/services/{service_name}',
      path: {
        service_name: serviceName
      },
      errors: {
        404: `Specified service name does not exist`
      }
    })
  }
  /**
   * Update the name, description and/or configuration of a service.
   * Update the name, description and/or configuration of a service.
   * @param serviceName Unique service name
   * @param requestBody
   * @returns UpdateServiceResponse Service successfully updated
   * @throws ApiError
   */
  public static updateService(
    serviceName: string,
    requestBody: UpdateServiceRequest
  ): CancelablePromise<UpdateServiceResponse> {
    return __request(OpenAPI, {
      method: 'PATCH',
      url: '/v0/services/{service_name}',
      path: {
        service_name: serviceName
      },
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        404: `Specified service name does not exist`
      }
    })
  }
  /**
   * Fetch a list of probes for a service, optionally filtered by id.
   * Fetch a list of probes for a service, optionally filtered by id.
   * @param serviceName Unique service name
   * @param id If provided, will filter based on exact match of the service probe
   * identifier.
   * @param limit If provided, will limit the amount of probes to the N most recent.
   * @param type If provided, will only have probes of that particular type.
   * @returns ServiceProbeDescr Service probes retrieved successfully.
   * @throws ApiError
   */
  public static listServiceProbes(
    serviceName: string,
    id?: string | null,
    limit?: number | null,
    type?: ServiceProbeType | null
  ): CancelablePromise<Array<ServiceProbeDescr>> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/v0/services/{service_name}/probes',
      path: {
        service_name: serviceName
      },
      query: {
        id: id,
        limit: limit,
        type: type
      },
      errors: {
        404: `Specified service name does not exist`
      }
    })
  }
  /**
   * Create a service probe.
   * Create a service probe.
   * @param serviceName Unique service name
   * @param requestBody
   * @returns CreateServiceProbeResponse Service probe created successfully
   * @throws ApiError
   */
  public static newServiceProbe(
    serviceName: string,
    requestBody: ServiceProbeRequest
  ): CancelablePromise<CreateServiceProbeResponse> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/v0/services/{service_name}/probes',
      path: {
        service_name: serviceName
      },
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        404: `Specified service name does not exist`
      }
    })
  }
}
