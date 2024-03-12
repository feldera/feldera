/* generated using openapi-typescript-codegen -- do no edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ConnectorDescr } from '../models/ConnectorDescr'
import type { CreateOrReplaceConnectorRequest } from '../models/CreateOrReplaceConnectorRequest'
import type { CreateOrReplaceConnectorResponse } from '../models/CreateOrReplaceConnectorResponse'
import type { NewConnectorRequest } from '../models/NewConnectorRequest'
import type { NewConnectorResponse } from '../models/NewConnectorResponse'
import type { UpdateConnectorRequest } from '../models/UpdateConnectorRequest'
import type { UpdateConnectorResponse } from '../models/UpdateConnectorResponse'
import type { CancelablePromise } from '../core/CancelablePromise'
import { OpenAPI } from '../core/OpenAPI'
import { request as __request } from '../core/request'
export class ConnectorsService {
  /**
   * Fetch connectors, optionally filtered by name or ID
   * Fetch connectors, optionally filtered by name or ID
   * @param id Unique connector identifier.
   * @param name Unique connector name.
   * @returns ConnectorDescr List of connectors retrieved successfully
   * @throws ApiError
   */
  public static listConnectors(id?: string | null, name?: string | null): CancelablePromise<Array<ConnectorDescr>> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/v0/connectors',
      query: {
        id: id,
        name: name
      },
      errors: {
        404: `Specified connector name or ID does not exist`
      }
    })
  }
  /**
   * Create a new connector.
   * Create a new connector.
   * @param requestBody
   * @returns NewConnectorResponse Connector successfully created
   * @throws ApiError
   */
  public static newConnector(requestBody: NewConnectorRequest): CancelablePromise<NewConnectorResponse> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/v0/connectors',
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        409: `A connector with this name already exists in the database`
      }
    })
  }
  /**
   * Fetch a connector by name.
   * Fetch a connector by name.
   * @param connectorName Unique connector name
   * @returns ConnectorDescr Connector retrieved successfully
   * @throws ApiError
   */
  public static getConnector(connectorName: string): CancelablePromise<ConnectorDescr> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/v0/connectors/{connector_name}',
      path: {
        connector_name: connectorName
      },
      errors: {
        404: `Specified connector name does not exist`
      }
    })
  }
  /**
   * Create or replace a connector.
   * Create or replace a connector.
   * @param connectorName Unique connector name
   * @param requestBody
   * @returns CreateOrReplaceConnectorResponse Connector updated successfully
   * @throws ApiError
   */
  public static createOrReplaceConnector(
    connectorName: string,
    requestBody: CreateOrReplaceConnectorRequest
  ): CancelablePromise<CreateOrReplaceConnectorResponse> {
    return __request(OpenAPI, {
      method: 'PUT',
      url: '/v0/connectors/{connector_name}',
      path: {
        connector_name: connectorName
      },
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        409: `A connector with this name already exists in the database`
      }
    })
  }
  /**
   * Delete an existing connector.
   * Delete an existing connector.
   * @param connectorName Unique connector name
   * @returns any Connector successfully deleted
   * @throws ApiError
   */
  public static deleteConnector(connectorName: string): CancelablePromise<any> {
    return __request(OpenAPI, {
      method: 'DELETE',
      url: '/v0/connectors/{connector_name}',
      path: {
        connector_name: connectorName
      },
      errors: {
        404: `Specified connector name does not exist`
      }
    })
  }
  /**
   * Update the name, description and/or configuration of a connector.
   * Update the name, description and/or configuration of a connector.
   * @param connectorName Unique connector name
   * @param requestBody
   * @returns UpdateConnectorResponse Connector successfully updated
   * @throws ApiError
   */
  public static updateConnector(
    connectorName: string,
    requestBody: UpdateConnectorRequest
  ): CancelablePromise<UpdateConnectorResponse> {
    return __request(OpenAPI, {
      method: 'PATCH',
      url: '/v0/connectors/{connector_name}',
      path: {
        connector_name: connectorName
      },
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        404: `Specified connector name does not exist`
      }
    })
  }
}
