/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ConnectorDescr } from '../models/ConnectorDescr'
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
      url: '/connectors',
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
   * @returns NewConnectorResponse Connector successfully created.
   * @throws ApiError
   */
  public static newConnector(requestBody: NewConnectorRequest): CancelablePromise<NewConnectorResponse> {
    return __request(OpenAPI, {
      method: 'POST',
      url: '/connectors',
      body: requestBody,
      mediaType: 'application/json'
    })
  }

  /**
   * Fetch a connector by ID.
   * Fetch a connector by ID.
   * @param connectorId Unique connector identifier
   * @returns ConnectorDescr Connector retrieved successfully.
   * @throws ApiError
   */
  public static getConnector(connectorId: string): CancelablePromise<ConnectorDescr> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/connectors/{connector_id}',
      path: {
        connector_id: connectorId
      },
      errors: {
        400: `Specified connector id is not a valid uuid.`
      }
    })
  }

  /**
   * Delete an existing connector.
   * Delete an existing connector.
   * @param connectorId Unique connector identifier
   * @returns any connector successfully deleted.
   * @throws ApiError
   */
  public static deleteConnector(connectorId: string): CancelablePromise<any> {
    return __request(OpenAPI, {
      method: 'DELETE',
      url: '/connectors/{connector_id}',
      path: {
        connector_id: connectorId
      },
      errors: {
        400: `Specified connector id is not a valid uuid.`,
        404: `Specified connector id does not exist.`
      }
    })
  }

  /**
   * Change a connector's name, description or configuration.
   * Change a connector's name, description or configuration.
   * @param connectorId Unique connector identifier
   * @param requestBody
   * @returns UpdateConnectorResponse connector successfully updated.
   * @throws ApiError
   */
  public static updateConnector(
    connectorId: string,
    requestBody: UpdateConnectorRequest
  ): CancelablePromise<UpdateConnectorResponse> {
    return __request(OpenAPI, {
      method: 'PATCH',
      url: '/connectors/{connector_id}',
      path: {
        connector_id: connectorId
      },
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        404: `Specified connector id does not exist.`
      }
    })
  }
}
