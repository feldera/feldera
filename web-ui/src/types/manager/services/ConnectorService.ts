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

export class ConnectorService {
  /**
   * Returns connector descriptor.
   * Returns connector descriptor.
   * @param id Unique connector identifier
   * @param name Unique connector name
   * @returns ConnectorDescr connector status retrieved successfully.
   * @throws ApiError
   */
  public static connectorStatus(id?: string | null, name?: string | null): CancelablePromise<ConnectorDescr> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/connector',
      query: {
        id: id,
        name: name
      },
      errors: {
        400: `Connector not specified. Use ?id or ?name query strings in the URL.`,
        404: `Specified connector name does not exist in the database.`
      }
    })
  }

  /**
   * Enumerate the connector database.
   * Enumerate the connector database.
   * @returns ConnectorDescr List of connectors retrieved successfully
   * @throws ApiError
   */
  public static listConnectors(): CancelablePromise<Array<ConnectorDescr>> {
    return __request(OpenAPI, {
      method: 'GET',
      url: '/connectors'
    })
  }

  /**
   * Create a new connector configuration.
   * Create a new connector configuration.
   * @param requestBody
   * @returns NewConnectorResponse connector successfully created.
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
   * Update existing connector.
   * Update existing connector.
   *
   * Updates config name and, optionally, code.
   * On success, increments config version by 1.
   * @param requestBody
   * @returns UpdateConnectorResponse connector successfully updated.
   * @throws ApiError
   */
  public static updateConnector(requestBody: UpdateConnectorRequest): CancelablePromise<UpdateConnectorResponse> {
    return __request(OpenAPI, {
      method: 'PATCH',
      url: '/connectors',
      body: requestBody,
      mediaType: 'application/json',
      errors: {
        404: `Specified connector id does not exist in the database.`
      }
    })
  }

  /**
   * Delete existing connector.
   * Delete existing connector.
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
        404: `Specified connector id does not exist in the database.`
      }
    })
  }
}
