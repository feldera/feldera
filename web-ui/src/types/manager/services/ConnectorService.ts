/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ConnectorDescr } from '../models/ConnectorDescr';
import type { NewConnectorRequest } from '../models/NewConnectorRequest';
import type { NewConnectorResponse } from '../models/NewConnectorResponse';
import type { UpdateConnectorRequest } from '../models/UpdateConnectorRequest';
import type { UpdateConnectorResponse } from '../models/UpdateConnectorResponse';

import type { CancelablePromise } from '../core/CancelablePromise';
import { OpenAPI } from '../core/OpenAPI';
import { request as __request } from '../core/request';

export class ConnectorService {

    /**
     * Create a new connector configuration.
     * Create a new connector configuration.
     * @param requestBody
     * @returns NewConnectorResponse connector successfully created.
     * @throws ApiError
     */
    public static newConnector(
        requestBody: NewConnectorRequest,
    ): CancelablePromise<NewConnectorResponse> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/connector',
            body: requestBody,
            mediaType: 'application/json',
        });
    }

    /**
     * Delete existing connector.
     * Delete existing connector.
     * @param connectorId Unique connector identifier
     * @returns any connector successfully deleted.
     * @throws ApiError
     */
    public static deleteConnector(
        connectorId: number,
    ): CancelablePromise<any> {
        return __request(OpenAPI, {
            method: 'DELETE',
            url: '/connector/{connector_id}',
            path: {
                'connector_id': connectorId,
            },
            errors: {
                404: `Specified \`connector_id\` does not exist in the database.`,
            },
        });
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
            url: '/connectors',
        });
    }

    /**
     * Update existing project connector.
     * Update existing project connector.
     *
     * Updates project config name and, optionally, code.
     * On success, increments config version by 1.
     * @param requestBody
     * @returns UpdateConnectorResponse connector successfully updated.
     * @throws ApiError
     */
    public static updateConnector(
        requestBody: UpdateConnectorRequest,
    ): CancelablePromise<UpdateConnectorResponse> {
        return __request(OpenAPI, {
            method: 'PATCH',
            url: '/connectors',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                404: `Specified \`connector_id\` does not exist in the database.`,
            },
        });
    }

    /**
     * Returns connector descriptor.
     * Returns connector descriptor.
     * @param connectorId Unique connector identifier
     * @returns ConnectorDescr connector status retrieved successfully.
     * @throws ApiError
     */
    public static connectorStatus(
        connectorId: number,
    ): CancelablePromise<ConnectorDescr> {
        return __request(OpenAPI, {
            method: 'GET',
            url: '/connectors/{connector_id}',
            path: {
                'connector_id': connectorId,
            },
            errors: {
                400: `Missing or invalid \`connector_id\` parameter.`,
                404: `Specified \`connector_id\` does not exist in the database.`,
            },
        });
    }

}
