/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ConfigDescr } from '../models/ConfigDescr';
import type { NewConfigRequest } from '../models/NewConfigRequest';
import type { NewConfigResponse } from '../models/NewConfigResponse';
import type { UpdateConfigRequest } from '../models/UpdateConfigRequest';
import type { UpdateConfigResponse } from '../models/UpdateConfigResponse';

import type { CancelablePromise } from '../core/CancelablePromise';
import { OpenAPI } from '../core/OpenAPI';
import { request as __request } from '../core/request';

export class ConfigService {

    /**
     * List project configurations.
     * List project configurations.
     * @returns ConfigDescr Project config list retrieved successfully.
     * @throws ApiError
     */
    public static listConfigs(): CancelablePromise<Array<ConfigDescr>> {
        return __request(OpenAPI, {
            method: 'GET',
            url: '/configs',
        });
    }

    /**
     * Create a new project configuration.
     * Create a new project configuration.
     * @param requestBody
     * @returns NewConfigResponse Configuration successfully created.
     * @throws ApiError
     */
    public static newConfig(
        requestBody: NewConfigRequest,
    ): CancelablePromise<NewConfigResponse> {
        return __request(OpenAPI, {
            method: 'POST',
            url: '/configs',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                404: `Specified \`project_id\` does not exist in the database.`,
            },
        });
    }

    /**
     * Update existing project configuration.
     * Update existing project configuration.
     *
     * Updates project config name, description and code and, optionally, config
     * and connectors. On success, increments config version by 1.
     * @param requestBody
     * @returns UpdateConfigResponse Configuration successfully updated.
     * @throws ApiError
     */
    public static updateConfig(
        requestBody: UpdateConfigRequest,
    ): CancelablePromise<UpdateConfigResponse> {
        return __request(OpenAPI, {
            method: 'PATCH',
            url: '/configs',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                404: `A connector ID in \`connectors\` does not exist in the database.`,
            },
        });
    }

    /**
     * List project configurations.
     * List project configurations.
     * @param configId Unique configuration identifier
     * @returns ConfigDescr Project config retrieved successfully.
     * @throws ApiError
     */
    public static configStatus(
        configId: number,
    ): CancelablePromise<ConfigDescr> {
        return __request(OpenAPI, {
            method: 'GET',
            url: '/configs/{config_id}',
            path: {
                'config_id': configId,
            },
            errors: {
                404: `Specified \`config_id\` does not exist in the database.`,
            },
        });
    }

    /**
     * Delete existing project configuration.
     * Delete existing project configuration.
     * @param configId Unique configuration identifier
     * @returns any Configuration successfully deleted.
     * @throws ApiError
     */
    public static deleteConfig(
        configId: number,
    ): CancelablePromise<any> {
        return __request(OpenAPI, {
            method: 'DELETE',
            url: '/configs/{config_id}',
            path: {
                'config_id': configId,
            },
            errors: {
                404: `Specified \`config_id\` does not exist in the database.`,
            },
        });
    }

}
