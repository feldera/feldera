/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ConnectorId } from './ConnectorId';

/**
 * Request to update an existing data-connector.
 */
export type UpdateConnectorRequest = {
    /**
     * New config YAML. If absent, existing YAML will be kept unmodified.
     */
    config?: string;
    connector_id: ConnectorId;
    /**
     * New connector description.
     */
    description: string;
    /**
     * New connector name.
     */
    name: string;
};

