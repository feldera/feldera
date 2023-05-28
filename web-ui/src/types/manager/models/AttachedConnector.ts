/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ConnectorId } from './ConnectorId';

/**
 * Format to add attached connectors during a config update.
 */
export type AttachedConnector = {
    /**
     * The YAML config for this attached connector.
     */
    config: string;
    connector_id: ConnectorId;
    /**
     * Is this an input or an output?
     */
    is_input: boolean;
    /**
     * A unique identifier for this attachement.
     */
    name: string;
};

