/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

/**
 * Request to create a new connector.
 */
export type NewConnectorRequest = {
    /**
     * connector config.
     */
    config: string;
    /**
     * connector description.
     */
    description: string;
    /**
     * connector name.
     */
    name: string;
};

