/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ConnectorId } from './ConnectorId';
import type { ConnectorType } from './ConnectorType';
import type { Direction } from './Direction';

/**
 * Connector descriptor.
 */
export type ConnectorDescr = {
    config: string;
    connector_id: ConnectorId;
    description: string;
    direction: Direction;
    name: string;
    typ: ConnectorType;
};

