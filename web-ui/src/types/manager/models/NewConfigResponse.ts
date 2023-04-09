/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ConfigId } from './ConfigId';
import type { Version } from './Version';

/**
 * Response to a config creation request.
 */
export type NewConfigResponse = {
    config_id: ConfigId;
    version: Version;
};

