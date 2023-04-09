/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ConfigId } from './ConfigId';
import type { Version } from './Version';

/**
 * Request to create a new pipeline.
 */
export type NewPipelineRequest = {
    config_id: ConfigId;
    config_version: Version;
};

