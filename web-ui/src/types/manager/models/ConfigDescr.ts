/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { AttachedConnector } from './AttachedConnector';
import type { ConfigId } from './ConfigId';
import type { PipelineDescr } from './PipelineDescr';
import type { ProjectId } from './ProjectId';
import type { Version } from './Version';

/**
 * Project configuration descriptor.
 */
export type ConfigDescr = {
    attached_connectors: Array<AttachedConnector>;
    config: string;
    config_id: ConfigId;
    description: string;
    name: string;
    pipeline?: PipelineDescr;
    project_id?: ProjectId;
    version: Version;
};

