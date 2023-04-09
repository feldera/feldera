/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ProjectId } from './ProjectId';
import type { Version } from './Version';

/**
 * Request to cancel ongoing project compilation.
 */
export type CancelProjectRequest = {
    project_id: ProjectId;
    version: Version;
};

