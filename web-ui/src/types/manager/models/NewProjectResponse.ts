/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ProjectId } from './ProjectId';
import type { Version } from './Version';

/**
 * Response to a new project request.
 */
export type NewProjectResponse = {
    project_id: ProjectId;
    version: Version;
};

