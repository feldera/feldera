/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ProjectId } from './ProjectId';
import type { Version } from './Version';

/**
 * Request to queue a project for compilation.
 */
export type CompileProjectRequest = {
    project_id: ProjectId;
    version: Version;
};

