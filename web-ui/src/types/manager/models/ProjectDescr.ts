/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ProjectId } from './ProjectId';
import type { ProjectStatus } from './ProjectStatus';
import type { Version } from './Version';

/**
 * Project descriptor.
 */
export type ProjectDescr = {
    /**
     * Project description.
     */
    description: string;
    /**
     * Project name (doesn't have to be unique).
     */
    name: string;
    project_id: ProjectId;
    /**
     * A JSON description of the SQL tables and view declarations including
     * field names and types.
     *
     * The schema is set/updated whenever the `status` field reaches >=
     * `ProjectStatus::CompilingRust`.
     *
     * # Example
     *
     * The given SQL program:
     *
     * ```no-run
     * CREATE TABLE USERS ( name varchar );
     * CREATE VIEW OUTPUT_USERS as SELECT * FROM USERS;
     * ```
     *
     * Would lead the following JSON string in `schema`:
     *
     * ```no-run
     * {
         * "inputs": [{
             * "name": "USERS",
             * "fields": [{ "name": "NAME", "type": "VARCHAR", "nullable": true }]
             * }],
             * "outputs": [{
                 * "name": "OUTPUT_USERS",
                 * "fields": [{ "name": "NAME", "type": "VARCHAR", "nullable": true }]
                 * }]
                 * }
                 * ```
                 */
                schema?: string;
                status: ProjectStatus;
                version: Version;
            };

