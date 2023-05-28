/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ProgramId } from './ProgramId';
import type { Version } from './Version';

/**
 * Request to queue a program for compilation.
 */
export type CompileProgramRequest = {
    program_id: ProgramId;
    version: Version;
};

