/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ProgramId } from './ProgramId';
import type { Version } from './Version';

/**
 * Request to cancel ongoing program compilation.
 */
export type CancelProgramRequest = {
    program_id: ProgramId;
    version: Version;
};

