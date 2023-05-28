/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ProgramId } from './ProgramId';

/**
 * Update program request.
 */
export type UpdateProgramRequest = {
    /**
     * New SQL code for the program or `None` to keep existing program
     * code unmodified.
     */
    code?: string;
    /**
     * New description for the program.
     */
    description?: string;
    /**
     * New name for the program.
     */
    name: string;
    program_id: ProgramId;
};

