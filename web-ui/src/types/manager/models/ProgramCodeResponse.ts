/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { ProgramDescr } from './ProgramDescr';

/**
 * Response to a program code request.
 */
export type ProgramCodeResponse = {
    /**
     * Program code.
     */
    code: string;
    program: ProgramDescr;
};

