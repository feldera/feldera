/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { SqlCompilerMessage } from './SqlCompilerMessage';

/**
 * Project compilation status.
 */
export type ProjectStatus = ('None' | 'Pending' | 'CompilingSql' | 'CompilingRust' | 'Success' | {
    SqlError: Array<SqlCompilerMessage>;
} | {
    /**
     * Rust compiler returned an error.
     */
    RustError: string;
} | {
    /**
     * System/OS returned an error when trying to invoke commands.
     */
    SystemError: string;
});

