/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { SqlCompilerMessage } from './SqlCompilerMessage'
/**
 * Program compilation status.
 */
export type ProgramStatus =
  | 'Pending'
  | 'CompilingSql'
  | 'CompilingRust'
  | 'Success'
  | {
      /**
       * SQL compiler returned an error.
       */
      SqlError: Array<SqlCompilerMessage>
    }
  | {
      /**
       * Rust compiler returned an error.
       */
      RustError: string
    }
  | {
      /**
       * System/OS returned an error when trying to invoke commands.
       */
      SystemError: string
    }
