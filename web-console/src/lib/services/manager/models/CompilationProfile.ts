/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * Argument to `cargo build --profile <>` passed to the rust compiler
 *
 * Note that this is a hint to the backend, and can be overriden by
 * the Feldera instance depending on the administrator configuration.
 */
export enum CompilationProfile {
  DEV = 'dev',
  UNOPTIMIZED = 'unoptimized',
  OPTIMIZED = 'optimized'
}
