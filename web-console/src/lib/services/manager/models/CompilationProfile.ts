/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
/**
 * Enumeration of possible compilation profiles that can be passed to the Rust compiler
 * as an argument via `cargo build --profile <>`. A compilation profile affects among
 * other things the compilation speed (how long till the program is ready to be run)
 * and runtime speed (the performance while running).
 */
export enum CompilationProfile {
  DEV = 'dev',
  UNOPTIMIZED = 'unoptimized',
  OPTIMIZED = 'optimized'
}
