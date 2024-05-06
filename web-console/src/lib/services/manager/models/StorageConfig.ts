/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { StorageCacheConfig } from './StorageCacheConfig'
/**
 * Configuration for persistent storage in a [`PipelineConfig`].
 */
export type StorageConfig = {
  cache: StorageCacheConfig
  /**
   * The location where the pipeline state is stored or will be stored.
   *
   * It should point to a path on the file-system of the machine/container
   * where the pipeline will run. If that path doesn't exist yet, or if it
   * does not contain any checkpoints, then the pipeline creates it and
   * starts from an initial state in which no data has yet been received. If
   * it does exist, then the pipeline starts from the most recent checkpoint
   * that already exists there. In either case, (further) checkpoints will be
   * written there.
   */
  path: string
}
