/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { InputEndpointConfig } from './InputEndpointConfig'
import type { OutputEndpointConfig } from './OutputEndpointConfig'
import type { ResourceConfig } from './ResourceConfig'
/**
 * Pipeline configuration specified by the user when creating
 * a new pipeline instance.
 *
 * This is the shape of the overall pipeline configuration, but is not
 * the publicly exposed type with which users configure pipelines.
 */
export type PipelineConfig = {
  /**
   * Enable CPU profiler.
   */
  cpu_profiler?: boolean
  /**
   * Maximal delay in microseconds to wait for `min_batch_size_records` to
   * get buffered by the controller, defaults to 0.
   */
  max_buffering_delay_usecs?: number
  /**
   * Minimal input batch size.
   *
   * The controller delays pushing input records to the circuit until at
   * least `min_batch_size_records` records have been received (total
   * across all endpoints) or `max_buffering_delay_usecs` microseconds
   * have passed since at least one input records has been buffered.
   * Defaults to 0.
   */
  min_batch_size_records?: number
  resources?: ResourceConfig
  /**
   * Number of DBSP worker threads.
   */
  workers?: number
} & {
  /**
   * Input endpoint configuration.
   */
  inputs: Record<string, InputEndpointConfig>
  /**
   * Maximum number of rows of any given persistent trace to keep in memory
   * before spilling it to storage. If this is 0, then all traces will be
   * stored on disk; if it is `usize::MAX`, then all traces will be kept in
   * memory; and intermediate values specify a threshold.
   */
  max_memory_rows?: number
  /**
   * Pipeline name
   */
  name?: string | null
  /**
   * Output endpoint configuration.
   */
  outputs?: Record<string, OutputEndpointConfig>
  /**
   * Storage location.
   *
   * An identifier for location where the pipeline's state is stored.
   * If not set, the pipeline's state is not persisted across
   * restarts.
   */
  storage_location?: string | null
}
