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
   * Should persistent storage be enabled for this pipeline?
   *
   * - If `true`, the pipeline state is stored in the specified location,
   * is persisted across restarts, and can be checkpointed and recovered.
   *
   * - If `false`, the pipeline's state is kept in in-memory data-structures.
   * This is useful if the pipeline is ephemeral and does not need to be recovered
   * after a restart. The pipeline will most likely run faster since it does not
   * need to read from, or write to disk
   */
  storage?: boolean
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
   * Pipeline name.
   */
  name?: string | null
  /**
   * Output endpoint configuration.
   */
  outputs?: Record<string, OutputEndpointConfig>
  /**
   * The location where the pipeline state is stored.
   *
   * It should point to a path on the file-system of the machine/container where the
   * pipeline can find its persistent state.
   *
   * This field must be set by the pipeline runner implementation on startup
   * if `global.storage` is `true`.
   * If `global.storage` is `false`, this field is ignored by the pipeline.
   */
  storage_location?: string | null
}
