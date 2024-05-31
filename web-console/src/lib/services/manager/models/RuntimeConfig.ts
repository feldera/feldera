/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { ResourceConfig } from './ResourceConfig'
/**
 * Global pipeline configuration settings. This is the publicly
 * exposed type for users to configure pipelines.
 */
export type RuntimeConfig = {
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
  /**
   * The minimum estimated number of rows in a batch to write it to storage.
   * This is provided for debugging and fine-tuning and should ordinarily be
   * left unset. It only has an effect when `storage` is set to true.
   *
   * A value of 0 will write even empty batches to storage, and nonzero
   * values provide a threshold.  `usize::MAX` would effectively disable
   * storage.
   */
  min_storage_rows?: number | null
  resources?: ResourceConfig
  /**
   * Should persistent storage be enabled for this pipeline?
   *
   * - If `false` (default), the pipeline's state is kept in in-memory data-structures.
   * This is useful if the pipeline is ephemeral and does not need to be recovered
   * after a restart. The pipeline will most likely run faster since it does not
   * need to read from, or write to disk
   *
   * - If `true`, the pipeline state is stored in the specified location,
   * is persisted across restarts, and can be checkpointed and recovered.
   * This feature is currently experimental.
   */
  storage?: boolean
  /**
   * Enable the TCP metrics exporter.
   *
   * This is used for development purposes only.
   * If enabled, the `metrics-observer` CLI tool
   * can be used to inspect metrics from the pipeline.
   *
   * Because of how Rust metrics work, this is only honored for the first
   * pipeline to be instantiated within a given process.
   */
  tcp_metrics_exporter?: boolean
  /**
   * Number of DBSP worker threads.
   */
  workers?: number
}
