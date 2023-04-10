/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */

import type { InputEndpointConfig } from './InputEndpointConfig'
import type { OutputEndpointConfig } from './OutputEndpointConfig'

/**
 * Pipeline configuration specified by the user when creating
 * a new pipeline instance.
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
   * Output endpoint configuration.
   */
  outputs?: Record<string, OutputEndpointConfig>
}
