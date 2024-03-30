/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
export type OutputBufferConfig = {
  /**
   * Enable output buffering.
   *
   * The output buffering mechanism allows decoupling the rate at which the pipeline
   * pushes changes to the output transport from the rate of input changes.
   *
   * By default, output updates produced by the pipeline are pushed directly to
   * the output transport. Some destinations may prefer to receive updates in fewer
   * bigger batches. For instance, when writing Parquet files, producing
   * one bigger file every few minutes is usually better than creating
   * small files every few milliseconds.
   *
   * To achieve such input/output decoupling, users can enable output buffering by
   * setting the `enable_buffer` flag to `true`.  When buffering is enabled, output
   * updates produced by the pipeline are consolidated in an internal buffer and are
   * pushed to the output transport when one of several conditions is satisfied:
   *
   * * data has been accumulated in the buffer for more than `max_buffer_time_millis`
   * milliseconds.
   * * buffer size exceeds `max_buffered_records` records.
   *
   * This flag is `false` by default.
   */
  enable_buffer?: boolean
  /**
   * Maximum number of output updates to be kept in the buffer.
   *
   * This parameter bounds the maximal size of the buffer.
   * Note that the size of the buffer is not always equal to the
   * total number of updates output by the pipeline. Updates to the
   * same record can overwrite or cancel previous updates.
   *
   * By default, the buffer can grow indefinitely until one of
   * the other output conditions is satisfied.
   *
   * NOTE: this configuration option requires the `enable_buffer` flag
   * to be set.
   */
  max_buffer_size_records?: number
  /**
   * Maximum time in milliseconds data is kept in the buffer.
   *
   * By default, data is kept in the buffer indefinitely until one of
   * the other output conditions is satisfied.  When this option is
   * set the buffer will be flushed at most every
   * `max_buffer_time_millis` milliseconds.
   *
   * NOTE: this configuration option requires the `enable_buffer` flag
   * to be set.
   */
  max_buffer_time_millis?: number
}
