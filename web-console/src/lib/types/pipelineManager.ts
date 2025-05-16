import type { InputEndpointConfig, OutputEndpointConfig } from '$lib/services/pipelineManager'
import type { BigNumber } from 'bignumber.js/bignumber.js'

export type ControllerStatus = {
  global_metrics: GlobalMetrics
  inputs: InputEndpointStatus[]
  outputs: OutputEndpointStatus[]
  metrics: ControllerMetric[]
  suspend_error: any
}

type ControllerMetric = {
  /// Metric name.
  key: string

  /// Optional key-value pairs that provide additional metadata about this
  /// metric.
  labels: [string, string][]

  /// Unit of measure for this metric, if any.
  unit?: MetricUnit

  /// Optional natural language description of the metric.
  description?: string

  /// The metric's value.
  value: MetricValue
}

type MetricUnit =
  | 'count'
  | 'percent'
  | 'seconds'
  | 'milliseconds'
  | 'microseconds'
  | 'nanoseconds'
  | 'tebibytes'
  | 'gigibytes'
  | 'mebibytes'
  | 'kibibytes'
  | 'bytes'
  | 'terabits_per_second'
  | 'gigabits_per_second'
  | 'megabits_per_second'
  | 'kilobits_per_second'
  | 'bits_per_second'
  | 'count_per_second'

type MetricValue =
  | {
      Counter: BigNumber
    }
  | { Gauge: number }
  | { Histogram: HistogramValue }

type HistogramValue = {
  count: BigNumber
  first: number
  middle: number
  last: number
  minimum: number
  maximum: number
  mean: number
}

export interface GlobalConfig {
  workers: number
  cpu_profiler: boolean
  min_batch_size_records: number
  max_buffering_delay_usecs: number
}

export interface GlobalMetrics {
  bootstrap_in_progress: boolean
  buffered_input_records: number
  cpu_msecs: number
  incarnation_uuid: string
  pipeline_complete: boolean
  rss_bytes: number
  runtime_elapsed_msecs: number
  start_time: number
  state: 'Running' | 'Paused'
  storage_bytes: number
  storage_mb_secs: number
  total_completed_records: number
  total_input_records: number
  total_processed_records: number
  uptime_msecs: number
}

export type GlobalMetricsTimestamp = GlobalMetrics & {
  timeMs: number
}

export type InputEndpointStatus = {
  endpoint_name: string
  config: Pick<InputEndpointConfig, 'stream'>
  metrics: InputEndpointMetrics
  is_fault_tolerant?: boolean
  paused?: boolean
  barrier?: boolean
  fatal_error?: boolean
}

export interface InputEndpointMetrics {
  total_bytes: number
  total_records: number
  buffered_records: number
  num_transport_errors: number
  num_parse_errors: number
  end_of_input: boolean
}

export type OutputEndpointStatus = {
  endpoint_name: string
  config: Pick<OutputEndpointConfig, 'stream'>
  metrics: OutputEndpointMetrics
  is_fault_tolerant?: boolean
}

export interface OutputEndpointMetrics {
  transmitted_records: number
  transmitted_bytes: number
  buffered_records: number
  buffered_batches: number
  num_encode_errors: number
  num_transport_errors: number
  total_processed_input_records: number
}

export interface ConnectorStatus {
  endpoint_name: string
  config: object
  metrics: InputEndpointMetrics | OutputEndpointMetrics
  fatal_error: string | null
}

export type XgressRecord = Record<string, string | number | boolean | BigNumber>
