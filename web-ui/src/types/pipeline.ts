export interface GlobalConfig {
  workers: number
  cpu_profiler: boolean
  min_batch_size_records: number
  max_buffering_delay_usecs: number
}

export interface GlobalMetrics {
  buffered_input_records: number
  total_input_records: number
  total_processed_records: number
  pipeline_complete: boolean
}

export interface InputConnectorMetrics {
  total_bytes: number
  total_records: number
  buffered_bytes: number
  buffered_records: number
  num_transport_errors: number
  num_parse_errors: number
  end_of_input: boolean
}

export interface OutputConnectorMetrics {
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
  metrics: InputConnectorMetrics | OutputConnectorMetrics
  fatal_error: string | null
}
