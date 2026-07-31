import type { GlobalControllerMetrics } from '$lib/services/manager'

export type GlobalMetricsTimestamp = GlobalControllerMetrics & {
  timeMs: number
}

export type XgressRecord = Record<string, string | number | boolean | BigNumber>

export type TimeSeriesEntry = {
  /**
   * Timestamp ms
   */
  t: number
  /**
   * Processed records
   */
  r: number
  /**
   * Used memory bytes
   */
  m: number
  /**
   * Used storage bytes
   */
  s: number
  /**
   * Input processing latency (ingest to circuit-processed), p50 across
   * connectors, microseconds. Absent when no connector has latency samples.
   */
  pp50?: number
  /**
   * Input processing latency, p99 across connectors, microseconds.
   */
  pp99?: number
  /**
   * End-to-end completion latency (ingest to all outputs pushed), p50 across
   * connectors, microseconds.
   */
  cp50?: number
  /**
   * End-to-end completion latency, p99 across connectors, microseconds.
   */
  cp99?: number
}

export type PipelineDiff = {
  tables: EntityDiff
  views: EntityDiff
  inputConnectors: EntityDiff
  outputConnectors: EntityDiff
  error: string | undefined
}

export type EntityDiff = {
  removed: string[]
  modified: string[]
  added: string[]
}
