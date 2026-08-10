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
