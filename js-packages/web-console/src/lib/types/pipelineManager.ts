import type { GlobalControllerMetrics } from '$lib/services/manager'

export type GlobalMetricsTimestamp = GlobalControllerMetrics & {
  timeMs: number
}

export type XgressRecord = Record<string, string | number | boolean | BigNumber>

export type TimeSeriesEntry = {
  /**
   * Timestamp ms
   */
  t: BigNumber
  /**
   * Processed records
   */
  r: BigNumber
  /**
   * Used memory bytes
   */
  m: BigNumber
  /**
   * Used storage bytes
   */
  s: BigNumber
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
