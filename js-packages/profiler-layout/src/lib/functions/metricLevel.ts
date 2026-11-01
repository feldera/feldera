// Fits the metric selector to the detail level of the metric tables.

import { MetricDescriptions, type MetricLevel, type MetricOption } from 'profiler-lib'

/** The metrics that the metric selector lists at a level. */
export function levelMetrics(metrics: MetricOption[], level: MetricLevel): MetricOption[] {
  return metrics.filter((metric) => MetricDescriptions.isShown(metric.id, level))
}

/** The metric to select from `listed`: `selected` when it is listed, or else the runtime, or else
 *  the first listed metric. With no listed metrics, `selected` stays. */
export function metricToSelect(selected: string, listed: MetricOption[]): string {
  if (listed.length === 0 || listed.some((metric) => metric.id === selected)) {
    return selected
  }
  const runtime = listed.find((metric) => metric.id === 'runtime_seconds')
  return (runtime ?? listed[0]!).id
}
