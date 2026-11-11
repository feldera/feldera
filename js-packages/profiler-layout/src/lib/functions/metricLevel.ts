// Fits the metric selector to the detail level of the metric tables.

import { MetricDescriptions, type MetricOption } from 'profiler-lib'

/** The metrics that the metric selector lists: all with `showAdvanced`, else the basic ones. */
export function levelMetrics(metrics: MetricOption[], showAdvanced: boolean): MetricOption[] {
  return metrics.filter((metric) => MetricDescriptions.isShown(metric.id, showAdvanced))
}

/** The metric that the selector prefers. */
export const PREFERRED_METRIC = 'runtime_seconds'

/** The metric to select from `listed`: `selected` when it is listed, or else `PREFERRED_METRIC`,
 *  or else the first listed metric. With no listed metrics, `selected` stays. */
export function metricToSelect(selected: string, listed: MetricOption[]): string {
  if (listed.length === 0 || listed.some((metric) => metric.id === selected)) {
    return selected
  }
  const preferred = listed.find((metric) => metric.id === PREFERRED_METRIC)
  return (preferred ?? listed[0]!).id
}
