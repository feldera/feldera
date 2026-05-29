import type { Severity, TriageRuleResult } from 'triage-types'

/** Display label for a severity ('error' → 'Critical', etc.). */
export const severityLabel: Record<Severity, string> = {
  error: 'Critical',
  warning: 'Medium',
  info: 'Low'
}

/** Sort order: most severe first. */
export const severityOrder: Record<Severity, number> = {
  error: 0,
  warning: 1,
  info: 2
}

/** Derives a human-readable category from a rule identifier (the prefix
 *  before the first '/', title-cased). Falls back to 'General' if no prefix. */
export function getCategory(result: TriageRuleResult): string {
  const prefix = result.rule.split('/')[0]
  if (!prefix) {
    return 'General'
  }
  return prefix.charAt(0).toUpperCase() + prefix.slice(1)
}

/** Unique sorted categories across a set of results, for filter dropdowns. */
export function uniqueCategories(results: TriageRuleResult[]): string[] {
  return [...new Set(results.map(getCategory))].sort()
}

/** Unique severities present in a set of results, ordered from most to least severe. */
export function uniqueSeverities(results: TriageRuleResult[]): Severity[] {
  return [...new Set(results.map((r) => r.severity))].sort(
    (a, b) => (severityOrder[a] ?? 99) - (severityOrder[b] ?? 99)
  )
}
