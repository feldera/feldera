/** Indicates the importance of a triage finding. */
export type Severity = 'info' | 'warning' | 'error'

/** A single finding produced by a triage rule. */
export interface TriageRuleResult {
  /** Identifier of the rule that produced this result. */
  rule: string
  severity: Severity
  /** Human-readable description of the finding. */
  message: string
  /** Rule-specific structured data supporting the finding. */
  details: Record<string, unknown>
}

/**
 * An opaque handle to the decoded contents of a support bundle.
 * Concrete implementations are provided by support-bundle-triage package in the cloud repo.
 */
export interface DecodedBundle {}

/** A triage plugin that inspects a support bundle and appends findings to a results accumulator. */
export interface TriagePlugin {
  /** Unique name identifying this plugin. */
  name: string
  /**
   * Inspects the bundle and appends any findings to `results`.
   * @param bundle - The decoded support bundle to inspect.
   * @param results - Accumulator to append findings into.
   */
  triage: (bundle: DecodedBundle, results: TriageResults) => void
}

/** Accumulator for triage findings produced across one or more plugins. */
export class TriageResults {
  constructor(public readonly results: Array<TriageRuleResult> = []) {
  }

  /**
   * Appends one or more findings to the results.
   * @param items - A single rule result or an array of rule results to append.
   */
  push(items: TriageRuleResult | TriageRuleResult[]): this {
    if (Array.isArray(items)) {
      this.results.push(...items)
    }
    else {
      this.results.push(items)
    }
    return this
  }
}
