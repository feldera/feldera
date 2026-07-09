import type { MetricKind } from './kind'

// Concise "what is this and how do I read it" help for kinds whose visualization is not
// self-evident. Shown as a tooltip on a help icon next to the metric title (MetricKindBlock).
// Kinds absent from this map get no help icon.
const KIND_HELP: Partial<Record<MetricKind, string>> = {
  K6:
    'Compaction state per spine level. One row per level (slot), one cell per worker, colored by ' +
    'state: idle, compaction requested, or compaction in progress. Read down a column to see one ' +
    'worker across levels; read across a row to compare workers at the same level. Hover a cell ' +
    'for its worker and state.',
  K7:
    'Batch-size distribution across workers. Each row is a worker, sorted by average records ' +
    'per batch (smallest on top); the x axis is records per batch. The avg line runs between the ' +
    'min and max lines, so the horizontal gap on a row is that worker’s batch-size spread. ' +
    'Hover a row to mark that worker on the lines.',
  K9:
    'Completed merge work per spine level. Each row is a spine level; bar length is the number ' +
    'of merge steps completed there (log-scaled, so small bars stay visible). Shows the average and skew ' +
    'across workers by default; hover the cursor left to right to inspect a single worker.'
}

/** Help text for a kind, or undefined when the visualization needs no explanation. */
export function kindHelp(kind: MetricKind): string | undefined {
  return KIND_HELP[kind]
}
