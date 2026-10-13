/**
 * The status of a service or a pipeline at one time, as the health views show it.
 * - `idle`: stopped on purpose
 * - `transitioning`: starting or stopping
 * - `healthy`: running without issues
 * - `unhealthy`: running, but with a service degradation
 * - `major_issue`: failed
 */
export type HealthStatus = 'idle' | 'transitioning' | 'healthy' | 'unhealthy' | 'major_issue'

/** Orders the statuses from the least to the most severe. A timeline bar with mixed statuses
 *  shows the most severe one. */
export const healthSeverity: Record<HealthStatus, number> = {
  idle: -1,
  healthy: 0,
  transitioning: 1,
  unhealthy: 2,
  major_issue: 3
}

/**
 * A group of consecutive events with the same status and tag. The incident list shows one entry
 * for each group. `active` is true while the incident continues.
 * @template S status union, e.g. `ClusterEventType` or `PipelineHealthStatus`
 * @template T tag union, which identifies the sub-service of the group
 */
export type HealthEventBucket<S extends string, T extends string> = {
  timestampFrom: Date
  timestampTo: Date
  type: S
  description: string
  tag: T
  active: boolean
  title: string
  events: {
    id: string
    timestamp: Date
    status: S
  }[]
}
