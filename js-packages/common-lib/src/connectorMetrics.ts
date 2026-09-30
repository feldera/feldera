// Per-relation aggregation of the connector statistics that a pipeline reports in its `/stats`
// response. The types are structural subsets of the pipeline-manager API types, so both a live
// `/stats` response and a support bundle's `stats.json` fit them.

import { groupBy } from './array.ts'
import { type Microseconds, microseconds } from './duration.ts'
import { normalizeCaseIndependentName } from './felderaRelation.ts'
import { nonNull } from './function.ts'
import { tuple } from './tuple.ts'

export type ConnectorHealth = {
  status: 'Healthy' | 'Unhealthy'
  description?: string | null
}

export type InputEndpointMetrics = {
  buffered_bytes: number
  buffered_records: number
  end_of_input: boolean
  num_parse_errors: number
  num_transport_errors: number
  processing_latency_p99_micros?: Microseconds | null
  total_bytes: number
  total_records: number
}

export type OutputEndpointMetrics = {
  batch_records_written?: number | null
  buffered_batches: number
  buffered_records: number
  memory: number
  num_encode_errors: number
  num_transport_errors: number
  queued_batches: number
  queued_records: number
  total_processed_input_records: number
  total_processed_steps: number
  transmitted_bytes: number
  transmitted_records: number
}

export type InputEndpointStatus = {
  endpoint_name: string
  config: { stream: string }
  metrics: InputEndpointMetrics
  paused: boolean
  barrier: boolean
  health?: ConnectorHealth | null
  fatal_error?: string | null
}

export type OutputEndpointStatus = {
  endpoint_name: string
  config: { stream: string }
  metrics: OutputEndpointMetrics
  /** Absent on pipelines that predate pausable output connectors. */
  paused?: boolean
  health?: ConnectorHealth | null
  fatal_error?: string | null
}

/** The subset of a `/stats` response that the connector aggregation reads. */
export type ConnectorStatus = {
  inputs: InputEndpointStatus[]
  outputs: OutputEndpointStatus[]
  global_metrics?: {
    transaction_initiators?: {
      initiated_by_connectors?: Record<string, { phase: 'Started' | 'Committed' } | undefined>
    }
  }
}

/** The kind of connector errors to show when a connector is selected. */
export type ConnectorErrorFilter = 'all' | 'parse' | 'transport' | 'encode'

export type AggregatedMetrics<M, EndpointStatus = {}> = {
  aggregate: { metrics: M }
  connectors: ({
    endpointName: string
    metrics: M
  } & EndpointStatus)[]
}

export type AggregatedInputEndpointMetrics = AggregatedMetrics<
  InputEndpointMetrics,
  Pick<InputEndpointStatus, 'paused' | 'barrier' | 'health' | 'fatal_error'> & {
    io_active: boolean
    transaction_phase?: 'started' | 'committed'
  }
>

export type AggregatedOutputEndpointMetrics = AggregatedMetrics<
  OutputEndpointMetrics,
  Pick<OutputEndpointStatus, 'paused' | 'health' | 'fatal_error'> & { io_active: boolean }
>

/** Connector metrics grouped by the table (inputs) or view (outputs) they connect to. */
export type ConnectorMetrics = {
  tables: Map<string, AggregatedInputEndpointMetrics>
  views: Map<string, AggregatedOutputEndpointMetrics>
}

/**
 * Higher of two connector latencies, ignoring connectors without samples.
 *
 * A relation's row summarizes its connectors, and percentiles cannot be summed
 * or averaged into another percentile. The slowest connector is reported
 * instead.
 */
const slowestLatency = (
  a: Microseconds | null | undefined,
  b: Microseconds | null | undefined
): Microseconds | undefined => {
  if (typeof a !== 'number') {
    return typeof b === 'number' ? b : undefined
  }
  return typeof b === 'number' ? microseconds(Math.max(a, b)) : a
}

const relationOf = (endpoint: { config: { stream: string } }) =>
  normalizeCaseIndependentName({ name: endpoint.config.stream })

const sumInputMetrics = (endpoints: InputEndpointStatus[]): InputEndpointMetrics =>
  endpoints.reduce(
    (acc: InputEndpointMetrics, { metrics }) => ({
      total_bytes: acc.total_bytes + metrics.total_bytes,
      total_records: acc.total_records + metrics.total_records,
      buffered_records: acc.buffered_records + metrics.buffered_records,
      num_transport_errors: acc.num_transport_errors + metrics.num_transport_errors,
      num_parse_errors: acc.num_parse_errors + metrics.num_parse_errors,
      end_of_input: acc.end_of_input && metrics.end_of_input,
      buffered_bytes: acc.buffered_bytes + metrics.buffered_bytes,
      processing_latency_p99_micros: slowestLatency(
        acc.processing_latency_p99_micros,
        metrics.processing_latency_p99_micros
      )
    }),
    {
      total_bytes: 0,
      total_records: 0,
      buffered_bytes: 0,
      buffered_records: 0,
      num_transport_errors: 0,
      num_parse_errors: 0,
      end_of_input: true,
      processing_latency_p99_micros: undefined
    }
  )

const sumOutputMetrics = (endpoints: OutputEndpointStatus[]): OutputEndpointMetrics =>
  endpoints.reduce(
    (acc: OutputEndpointMetrics, { metrics }) => ({
      buffered_batches: acc.buffered_batches + metrics.buffered_batches,
      buffered_records: acc.buffered_records + metrics.buffered_records,
      num_encode_errors: acc.num_encode_errors + metrics.num_encode_errors,
      num_transport_errors: acc.num_transport_errors + metrics.num_transport_errors,
      total_processed_input_records:
        acc.total_processed_input_records + metrics.total_processed_input_records,
      transmitted_bytes: acc.transmitted_bytes + metrics.transmitted_bytes,
      transmitted_records: acc.transmitted_records + metrics.transmitted_records,
      total_processed_steps: acc.total_processed_steps + metrics.total_processed_steps,
      queued_batches: acc.queued_batches + metrics.queued_batches,
      queued_records: acc.queued_records + metrics.queued_records,
      memory: acc.memory + metrics.memory,
      batch_records_written: !nonNull(metrics.batch_records_written)
        ? acc.batch_records_written
        : (acc.batch_records_written ?? 0) + metrics.batch_records_written
    }),
    {
      buffered_batches: 0,
      buffered_records: 0,
      num_encode_errors: 0,
      num_transport_errors: 0,
      total_processed_input_records: 0,
      transmitted_bytes: 0,
      transmitted_records: 0,
      total_processed_steps: 0,
      queued_batches: 0,
      queued_records: 0,
      memory: 0,
      batch_records_written: null
    }
  )

/** Lower-cased transaction phase of a connector, when it has started or committed one. */
const transactionPhaseOf = (
  initiator: { phase: 'Started' | 'Committed' } | undefined
): 'started' | 'committed' | undefined => {
  const phase = initiator?.phase?.toLowerCase()
  return phase === 'started' || phase === 'committed' ? phase : undefined
}

/**
 * Group connectors by the relation they connect to, and sum each relation's metrics.
 *
 * @param status The `/stats` response of a pipeline.
 * @param previous The aggregation of the preceding sample. A connector is marked `io_active` when
 *                 its record count grew since then; without a preceding sample no connector is.
 */
export const aggregateConnectorMetrics = (
  status: ConnectorStatus,
  previous?: ConnectorMetrics
): ConnectorMetrics => {
  const initiatedByConnectors =
    status.global_metrics?.transaction_initiators?.initiated_by_connectors ?? {}

  const tables = new Map(
    groupBy(status.inputs, relationOf).map(([relationName, endpoints]) => {
      const oldRelation = previous?.tables.get(relationName)
      const connectors = endpoints.map((cur) => {
        const prev = oldRelation?.connectors.find((c) => c.endpointName === cur.endpoint_name)
        return {
          endpointName: cur.endpoint_name,
          metrics: cur.metrics,
          paused: cur.paused,
          barrier: cur.barrier,
          io_active: prev !== undefined && cur.metrics.total_records > prev.metrics.total_records,
          transaction_phase: transactionPhaseOf(initiatedByConnectors[cur.endpoint_name]),
          health: cur.health,
          fatal_error: cur.fatal_error
        }
      })
      const metrics: AggregatedInputEndpointMetrics = {
        connectors,
        aggregate: { metrics: sumInputMetrics(endpoints) }
      }
      return tuple(relationName, metrics)
    })
  )

  const views = new Map(
    groupBy(status.outputs, relationOf).map(([relationName, endpoints]) => {
      const oldRelation = previous?.views.get(relationName)
      const metrics: AggregatedOutputEndpointMetrics = {
        connectors: endpoints.map((cur) => {
          const prev = oldRelation?.connectors.find((c) => c.endpointName === cur.endpoint_name)
          return {
            endpointName: cur.endpoint_name,
            metrics: cur.metrics,
            io_active:
              prev !== undefined &&
              (cur.metrics.transmitted_records > prev.metrics.transmitted_records ||
                (nonNull(cur.metrics.batch_records_written) &&
                  (!nonNull(prev.metrics.batch_records_written) ||
                    cur.metrics.batch_records_written !== prev.metrics.batch_records_written))),
            paused: cur.paused,
            health: cur.health,
            fatal_error: cur.fatal_error
          }
        }),
        aggregate: { metrics: sumOutputMetrics(endpoints) }
      }
      return tuple(relationName, metrics)
    })
  )

  return { tables, views }
}
