import { describe, expect, it } from 'vitest'
import {
  aggregateConnectorMetrics,
  type ConnectorStatus,
  type InputEndpointStatus,
  type OutputEndpointStatus
} from './connectorMetrics.ts'
import { microseconds } from './duration.ts'

const input = (
  name: string,
  stream: string,
  m: Partial<InputEndpointStatus['metrics']> = {}
): InputEndpointStatus => ({
  endpoint_name: name,
  config: { stream },
  paused: false,
  barrier: false,
  metrics: {
    buffered_bytes: 0,
    buffered_records: 0,
    end_of_input: false,
    num_parse_errors: 0,
    num_transport_errors: 0,
    total_bytes: 0,
    total_records: 0,
    ...m
  }
})

const output = (
  name: string,
  stream: string,
  m: Partial<OutputEndpointStatus['metrics']> = {}
): OutputEndpointStatus => ({
  endpoint_name: name,
  config: { stream },
  metrics: {
    buffered_batches: 0,
    buffered_records: 0,
    memory: 0,
    num_encode_errors: 0,
    num_transport_errors: 0,
    queued_batches: 0,
    queued_records: 0,
    total_processed_input_records: 0,
    total_processed_steps: 0,
    transmitted_bytes: 0,
    transmitted_records: 0,
    ...m
  }
})

const status = (s: Partial<ConnectorStatus>): ConnectorStatus => ({
  inputs: [],
  outputs: [],
  ...s
})

describe('aggregateConnectorMetrics', () => {
  it('returns empty maps without connectors', () => {
    const r = aggregateConnectorMetrics(status({}))
    expect([r.tables.size, r.views.size]).toEqual([0, 0])
  })

  it('groups connectors of one relation and sums metrics', () => {
    const t = aggregateConnectorMetrics(
      status({
        inputs: [
          input('a', 't', { total_records: 2, end_of_input: true }),
          input('b', 't', { total_records: 3 })
        ]
      })
    ).tables.get('t')
    expect(t?.connectors.map((c) => c.endpointName)).toEqual(['a', 'b'])
    expect(t?.aggregate.metrics.total_records).toBe(5)
    expect(t?.aggregate.metrics.end_of_input).toBe(false)
  })

  it('keeps quoted case-sensitive names apart', () => {
    const r = aggregateConnectorMetrics(status({ inputs: [input('a', '"T"'), input('b', '"t"')] }))
    expect(r.tables.size).toBe(2)
  })

  it('reports the slowest latency, ignoring missing samples', () => {
    const r = aggregateConnectorMetrics(
      status({
        inputs: [
          input('a', 't', { processing_latency_p99_micros: microseconds(10) }),
          input('b', 't', { processing_latency_p99_micros: null }),
          input('c', 't', { processing_latency_p99_micros: microseconds(30) })
        ]
      })
    )
    expect(r.tables.get('t')?.aggregate.metrics.processing_latency_p99_micros).toBe(30)
  })

  it('leaves batch_records_written null unless reported', () => {
    const unreported = aggregateConnectorMetrics(status({ outputs: [output('a', 'v')] }))
    expect(unreported.views.get('v')?.aggregate.metrics.batch_records_written).toBeNull()
    const reported = aggregateConnectorMetrics(
      status({
        outputs: [output('a', 'v'), output('b', 'v', { batch_records_written: 4 })]
      })
    )
    expect(reported.views.get('v')?.aggregate.metrics.batch_records_written).toBe(4)
  })

  it('marks io_active only on growth since the previous sample', () => {
    const s1 = aggregateConnectorMetrics(
      status({
        inputs: [input('a', 't', { total_records: 1 })],
        outputs: [output('o', 'v')]
      })
    )
    expect(s1.tables.get('t')?.connectors[0].io_active).toBe(false)
    const next = status({
      inputs: [input('a', 't', { total_records: 2 })],
      outputs: [output('o', 'v', { batch_records_written: 1 })]
    })
    const s2 = aggregateConnectorMetrics(next, s1)
    expect(s2.tables.get('t')?.connectors[0].io_active).toBe(true)
    expect(s2.views.get('v')?.connectors[0].io_active).toBe(true)
    const s3 = aggregateConnectorMetrics(next, s2)
    expect(s3.tables.get('t')?.connectors[0].io_active).toBe(false)
    expect(s3.views.get('v')?.connectors[0].io_active).toBe(false)
  })

  it('reads the transaction phase of each connector', () => {
    const r = aggregateConnectorMetrics(
      status({
        inputs: [input('a', 't'), input('b', 't'), input('c', 't')],
        global_metrics: {
          transaction_initiators: {
            initiated_by_connectors: {
              a: { phase: 'Committed' },
              b: { phase: 'Started' }
            }
          }
        }
      })
    )
    expect(r.tables.get('t')?.connectors.map((c) => c.transaction_phase)).toEqual([
      'committed',
      'started',
      undefined
    ])
  })

  it('tolerates a missing global_metrics', () => {
    const r = aggregateConnectorMetrics(status({ inputs: [input('a', 't')] }))
    expect(r.tables.get('t')?.connectors[0].transaction_phase).toBeUndefined()
  })
})
