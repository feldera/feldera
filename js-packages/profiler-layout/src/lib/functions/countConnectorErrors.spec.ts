import { aggregateConnectorMetrics, countConnectorErrors } from 'common-ui/connectorMetrics'
import { describe, expect, it } from 'vitest'

const input = (name: string, stream: string, parse: number, transport: number) => ({
  endpoint_name: name,
  config: { stream },
  metrics: {
    buffered_bytes: 0,
    buffered_records: 0,
    end_of_input: false,
    num_parse_errors: parse,
    num_transport_errors: transport,
    total_bytes: 0,
    total_records: 0
  },
  paused: false,
  barrier: false
})

const output = (name: string, stream: string, encode: number, transport: number) => ({
  endpoint_name: name,
  config: { stream },
  metrics: {
    buffered_batches: 0,
    buffered_records: 0,
    memory: 0,
    num_encode_errors: encode,
    num_transport_errors: transport,
    queued_batches: 0,
    queued_records: 0,
    total_processed_input_records: 0,
    total_processed_steps: 0,
    transmitted_bytes: 0,
    transmitted_records: 0
  }
})

const count = (inputs: ReturnType<typeof input>[], outputs: ReturnType<typeof output>[]) =>
  countConnectorErrors(aggregateConnectorMetrics({ inputs, outputs }))

describe('countConnectorErrors', () => {
  it('is 0 without connectors', () => {
    expect(count([], [])).toBe(0)
  })

  it('is 0 when no connector has errors', () => {
    expect(count([input('t.a', 't', 0, 0)], [output('v.a', 'v', 0, 0)])).toBe(0)
  })

  it('adds the parse and transport errors of inputs', () => {
    expect(count([input('t.a', 't', 2, 3)], [])).toBe(5)
  })

  it('adds the encode and transport errors of outputs', () => {
    expect(count([], [output('v.a', 'v', 4, 1)])).toBe(5)
  })

  it('adds inputs and outputs of several relations and connectors', () => {
    expect(
      count(
        [input('t.a', 't', 1, 0), input('t.b', 't', 0, 2), input('u.a', 'u', 3, 0)],
        [output('v.a', 'v', 10, 0), output('w.a', 'w', 0, 20)]
      )
    ).toBe(36)
  })

  it('counts a missing input error counter as 0', () => {
    const { metrics } = input('t.a', 't', 0, 2)
    const missing = { ...metrics, num_parse_errors: undefined as unknown as number }
    const tables = new Map([['t', { aggregate: { metrics: missing }, connectors: [] }]])
    expect(countConnectorErrors({ tables, views: new Map() })).toBe(2)
  })

  it('counts a missing output error counter as 0', () => {
    const { metrics } = output('v.a', 'v', 0, 2)
    const missing = { ...metrics, num_encode_errors: undefined as unknown as number }
    const views = new Map([['v', { aggregate: { metrics: missing }, connectors: [] }]])
    expect(countConnectorErrors({ tables: new Map(), views })).toBe(2)
  })
})
