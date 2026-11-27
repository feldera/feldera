import type { ZipItem } from 'but-unzip'
import { describe, expect, it } from 'vitest'
import { parseConnectorStatus, processProfileFiles } from './processZipBundle.js'

const TIMESTAMP = '2026-05-25T12-34-56.789Z'

/** Build a ZipItem whose `read()` returns the UTF-8 bytes of `text`. */
const file = (filename: string, text: string): ZipItem => ({
  filename,
  comment: '',
  read: () => new TextEncoder().encode(text)
})

const stats = (value: unknown) => file(`${TIMESTAMP}/stats.json`, JSON.stringify(value))

const input = (name: string, stream: string) => ({
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
    total_bytes: 10,
    total_records: 5
  }
})

const output = (name: string, stream: string) => ({
  endpoint_name: name,
  config: { stream },
  metrics: {
    buffered_batches: 0,
    buffered_records: 0,
    num_encode_errors: 0,
    num_transport_errors: 0,
    total_processed_input_records: 0,
    transmitted_bytes: 0,
    transmitted_records: 7,
    total_processed_steps: 0,
    queued_batches: 0,
    queued_records: 0,
    memory: 0
  }
})

describe('parseConnectorStatus', () => {
  it('returns undefined when there are no connector arrays', () => {
    expect(parseConnectorStatus(null)).toBeUndefined()
    expect(parseConnectorStatus({})).toBeUndefined()
    expect(parseConnectorStatus({ inputs: [], outputs: [] })).toBeUndefined()
    expect(parseConnectorStatus({ inputs: {}, outputs: 'x' } as never)).toBeUndefined()
  })

  it('keeps inputs when outputs are missing', () => {
    const status = parseConnectorStatus({ inputs: [input('a', 't')] } as never)
    expect(status?.inputs).toHaveLength(1)
    expect(status?.outputs).toEqual([])
  })

  it('keeps outputs when inputs are missing', () => {
    const status = parseConnectorStatus({
      outputs: [output('o', 'v')]
    } as never)
    expect(status?.inputs).toEqual([])
    expect(status?.outputs).toHaveLength(1)
  })
})

describe('processProfileFiles connectorMetrics', () => {
  it('reads inputs from stats.json and groups them by table', async () => {
    const r = await processProfileFiles([
      stats({ inputs: [input('a', 't'), input('b', 't')], outputs: [] })
    ])
    const t = r.connectorMetrics?.tables.get('t')
    expect(t?.aggregate.metrics.total_records).toBe(10)
    expect(t?.connectors.map((c) => c.io_active)).toEqual([false, false])
  })

  it('reads a bundle that has outputs but no inputs', async () => {
    const r = await processProfileFiles([stats({ outputs: [output('o', 'v')] })])
    expect(r.connectorMetrics?.tables.size).toBe(0)
    expect(r.connectorMetrics?.views.get('v')?.aggregate.metrics.transmitted_records).toBe(7)
  })

  it('leaves connectorMetrics undefined for an unparsable or null stats.json', async () => {
    for (const text of ['{not json', 'null']) {
      const r = await processProfileFiles([file(`${TIMESTAMP}/stats.json`, text)])
      expect(r.connectorMetrics).toBeUndefined()
    }
  })

  it('rejects a stats.json with a connector entry that cannot be aggregated', async () => {
    // Regression: an entry without `config.stream` made the aggregation throw in the layout and
    // broke the viewer.
    await expect(
      processProfileFiles([stats({ inputs: [{ endpoint_name: 'x' }] })])
    ).rejects.toThrow(/^Cannot read the connector statistics in stats.json: /)
  })
})
