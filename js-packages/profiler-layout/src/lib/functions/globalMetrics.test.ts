import { describe, expect, it } from 'vitest'
import openapi from '../../../../../openapi.json'
import { buildGlobalMetrics, type GlobalMetrics } from './globalMetrics.js'

/** The formatted value of one field, or `undefined` when the tile skips the field. */
function format(key: string, raw: unknown): string | undefined {
  return buildGlobalMetrics({ [key]: raw })
    .find((e) => e.key === key)
    ?.value.toString()
}

/** A partial `global_metrics` object with known fields, one unknown field and a `null` field. */
function fullMetrics(): GlobalMetrics {
  return {
    // An unknown field, which the tile shows after the known fields.
    future_metric: 3,
    uptime_msecs: 90_000,
    state: 'Running',
    total_input_records: 1500,
    total_input_bytes: 2048,
    cpu_msecs: 5000,
    rss_bytes: 999_999,
    pipeline_complete: false,
    start_time: 1_700_000_000,
    transaction_id: 1500,
    transaction_msecs: null,
    transaction_initiators: {
      initiated_by_api: 'Started',
      initiated_by_connectors: { kafka_input: { phase: 'Committed' } }
    },
    commit_progress: {
      completed: 3,
      in_progress: 1,
      remaining: 5,
      in_progress_processed_records: 10,
      in_progress_total_records: 20
    }
  }
}

type Schema = {
  type?: string
  enum?: string[]
  $ref?: string
  allOf?: Schema[]
  properties?: Record<string, Schema>
}
const schemas = (openapi as unknown as { components: { schemas: Record<string, Schema> } })
  .components.schemas

/** Follows `$ref` and single-element `allOf` wrappers to the schema that has a `type`. */
function resolve(schema: Schema): Schema {
  if (schema.$ref) {
    return resolve(schemas[schema.$ref.replace('#/components/schemas/', '')])
  }
  if (schema.allOf?.length === 1) {
    return resolve(schema.allOf[0])
  }
  return schema
}

/** Builds a sample value of the OpenAPI type for each `GlobalControllerMetrics` field. An object
 *  field gets `{}`. */
function metricsFromSpec(): GlobalMetrics {
  const properties = schemas.GlobalControllerMetrics.properties!
  return Object.fromEntries(
    Object.entries(properties).map(([key, property]) => {
      const schema = resolve(property)
      const sample =
        schema.type === 'string'
          ? (schema.enum?.[0] ?? 'text')
          : schema.type === 'integer'
            ? 1
            : schema.type === 'boolean'
              ? true
              : {}
      return [key, sample]
    })
  )
}

describe('buildGlobalMetrics', () => {
  it('returns an empty list when no stats are present', () => {
    expect(buildGlobalMetrics(undefined)).toEqual([])
    expect(buildGlobalMetrics({})).toEqual([])
  })

  it('shows known fields in a stable order, then unknown fields, and skips nulls', () => {
    const keys = buildGlobalMetrics(fullMetrics()).map((e) => e.key)
    expect(keys).toEqual([
      'state',
      'pipeline_complete',
      'start_time',
      'uptime_msecs',
      'total_input_records',
      'total_input_bytes',
      'cpu_msecs',
      'rss_bytes',
      'transaction_id',
      'transaction_initiators',
      'commit_progress',
      'future_metric'
    ])
  })

  it('shows every field of the OpenAPI schema with a label', () => {
    const metrics = metricsFromSpec()
    const entries = buildGlobalMetrics(metrics)
    expect(entries.map((e) => e.key).sort()).toEqual(Object.keys(metrics).sort())
    // An unknown field uses its key as the label, so this test fails for a field that has no
    // descriptor.
    const unlabeled = entries.filter((e) => e.label === e.key).map((e) => e.key)
    expect(unlabeled).toEqual([])
  })

  it.each([
    ['total_input_records', 1500, '1.5K'],
    ['total_input_records', 0, '0'],
    ['total_input_bytes', 2048, '2KiB'],
    ['cpu_msecs', 5000, '5s'],
    ['output_stall_msecs', 0, '0s'],
    ['state', 'Running', 'Running'],
    ['pipeline_complete', true, 'true'],
    ['pipeline_complete', false, 'false'],
    ['start_time', 0, '1970-01-01 00:00:00 UTC'],
    ['start_time', 1_700_000_000, '2023-11-14 22:13:20 UTC'],
    // The fraction of a second is dropped.
    ['start_time', 1_700_000_000.5, '2023-11-14 22:13:20 UTC'],
    ['transaction_id', 123_456_789, '123456789'],
    ['memory_pressure_epoch', 1500, '1500']
  ])('formats %s = %j as %s', (key, raw, expected) => {
    expect(format(key, raw)).toBe(expected)
  })

  it('formats a commit progress and shows "?" for each missing count', () => {
    expect(
      format('concurrent_bootstrap_progress', {
        completed: 3,
        in_progress: 1,
        remaining: 5,
        in_progress_processed_records: 1500,
        in_progress_total_records: 2000
      })
    ).toBe('3 completed, 1 in progress, 5 remaining operators (1.5K of 2K records)')
    expect(format('commit_progress', { completed: 3, remaining: 'x' })).toBe(
      '3 completed, ? in progress, ? remaining operators (? of ? records)'
    )
  })

  it.each([
    [{ initiated_by_connectors: {} }, 'None'],
    [{}, 'None'],
    [{ initiated_by_api: 'Started', initiated_by_connectors: {} }, 'API (Started)'],
    [
      { initiated_by_api: null, initiated_by_connectors: { a: { phase: 'Started' } } },
      'a (Started)'
    ],
    [
      { initiated_by_connectors: { a: { phase: 'Started' }, b: { label: 'x' }, c: null } },
      'a (Started), b (?), c (?)'
    ]
  ])('formats the transaction initiators %j as %s', (raw, expected) => {
    expect(format('transaction_initiators', raw)).toBe(expected)
  })

  it.each([
    ['total_input_records', '10'],
    ['total_input_records', Number.NaN],
    ['total_input_records', Number.POSITIVE_INFINITY],
    ['total_input_bytes', true],
    ['cpu_msecs', '5000'],
    ['pipeline_complete', 'true'],
    ['state', 5],
    ['start_time', '2023-11-14'],
    ['transaction_id', '7'],
    ['commit_progress', 'done'],
    ['transaction_initiators', 5],
    ['transaction_msecs', null],
    ['disk_total_bytes', null],
    ['commit_progress', null]
  ])('skips the known field %s with the value %j', (key, raw) => {
    expect(format(key, raw)).toBeUndefined()
  })

  it.each([
    [1500, '1.5K'],
    [true, 'true'],
    ['text', 'text'],
    [{ a: 1 }, '{"a":1}'],
    [[1, 2], '[1,2]']
  ])('formats the unknown field value %j as %s', (raw, expected) => {
    expect(format('future_metric', raw)).toBe(expected)
  })

  it('skips an unknown null field and keeps the order of the unknown fields', () => {
    const keys = buildGlobalMetrics({ zeta: 1, alpha: null, beta: 2, state: 'Running' }).map(
      (e) => e.key
    )
    expect(keys).toEqual(['state', 'zeta', 'beta'])
  })

  it('does not throw for any JSON type in any field', () => {
    const values: unknown[] = [null, 0, -1, 1.5, '', 'x', true, false, [], [1], {}, { a: null }]
    for (const key of [...Object.keys(metricsFromSpec()), 'future_metric']) {
      for (const raw of values) {
        expect(
          () => buildGlobalMetrics({ [key]: raw }),
          `${key} = ${JSON.stringify(raw)}`
        ).not.toThrow()
      }
    }
  })
})
