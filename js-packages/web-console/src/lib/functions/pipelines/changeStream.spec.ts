/**
 * Unit tests covering the two regressions the change-stream rewrite was meant to fix:
 *
 *  1. **Parsing correctness under overflow shedding.** The original implementation dropped
 *     arbitrary byte ranges mid-record, which sometimes produced syntactically valid
 *     but structurally broken JSON and crashed the UI. The current parser must only
 *     ever see complete top-level documents.
 *  2. **Section header preservation under high throughput.** The original
 *     `pushAsCircularBuffer`-based push lost the only section header when rows shifted
 *     off the front, leaving orphan data rows. `appendRowsForRelation` must keep every
 *     surviving section preceded by its header.
 */

import { BigNumber } from 'bignumber.js'
import { describe, expect, it } from 'vitest'
import type { ChangeStreamData, Row } from '$lib/components/pipelines/editor/ChangeStream.svelte'
import type { XgressEntry } from '$lib/services/pipelineManager'
import {
  appendRowsForRelation,
  createBigNumberStreamParser,
  parseStream,
  type StreamingJsonParser
} from './changeStream'

// --- Mock stream factory ---

const makeMockStream = (chunks: (Uint8Array | string)[]): ReadableStream<Uint8Array> => {
  const encoder = new TextEncoder()
  let i = 0
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (i < chunks.length) {
        const c = chunks[i++]
        controller.enqueue(typeof c === 'string' ? encoder.encode(c) : c)
      } else {
        controller.close()
      }
    }
  })
}

// Runs `parseStream` over `chunks` and resolves the returned Promise once the stream has ended and the
// last flush has fired. Returns everything the consumer would have observed.
const runParseStream = <T>(
  chunks: (Uint8Array | string)[],
  parserOpts: Parameters<typeof createBigNumberStreamParser<T>>[0] = {
    paths: ['$'],
    separator: ''
  },
  options?: Parameters<typeof parseStream<T>>[3]
) =>
  new Promise<{
    values: T[]
    skipped: number[]
    parser: StreamingJsonParser<T>
  }>((resolve) => {
    const values: T[] = []
    const skipped: number[] = []
    const parser = createBigNumberStreamParser<T>(parserOpts)
    parseStream<T>(
      { stream: makeMockStream(chunks), cancel: () => {} },
      parser,
      {
        pushChanges: (vs) => {
          for (const v of vs) {
            values.push(v)
          }
        },
        onBytesSkipped: (n) => skipped.push(n),
        onParseEnded: () => resolve({ values, skipped, parser })
      },
      options
    )
  })

// --- Row / Header builders ---

type DataRow = Extract<Row, { insert: unknown } | { delete: unknown }>

const insertRow = (data: Record<string, unknown>): DataRow =>
  ({ insert: data }) as unknown as DataRow

// A header row is `{ relationName, columns: Field[] }`. We don't exercise the Field
// shape in any of these tests, so `[] as unknown as Field[]` is fine.
const headerRow = (relationName: string): Row =>
  ({ relationName, columns: [] as unknown[] }) as unknown as Row

const skipRow = (relationName: string, skippedBytes: number): Row =>
  ({ relationName, skippedBytes }) as unknown as Row

const isHeader = (row: Row | undefined): boolean => !!row && 'columns' in (row as object)

// A header-builder usable by the function-under-test that just returns a header for
// the relation the test is exercising. The optional `sample` argument is ignored.
const stubHeaderBuilder = (relationName: string) => () => headerRow(relationName)

const emptyChangeStream = (): ChangeStreamData => ({
  rows: [],
  headers: [],
  totalSkippedBytes: 0
})

describe('parseStream', () => {
  it('parses NDJSON delivered in a single network chunk', async () => {
    const { values, skipped } = await runParseStream<{ a: BigNumber }>([
      '{"a":1}\n{"a":2}\n{"a":3}\n'
    ])
    expect(values.map((v) => v.a.toString())).toEqual(['1', '2', '3'])
    expect(skipped).toEqual([])
  })

  it('parses a line that straddles two network chunks', async () => {
    const { values, skipped } = await runParseStream<{ a: BigNumber }>([
      '{"a":1}\n{"a":',
      '2}\n{"a":3}\n'
    ])
    expect(values.map((v) => v.a.toString())).toEqual(['1', '2', '3'])
    expect(skipped).toEqual([])
  })

  it('does not emit a trailing partial line that lacks a newline', async () => {
    const { values, skipped } = await runParseStream<{ a: BigNumber }>(['{"a":1}\n{"a":2}'])
    expect(values.map((v) => v.a.toString())).toEqual(['1'])
    // The dangling `{"a":2}` is silently discarded (no skipped-bytes report); it's
    // the connection-was-cut case, not an overflow-shedding drop.
    expect(skipped).toEqual([])
  })

  it('handles CRLF line terminators (the format the server actually emits)', async () => {
    const { values, skipped } = await runParseStream<{ a: BigNumber }>([
      '{"a":1}\r\n{"a":2}\r\n{"a":3}\r\n'
    ])
    expect(values.map((v) => v.a.toString())).toEqual(['1', '2', '3'])
    expect(skipped).toEqual([])
  })

  it('skips malformed JSON without poisoning subsequent values', async () => {
    // The bad line is in its own chunk so it's parsed as an isolated piece. This is
    // the regression test for the original crash: a parse failure must never let a
    // partially-built value leak into `pushChanges`.
    const { values, skipped } = await runParseStream<{ a: BigNumber }>([
      '{"a":1}\n',
      '{not valid json}\n',
      '{"a":3}\n'
    ])
    expect(values.map((v) => v.a.toString())).toEqual(['1', '3'])
    expect(skipped.length).toBeGreaterThan(0)
    // Each emitted value is a real object — no partials, no garbage.
    for (const v of values) {
      expect(v).toBeInstanceOf(Object)
      expect(BigNumber.isBigNumber(v.a)).toBe(true)
    }
  })

  it('preserves arbitrary-precision numbers as BigNumber', async () => {
    const huge = '9999999999999999999.999'
    const { values } = await runParseStream<{ v: BigNumber }>([`{"v":${huge}}\n`])
    expect(values.length).toBe(1)
    expect(BigNumber.isBigNumber(values[0].v)).toBe(true)
    // JS Number would coerce this to 1e19; BigNumber preserves it verbatim.
    expect(values[0].v.toFixed()).toBe(huge)
  })
})

describe('appendRowsForRelation', () => {
  it('adds a section header for the first batch into an empty stream', () => {
    const cs = emptyChangeStream()
    const data = [insertRow({ id: 1 }), insertRow({ id: 2 }), insertRow({ id: 3 })]
    appendRowsForRelation(cs, 'rel_A', data, stubHeaderBuilder('rel_A'), 100)
    expect(cs.headers).toEqual([0])
    expect(cs.rows.length).toBe(4)
    expect(isHeader(cs.rows[0])).toBe(true)
    expect((cs.rows[0] as { relationName: string }).relationName).toBe('rel_A')
  })

  it('does not duplicate the header when continuing the same relation', () => {
    const cs = emptyChangeStream()
    appendRowsForRelation(
      cs,
      'rel_A',
      [insertRow({ id: 1 }), insertRow({ id: 2 }), insertRow({ id: 3 })],
      stubHeaderBuilder('rel_A'),
      100
    )
    appendRowsForRelation(
      cs,
      'rel_A',
      [insertRow({ id: 4 }), insertRow({ id: 5 })],
      stubHeaderBuilder('rel_A'),
      100
    )
    expect(cs.headers).toEqual([0])
    expect(cs.rows.length).toBe(6)
  })

  it('adds a new section header when the relation changes', () => {
    const cs = emptyChangeStream()
    appendRowsForRelation(
      cs,
      'rel_A',
      [insertRow({ id: 1 }), insertRow({ id: 2 }), insertRow({ id: 3 })],
      stubHeaderBuilder('rel_A'),
      100
    )
    appendRowsForRelation(
      cs,
      'rel_B',
      [insertRow({ x: 'a' }), insertRow({ x: 'b' })],
      stubHeaderBuilder('rel_B'),
      100
    )
    expect(cs.headers).toEqual([0, 4])
    expect(cs.rows.length).toBe(7)
    expect((cs.rows[4] as { relationName: string }).relationName).toBe('rel_B')
  })

  it('re-inserts the dropped header when the front shift would orphan the section', () => {
    // Drift regression: bufferSize=10, fill it with a header + 9 data rows, push 5
    // more for the same relation. The shift evicts the original header at index 0
    // along with 4 data rows; without the fix, the remaining 5 old rows and the 5
    // new rows would render with no section header above them.
    const cs = emptyChangeStream()
    const initial = Array.from({ length: 9 }, (_, i) => insertRow({ id: i }))
    appendRowsForRelation(cs, 'rel_A', initial, stubHeaderBuilder('rel_A'), 10)
    expect(cs.headers).toEqual([0])
    expect(cs.rows.length).toBe(10)

    const more = Array.from({ length: 5 }, (_, i) => insertRow({ id: 100 + i }))
    appendRowsForRelation(cs, 'rel_A', more, stubHeaderBuilder('rel_A'), 10)

    expect(cs.headers).toEqual([0])
    expect(isHeader(cs.rows[0])).toBe(true)
    expect((cs.rows[0] as { relationName: string }).relationName).toBe('rel_A')
    // Soft cap: every cycle that drops a header re-inserts one, so the buffer can
    // sit at bufferSize+1 in steady state. What matters is the invariant: rows[0]
    // is always a header.
    expect(cs.rows.length).toBeLessThanOrEqual(11)
    // No row past index 0 is a header — there's exactly one section.
    expect(cs.headers.length).toBe(1)
  })

  it('keeps a header at the front when a single batch overflows the buffer', () => {
    // Overflow regression: pushing more rows than fit must still leave a header at
    // rows[0]. Previously the prepended header was the *first* item of the values
    // array, so `values.slice(-bufferSize)` dropped it along with the overflowing
    // tail and the kept rows were left orphan.
    const cs = emptyChangeStream()
    const data = Array.from({ length: 50 }, (_, i) => insertRow({ id: i }))
    appendRowsForRelation(cs, 'rel_A', data, stubHeaderBuilder('rel_A'), 10)

    expect(cs.headers).toEqual([0])
    expect(cs.rows.length).toBe(10)
    expect(isHeader(cs.rows[0])).toBe(true)
    expect((cs.rows[0] as { relationName: string }).relationName).toBe('rel_A')

    // The 9 kept data rows must be the LATEST 9 of the input (ids 41..49), not the
    // earliest — we drop oldest, keep newest.
    const keptIds = (cs.rows.slice(1) as unknown as { insert: { id: number } }[]).map(
      (r) => r.insert.id
    )
    expect(keptIds).toEqual([41, 42, 43, 44, 45, 46, 47, 48, 49])
  })

  it('re-inserts the most-recent dropped header when multiple sections drop at once', () => {
    // Pre-build state: [header_A, 5×A, header_B, 5×B] = 12 rows, headers=[0, 6].
    // With bufferSize=15 and a new C-batch sized so dropCount=7, both A and B
    // headers are dropped from the front; the orphan rows that remain belonged to
    // B (rows at old indices 7..11), so the helper must re-insert B's header — not
    // A's — at the front of the kept tail.
    const cs = emptyChangeStream()
    appendRowsForRelation(
      cs,
      'rel_A',
      Array.from({ length: 5 }, (_, i) => insertRow({ id: i })),
      stubHeaderBuilder('rel_A'),
      15
    )
    appendRowsForRelation(
      cs,
      'rel_B',
      Array.from({ length: 5 }, (_, i) => insertRow({ id: 100 + i })),
      stubHeaderBuilder('rel_B'),
      15
    )
    expect(cs.headers).toEqual([0, 6])
    expect(cs.rows.length).toBe(12)

    appendRowsForRelation(
      cs,
      'rel_C',
      Array.from({ length: 9 }, (_, i) => insertRow({ id: 200 + i })),
      stubHeaderBuilder('rel_C'),
      15
    )

    // dropCount = 12 + (1 + 9) - 15 = 7, evicting header_A + 5 A_rows + header_B.
    // All 5 B data rows survive at old indices 7..11 → after splice(0,7) at new
    // 0..4 → after unshift(header_B) at new 1..5.
    // Expected layout:
    //   rows[0]  = header_B (re-inserted; most-recent dropped header)
    //   rows[1..5] = the 5 surviving B data rows (ids 100..104)
    //   rows[6]  = header_C (new section)
    //   rows[7..15] = the 9 new C data rows
    expect(cs.headers).toEqual([0, 6])
    expect(cs.rows.length).toBe(16)
    expect((cs.rows[0] as { relationName: string }).relationName).toBe('rel_B')
    expect((cs.rows[6] as { relationName: string }).relationName).toBe('rel_C')
    // Confirm the orphans-under-B are actually B's data rows, not A's.
    const orphanIds = (cs.rows.slice(1, 6) as unknown as { insert: { id: number } }[]).map(
      (r) => r.insert.id
    )
    expect(orphanIds).toEqual([100, 101, 102, 103, 104])
  })

  it('inserts a header before a skip marker when the relation changed', () => {
    const cs = emptyChangeStream()
    appendRowsForRelation(
      cs,
      'rel_A',
      [insertRow({ id: 1 }), insertRow({ id: 2 }), insertRow({ id: 3 })],
      stubHeaderBuilder('rel_A'),
      100
    )
    // Skip marker arrives for a different relation — must get its own header so
    // the marker doesn't render under the wrong section.
    appendRowsForRelation(cs, 'rel_B', [skipRow('rel_B', 512)], stubHeaderBuilder('rel_B'), 100)
    expect(cs.headers).toEqual([0, 4])
    expect(cs.rows.length).toBe(6)
    expect((cs.rows[4] as { relationName: string }).relationName).toBe('rel_B')
    expect('skippedBytes' in (cs.rows[5] as object)).toBe(true)
  })

  it('does not add a header for a skip marker in the current relation', () => {
    const cs = emptyChangeStream()
    appendRowsForRelation(
      cs,
      'rel_A',
      [insertRow({ id: 1 }), insertRow({ id: 2 }), insertRow({ id: 3 })],
      stubHeaderBuilder('rel_A'),
      100
    )
    appendRowsForRelation(cs, 'rel_A', [skipRow('rel_A', 512)], stubHeaderBuilder('rel_A'), 100)
    expect(cs.headers).toEqual([0])
    expect(cs.rows.length).toBe(5)
    expect('skippedBytes' in (cs.rows[4] as object)).toBe(true)
  })
})
