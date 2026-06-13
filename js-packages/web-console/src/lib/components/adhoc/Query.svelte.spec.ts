/**
 * Component test for the adhoc-query result table. Builds an Arrow IPC stream
 * that covers every distinct return-shape of `arrowIpcValueToJS`
 * (BigNumber-wrapped, Dayjs-wrapped, pass-through), runs the full
 * stream → RecordBatchReader → arrowIpcBatchToJS pipeline that
 * TabAdHocQuery uses, and renders the resulting row through Query.svelte.
 *
 * The test deliberately puts all types in a single row so a regression in any
 * one branch of the `match` in `arrowIpcValueToJS` is caught here.
 *
 * Note on Int64/Uint64: apache-arrow's IPC reader recreates Int columns as
 * the base `Int_` class with bitWidth=64 and `typeId === Type.Int`, never as
 * the granular `Int64`/`Uint64` subclasses. The Int64/Uint64 cases listed in
 * `arrowIpcValueToJS` therefore never fire at runtime — 64-bit ints flow
 * through the pass-through branch as `bigint`. The Decimal column below
 * exercises the BigNumber branch.
 */
import {
  Binary,
  Bool,
  type DataType,
  DateMillisecond,
  Decimal,
  Field,
  Float64,
  Int32,
  Int64,
  List,
  Map_,
  makeBuilder,
  Null,
  RecordBatchReader,
  RecordBatchStreamWriter,
  Struct,
  Table,
  TimeMicrosecond,
  TimeMillisecond,
  TimeNanosecond,
  TimestampMicrosecond,
  TimestampMillisecond,
  Uint64,
  Utf8,
  type Vector
} from 'apache-arrow'
import { BigNumber } from 'bignumber.js'
import Dayjs, { isDayjs } from 'dayjs'
import { describe, expect, it } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import Query, { type Row } from '$lib/components/adhoc/Query.svelte'
import { arrowIpcBatchToJS, arrowSchemaToFelderaFields } from '$lib/functions/apacheArrow'
import { enclosure } from '$lib/functions/common/function'
import type { SQLValueJS } from '$lib/types/sql'

// Wraps a single value into a typed Arrow Vector.
const buildVector = <T extends DataType>(type: T, value: unknown): Vector<T> => {
  const builder = makeBuilder<T>({ type })
  builder.append(value)
  builder.finish()
  return builder.toVector()
}

// Pack an unsigned bigint into a little-endian Uint32Array of `limbs` 32-bit
// words. Used to feed the Decimal builder, which sets values via
// `subarray(0, stride)` over its raw byte buffer.
const decimalLimbsLE = (value: bigint, limbs: number): Uint32Array => {
  const out = new Uint32Array(limbs)
  const mask = 0xffffffffn
  let v = value
  for (let i = 0; i < limbs; i++) {
    out[i] = Number(v & mask)
    v >>= 32n
  }
  return out
}

const ipcStreamFromTable = (table: Table): ReadableStream<Uint8Array> => {
  const writer = RecordBatchStreamWriter.writeAll(table)
  const bytes = writer.toUint8Array(true) as unknown as Uint8Array
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(bytes)
      controller.close()
    }
  })
}

describe('adhoc-query Query.svelte table — arrowIpcValueToJS serialization', () => {
  it('decodes and renders every return-shape of arrowIpcValueToJS in one row', async () => {
    const tsMs = Date.UTC(2026, 4, 19, 12, 34, 56) // 2026-05-19T12:34:56Z

    // One column per distinct value-shape returned by arrowIpcValueToJS.
    // Column order is preserved through the IPC roundtrip, so the destructure
    // below aligns positionally with this object's keys.
    const table = new Table({
      bool_col: buildVector(new Bool(), true),
      int32_col: buildVector(new Int32(), 42),
      float64_col: buildVector(new Float64(), 3.5),
      utf8_col: buildVector(new Utf8(), 'hello'),
      binary_col: buildVector(new Binary(), new Uint8Array([0xde, 0xad])),
      int64_col: buildVector(new Int64(), 9007199254740993n),
      uint64_col: buildVector(new Uint64(), 18446744073709551610n),
      decimal_col: buildVector(new Decimal(0, 20, 128), decimalLimbsLE(123456789n, 4)),
      date_ms_col: buildVector(new DateMillisecond(), tsMs),
      timestamp_ms_col: buildVector(new TimestampMillisecond(), tsMs),
      time_ms_col: buildVector(new TimeMillisecond(), 45_296_000), // 12:34:56
      null_col: buildVector(new Null(), null),
      // Struct's constructor is parameterised by a uniform child type;
      // mixed-type children need an `any` annotation to typecheck.
      struct_col: buildVector(
        new Struct<any>([new Field('a', new Int32(), true), new Field('b', new Utf8(), true)]),
        { a: 7, b: 'foo' }
      ),
      list_col: buildVector(new List(new Field('item', new Int32(), true)), [10, 20, 30]),
      map_col: buildVector(
        new Map_(
          new Field(
            'entries',
            new Struct<any>([
              new Field('key', new Utf8(), false),
              new Field('value', new Int32(), true)
            ]),
            false
          )
        ),
        { k1: 42 }
      ),
      map_int_key_col: buildVector(
        new Map_(
          new Field(
            'entries',
            new Struct<any>([
              new Field('key', new Int32(), false),
              new Field('value', new Utf8(), true)
            ]),
            false
          )
        ),
        new Map([[1, 'one']])
      ),
      map_bool_key_col: buildVector(
        new Map_(
          new Field(
            'entries',
            new Struct<any>([
              new Field('key', new Bool(), false),
              new Field('value', new Int32(), true)
            ]),
            false
          )
        ),
        new Map([[true, 100]])
      ),
      map_struct_key_col: buildVector(
        new Map_(
          new Field(
            'entries',
            new Struct<any>([
              new Field(
                'key',
                new Struct<any>([
                  new Field('x', new Int32(), false),
                  new Field('y', new Int32(), false)
                ]),
                false
              ),
              new Field('value', new Utf8(), true)
            ]),
            false
          )
        ),
        new Map([[{ x: 3, y: 4 }, 'val']])
      ),
      map_date_key_col: buildVector(
        new Map_(
          new Field(
            'entries',
            new Struct<any>([
              new Field('key', new DateMillisecond(), false),
              new Field('value', new Int32(), true)
            ]),
            false
          )
        ),
        new Map([[tsMs, 99]])
      )
    })

    const reader = await RecordBatchReader.from(ipcStreamFromTable(table))
    await reader.open()
    const columns = arrowSchemaToFelderaFields(reader.schema)

    const decoded: { row: SQLValueJS[] }[] = []
    for await (const batch of reader) {
      decoded.push(...arrowIpcBatchToJS(batch))
    }

    expect(decoded).toHaveLength(1)
    const [
      bool,
      int32,
      float64,
      utf8,
      binary,
      int64,
      uint64,
      decimal,
      dateMs,
      timestampMs,
      timeMs,
      nullValue,
      struct,
      list,
      map,
      mapIntKey,
      mapBoolKey,
      mapStructKey,
      mapDateKey
    ] = decoded[0].row

    // Pass-through branch (typeId not specially matched).
    expect(bool).toBe(true)
    expect(int32).toBe(42)
    expect(float64).toBe(3.5)
    expect(utf8).toBe('hello')
    expect(binary).toBeInstanceOf(Uint8Array)
    expect(Array.from(binary as unknown as Uint8Array)).toEqual([0xde, 0xad])
    // 64-bit ints survive as bigint (see header note for why the BigNumber
    // branch does not fire for these).
    expect(typeof int64).toBe('bigint')
    expect(int64).toBe(9007199254740993n)
    expect(typeof uint64).toBe('bigint')
    expect(uint64).toBe(18446744073709551610n)

    // BigNumber branch — exercised through Decimal, the only `typeId` in the
    // case list that is actually emitted at runtime.
    expect(BigNumber.isBigNumber(decimal)).toBe(true)
    expect((decimal as BigNumber).toFixed()).toBe('123456789')

    // Dayjs branch: Date/Time/Timestamp at every granularity wrap the same way.
    expect(isDayjs(dateMs)).toBe(true)
    expect((dateMs as Dayjs.Dayjs).valueOf()).toBe(tsMs)
    expect(isDayjs(timestampMs)).toBe(true)
    expect((timestampMs as Dayjs.Dayjs).valueOf()).toBe(tsMs)
    expect(isDayjs(timeMs)).toBe(true)

    // Composite / null branches all pass through; SQLValue.svelte serialises
    // them via JSONbig.stringify (Struct/List/Map expose `toJSON`).
    expect(nullValue).toBeNull()
    expect((struct as { toJSON(): unknown }).toJSON()).toEqual({ a: 7, b: 'foo' })
    expect((list as { toJSON(): unknown }).toJSON()).toEqual([10, 20, 30])
    expect((map as { toJSON(): unknown }).toJSON()).toEqual({ k1: 42 })
    // Non-string map keys are coerced to strings via their toString()/ToPrimitive.
    // Int32 key 1 → "1", Bool key true → "true", Struct key via StructRow.toString()
    // → '{"x": 3, "y": 4}', DateMillisecond key → the raw ms number as a string.
    expect((mapIntKey as { toJSON(): unknown }).toJSON()).toEqual({ 1: 'one' })
    expect((mapBoolKey as { toJSON(): unknown }).toJSON()).toEqual({ true: 100 })
    expect((mapStructKey as { toJSON(): unknown }).toJSON()).toEqual({ '{"x": 3, "y": 4}': 'val' })
    expect((mapDateKey as { toJSON(): unknown }).toJSON()).toEqual({ [tsMs]: 99 })

    // Now drive the rendered table component with the decoded row to confirm
    // every cell flows through SQLValue without throwing and produces
    // recognisable text.
    const rows: Row[] = [{ cells: decoded[0].row }]
    render(Query, {
      query: 'SELECT * FROM all_types',
      result: {
        rows: enclosure(rows),
        columns,
        totalSkippedBytes: 0,
        endResultStream: () => {}
      },
      progress: false,
      onSubmitQuery: () => {},
      onDeleteQuery: () => {},
      disabled: false,
      isLastQuery: true
    })

    // Column headers come from arrowSchemaToFelderaFields.
    await expect.element(page.getByText('bool_col')).toBeInTheDocument()
    await expect.element(page.getByText('decimal_col')).toBeInTheDocument()
    await expect.element(page.getByText('timestamp_ms_col')).toBeInTheDocument()

    // Wait for the row to render before snapshotting the DOM.
    await expect.element(page.getByText('hello')).toBeInTheDocument()

    // Each data cell, in declared column order. The leading '0' is the row
    // index column injected by Query.svelte. The trailing values exercise
    // every rendering path in SQLValue.svelte:
    //   - primitives → JSONbig.stringify (`true`, `42`, `3.5`)
    //   - string → trimmed verbatim (`hello`)
    //   - Uint8Array (binary) → lowercase hex (`dead`)
    //   - plain object / list → indented JSON
    //   - bigint → JSONbig.stringify writes the digits unquoted
    //   - BigNumber → toFixed(3, ROUND_DOWN) with trailing zeros stripped
    //   - Dayjs → JSON.stringify of its toJSON() output (ISO string in quotes)
    const tdTexts = Array.from(document.querySelectorAll('tbody tr td')).map(
      (td) => td.textContent ?? ''
    )
    expect(tdTexts).toEqual([
      '0',
      'true',
      '42',
      '3.5',
      'hello',
      // Binary renders as a lowercase hex string (0xde 0xad).
      'dead',
      '9007199254740993',
      '18446744073709551610',
      '123456789',
      '"2026-05-19T12:34:56.000Z"',
      '"2026-05-19T12:34:56.000Z"',
      '"1970-01-01T12:34:56.000Z"',
      'NULL',
      '{\n "a": 7,\n "b": "foo"\n}',
      '[\n 10,\n 20,\n 30\n]',
      '{\n "k1": 42\n}',
      '{\n "1": "one"\n}',
      '{\n "true": 100\n}',
      '{\n "{\\"x\\": 3, \\"y\\": 4}": "val"\n}',
      `{\n "${tsMs}": 99\n}`
    ])

    // The null cell is rendered in italics; non-null cells are not. The
    // SQLValue.svelte template toggles the `italic` class on `value === null`.
    const tds = Array.from(document.querySelectorAll('tbody tr td')) as HTMLTableCellElement[]
    const nullCellIdx = tdTexts.indexOf('NULL')
    expect(tds[nullCellIdx].classList.contains('italic')).toBe(true)
    expect(tds[tdTexts.indexOf('hello')].classList.contains('italic')).toBe(false)
  })

  // Regression: the server emits SQL TIME as Arrow Time64(ns) and SQL DECIMAL
  // as Decimal128(precision, scale). The Time64 getter returns a `bigint` count
  // since midnight, which used to be passed straight to Dayjs and threw
  // "can't convert BigInt to number"; the Decimal getter returns the *unscaled*
  // integer, which used to render without the decimal point. Both must now
  // decode correctly.
  it('decodes Time64(micro/nano) and scaled Decimal as the server emits them', async () => {
    // Pack an unsigned bigint into little-endian 32-bit limbs for the Decimal
    // builder (reused from the helper above's contract: 128-bit → 4 limbs).
    const decLimbs = (value: bigint, limbs: number): Uint32Array => {
      const out = new Uint32Array(limbs)
      const mask = 0xffffffffn
      let v = value
      for (let i = 0; i < limbs; i++) {
        out[i] = Number(v & mask)
        v >>= 32n
      }
      return out
    }

    // 12:34:56.789 expressed in each Time64 unit.
    const nanosSinceMidnight = 45_296_789_000_000n
    const microsSinceMidnight = 45_296_789_000n

    const table = new Table({
      time_ns_col: buildVector(new TimeNanosecond(), nanosSinceMidnight),
      time_us_col: buildVector(new TimeMicrosecond(), microsSinceMidnight),
      // DECIMAL(38, 10) carrying 123456789012345678.9012345678 — its unscaled
      // 128-bit integer is 1234567890123456789012345678.
      decimal_scaled_col: buildVector(
        new Decimal(10, 38, 128),
        decLimbs(1234567890123456789012345678n, 4)
      ),
      // Timestamp(µs) — the server's TIMESTAMP precision; getter normalises to ms.
      timestamp_us_col: buildVector(
        new TimestampMicrosecond(),
        Date.UTC(2024, 0, 15, 12, 34, 56, 789)
      )
    })

    const reader = await RecordBatchReader.from(ipcStreamFromTable(table))
    await reader.open()

    const decoded: { row: SQLValueJS[] }[] = []
    for await (const batch of reader) {
      decoded.push(...arrowIpcBatchToJS(batch))
    }

    expect(decoded).toHaveLength(1)
    const [timeNs, timeUs, decimalScaled, timestampUs] = decoded[0].row

    // Time64 columns decode to the same wall-clock instant without throwing.
    expect(isDayjs(timeNs)).toBe(true)
    expect((timeNs as Dayjs.Dayjs).toISOString()).toBe('1970-01-01T12:34:56.789Z')
    expect(isDayjs(timeUs)).toBe(true)
    expect((timeUs as Dayjs.Dayjs).toISOString()).toBe('1970-01-01T12:34:56.789Z')

    // Decimal applies its scale (no precision loss for 28-digit values).
    expect(BigNumber.isBigNumber(decimalScaled)).toBe(true)
    expect((decimalScaled as BigNumber).toFixed()).toBe('123456789012345678.9012345678')

    expect(isDayjs(timestampUs)).toBe(true)
    expect((timestampUs as Dayjs.Dayjs).toISOString()).toBe('2024-01-15T12:34:56.789Z')
  })

  // `arrowSchemaToFelderaFields` recursively recovers nested element types so a
  // column header can render `INTEGER ARRAY` rather than a bare `ARRAY`. The
  // Arrow schema retains the full child tree across the IPC roundtrip.
  it('recovers nested array element types into ColumnType.component', async () => {
    const table = new Table({
      int_array: buildVector(new List(new Field('item', new Int32(), true)), [1, 2, 3]),
      int_array_2d: buildVector(
        new List(new Field('item', new List(new Field('item', new Int32(), true)), true)),
        [[1, 2]]
      ),
      decimal_array: buildVector(new List(new Field('item', new Decimal(10, 38, 128), true)), [
        decimalLimbsLE(5n, 4)
      ]),
      top_decimal: buildVector(new Decimal(10, 38, 128), decimalLimbsLE(5n, 4))
    })

    const reader = await RecordBatchReader.from(ipcStreamFromTable(table))
    await reader.open()
    const [intArray, intArray2d, decimalArray, topDecimal] = arrowSchemaToFelderaFields(
      reader.schema
    )

    // ARRAY → element type lives in `component` (recursively for nested arrays).
    expect(intArray.columntype.type).toBe('ARRAY')
    expect(intArray.columntype.component?.type).toBe('INTEGER')

    expect(intArray2d.columntype.type).toBe('ARRAY')
    expect(intArray2d.columntype.component?.type).toBe('ARRAY')
    expect(intArray2d.columntype.component?.component?.type).toBe('INTEGER')

    // Leaf precision/scale survive through the element type.
    expect(decimalArray.columntype.component?.type).toBe('DECIMAL')
    expect(decimalArray.columntype.component?.precision).toBe(38)
    expect(decimalArray.columntype.component?.scale).toBe(10)

    // Top-level Decimal also carries precision/scale.
    expect(topDecimal.columntype.type).toBe('DECIMAL')
    expect(topDecimal.columntype.precision).toBe(38)
    expect(topDecimal.columntype.scale).toBe(10)
  })
})
