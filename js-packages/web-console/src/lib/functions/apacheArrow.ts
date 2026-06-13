import type {
  Field as ArrowField,
  Schema as ArrowSchema,
  DataType,
  RecordBatch,
  TypeMap
} from 'apache-arrow'
import { Precision, TimeUnit, Type } from 'apache-arrow'
import BigNumber from 'bignumber.js'
import Dayjs from 'dayjs'
import invariant from 'tiny-invariant'
import { match } from 'ts-pattern'
import type { ColumnType, Field, SqlType } from '$lib/services/manager'
import type { SQLValueJS } from '$lib/types/sql'

function enumValue<const T>(
  value: T
): `${Extract<T, number>}` extends `${infer N extends number}` ? N : T {
  return value as `${Extract<T, number>}` extends `${infer N extends number}` ? N : T
}

// Time-of-day Arrow type ids. Unlike Timestamp/Date — whose getters already
// normalise to epoch milliseconds — a Time getter returns the raw count in the
// column's declared unit, and the micro/nanosecond variants return a `bigint`.
const TIME_TYPE_IDS: Type[] = [
  Type.Time,
  Type.TimeSecond,
  Type.TimeMillisecond,
  Type.TimeMicrosecond,
  Type.TimeNanosecond
]

/**
 * Normalise an Arrow temporal value to epoch milliseconds for Dayjs.
 *
 * Dayjs only accepts a `number`; handing it a `bigint` throws
 * "can't convert BigInt to number". Time64 columns (microsecond/nanosecond
 * precision) come back as a bigint count since midnight, so they must be
 * scaled down to milliseconds. Timestamp and Date getters already yield
 * milliseconds, so they pass through unchanged.
 */
const temporalToMs = (type: DataType, value: number | bigint): number => {
  const ms = typeof value === 'bigint' ? Number(value) : value
  if (!TIME_TYPE_IDS.includes(type.typeId)) {
    return ms
  }
  switch ((type as { unit?: TimeUnit }).unit) {
    case TimeUnit.SECOND:
      return ms * 1000
    case TimeUnit.MICROSECOND:
      return ms / 1000
    case TimeUnit.NANOSECOND:
      return ms / 1_000_000
    case TimeUnit.MILLISECOND:
    default:
      return ms
  }
}

const arrowIpcValueToJS = <T extends { typeId: Type }, Data extends DataType<T['typeId']>>(
  arrowType: ArrowField<Data>,
  value: any
) => {
  if (value === null) {
    return null
  }
  return match(arrowType.typeId)
    .with(enumValue(Type.Decimal), () =>
      // Arrow hands back the unscaled 128/256-bit integer; recover the true
      // fixed-point value by shifting in the column's scale (lossless, unlike
      // the BigNum's lossy `valueOf`).
      new BigNumber(value.toString()).shiftedBy(
        -((arrowType.type as { scale?: number }).scale ?? 0)
      )
    )
    .with(enumValue(Type.Int64), enumValue(Type.Uint64), () => new BigNumber(value.toString()))
    .with(
      enumValue(Type.Date),
      enumValue(Type.Time),
      enumValue(Type.Timestamp),
      enumValue(Type.DateDay),
      enumValue(Type.DateMillisecond),
      enumValue(Type.TimestampSecond),
      enumValue(Type.TimestampMillisecond),
      enumValue(Type.TimestampMicrosecond),
      enumValue(Type.TimestampNanosecond),
      enumValue(Type.TimeSecond),
      enumValue(Type.TimeMillisecond),
      enumValue(Type.TimeMicrosecond),
      enumValue(Type.TimeNanosecond),
      () => Dayjs(temporalToMs(arrowType.type, value))
    )
    .with(
      enumValue(Type.NONE),
      enumValue(Type.Null),
      enumValue(Type.Int),
      enumValue(Type.Float),
      enumValue(Type.Binary),
      enumValue(Type.Utf8),
      enumValue(Type.Bool),
      enumValue(Type.Interval),
      enumValue(Type.List),
      enumValue(Type.Struct),
      enumValue(Type.Union),
      enumValue(Type.FixedSizeBinary),
      enumValue(Type.FixedSizeList),
      enumValue(Type.Map),
      enumValue(Type.Duration),
      enumValue(Type.LargeBinary),
      enumValue(Type.LargeUtf8),
      enumValue(Type.LargeList),
      enumValue(Type.Dictionary),
      enumValue(Type.BinaryView),
      enumValue(Type.Utf8View),
      enumValue(Type.Int8),
      enumValue(Type.Int16),
      enumValue(Type.Int32),
      enumValue(Type.Uint8),
      enumValue(Type.Uint16),
      enumValue(Type.Uint32),
      enumValue(Type.Float16),
      enumValue(Type.Float32),
      enumValue(Type.Float64),
      enumValue(Type.DenseUnion),
      enumValue(Type.SparseUnion),
      enumValue(Type.IntervalDayTime),
      enumValue(Type.IntervalYearMonth),
      enumValue(Type.DurationSecond),
      enumValue(Type.DurationMillisecond),
      enumValue(Type.DurationMicrosecond),
      enumValue(Type.DurationNanosecond),
      enumValue(Type.IntervalMonthDayNano),
      () => value
    )
    .exhaustive()
}

/**
 * Map an arrow `Type` to the closest Feldera `SqlType` for display purposes.
 * Returns `undefined` when no Feldera-side equivalent exists — the SQL column
 * header renderer treats that as "no type annotation".
 */
const arrowTypeIdToSqlType = (typeId: Type): SqlType | undefined =>
  match(typeId)
    .returnType<SqlType | undefined>()
    .with(enumValue(Type.Bool), () => 'BOOLEAN')
    .with(enumValue(Type.Int8), () => 'TINYINT')
    .with(enumValue(Type.Int16), () => 'SMALLINT')
    .with(enumValue(Type.Int32), enumValue(Type.Int), () => 'INTEGER')
    .with(enumValue(Type.Int64), () => 'BIGINT')
    .with(enumValue(Type.Uint8), () => 'UTINYINT')
    .with(enumValue(Type.Uint16), () => 'USMALLINT')
    .with(enumValue(Type.Uint32), () => 'UINTEGER')
    .with(enumValue(Type.Uint64), () => 'UBIGINT')
    .with(enumValue(Type.Float32), enumValue(Type.Float16), () => 'REAL')
    .with(enumValue(Type.Float64), enumValue(Type.Float), () => 'DOUBLE')
    .with(enumValue(Type.Decimal), () => 'DECIMAL')
    .with(
      enumValue(Type.Utf8),
      enumValue(Type.LargeUtf8),
      enumValue(Type.Utf8View),
      () => 'VARCHAR'
    )
    .with(
      enumValue(Type.Binary),
      enumValue(Type.LargeBinary),
      enumValue(Type.FixedSizeBinary),
      enumValue(Type.BinaryView),
      () => 'VARBINARY'
    )
    .with(
      enumValue(Type.Date),
      enumValue(Type.DateDay),
      enumValue(Type.DateMillisecond),
      () => 'DATE'
    )
    .with(
      enumValue(Type.Time),
      enumValue(Type.TimeSecond),
      enumValue(Type.TimeMillisecond),
      enumValue(Type.TimeMicrosecond),
      enumValue(Type.TimeNanosecond),
      () => 'TIME'
    )
    .with(
      enumValue(Type.Timestamp),
      enumValue(Type.TimestampSecond),
      enumValue(Type.TimestampMillisecond),
      enumValue(Type.TimestampMicrosecond),
      enumValue(Type.TimestampNanosecond),
      () => 'TIMESTAMP'
    )
    .with(enumValue(Type.Null), () => 'NULL')
    .with(
      enumValue(Type.List),
      enumValue(Type.FixedSizeList),
      enumValue(Type.LargeList),
      () => 'ARRAY'
    )
    .with(enumValue(Type.Struct), () => 'STRUCT')
    .with(enumValue(Type.Map), () => 'MAP')
    // Arrow types with no Feldera SQL-side equivalent. The SQL column header
    // renderer treats `undefined` as "no type annotation". Listed explicitly so
    // the `.exhaustive()` below fails to compile if apache-arrow ever adds a
    // new `Type` member, forcing a deliberate mapping decision here.
    .with(
      enumValue(Type.NONE),
      enumValue(Type.Interval),
      enumValue(Type.IntervalDayTime),
      enumValue(Type.IntervalYearMonth),
      enumValue(Type.IntervalMonthDayNano),
      enumValue(Type.Duration),
      enumValue(Type.DurationSecond),
      enumValue(Type.DurationMillisecond),
      enumValue(Type.DurationMicrosecond),
      enumValue(Type.DurationNanosecond),
      enumValue(Type.Union),
      enumValue(Type.DenseUnion),
      enumValue(Type.SparseUnion),
      enumValue(Type.Dictionary),
      () => undefined
    )
    .exhaustive()

// Arrow list-like type ids — each carries its element type as a single child.
const LIST_TYPE_IDS: Type[] = [Type.List, Type.FixedSizeList, Type.LargeList]

/**
 * The Arrow IPC reader collapses every integer width to the base `Int` type id
 * (and every float to `Float`), keeping the distinguishing detail on the type
 * instance: `bitWidth`/`isSigned` for ints, the `Precision` enum for floats.
 * `arrowTypeIdToSqlType` only sees the type id, so these two families are
 * resolved from the full type to recover TINYINT/BIGINT/REAL/… in headers.
 */
const arrowIntToSqlType = (type: DataType): SqlType => {
  const { bitWidth, isSigned } = type as unknown as { bitWidth: number; isSigned: boolean }
  // [signed, unsigned] SqlType per bit width.
  const byWidth: Record<number, [SqlType, SqlType]> = {
    8: ['TINYINT', 'UTINYINT'],
    16: ['SMALLINT', 'USMALLINT'],
    32: ['INTEGER', 'UINTEGER'],
    64: ['BIGINT', 'UBIGINT']
  }
  return (byWidth[bitWidth] ?? byWidth[32])[isSigned ? 0 : 1]
}

const arrowFloatToSqlType = (type: DataType): SqlType =>
  (type as unknown as { precision: Precision }).precision === Precision.DOUBLE ? 'DOUBLE' : 'REAL'

/**
 * Recursively translate an Arrow `DataType` into Feldera's `ColumnType`.
 *
 * The Arrow schema retains the full nested type tree across the IPC roundtrip,
 * so an `ARRAY` column carries its element type as the list's single child.
 * Mirroring that into `ColumnType.component` lets the column header render the
 * element type — e.g. `INTEGER ARRAY` or `DECIMAL(38, 10) ARRAY` — rather than
 * a bare `ARRAY`. Nested arrays (`List<List<…>>`) recurse naturally.
 *
 * `precision`/`scale` are carried for `DECIMAL` so the header shows the full
 * `DECIMAL(p, s)`; other parametric widths (e.g. `VARCHAR(n)`) are not encoded
 * in the Arrow type and so cannot be recovered here.
 */
const arrowTypeToColumnType = (type: DataType, nullable: boolean): ColumnType => {
  const sqlType =
    type.typeId === enumValue(Type.Int)
      ? arrowIntToSqlType(type)
      : type.typeId === enumValue(Type.Float)
        ? arrowFloatToSqlType(type)
        : arrowTypeIdToSqlType(type.typeId)
  const columntype: ColumnType = {
    nullable,
    type: sqlType
  }
  if (type.typeId === enumValue(Type.Decimal)) {
    const decimal = type as unknown as { precision: number; scale: number }
    columntype.precision = decimal.precision
    columntype.scale = decimal.scale
  }
  if (LIST_TYPE_IDS.includes(type.typeId)) {
    const element = (type as unknown as { children?: readonly ArrowField[] }).children?.[0]
    if (element) {
      columntype.component = arrowTypeToColumnType(element.type, element.nullable)
    }
  }
  return columntype
}

/**
 * Convert an arrow schema into the Feldera `Field[]` shape that
 * `SqlColumnHeader.svelte` and downstream display helpers expect. The schema
 * arrives from the server before any rows, so column headers render even for
 * empty result sets.
 */
export const arrowSchemaToFelderaFields = (schema: ArrowSchema): Field[] =>
  schema.fields.map((f) => ({
    name: f.name,
    case_sensitive: false,
    columntype: arrowTypeToColumnType(f.type, f.nullable),
    unused: false
  }))

export const arrowIpcBatchToJS = <T extends TypeMap>(
  batch: RecordBatch<T>
): { row: SQLValueJS[] }[] => {
  const res: { row: SQLValueJS[] }[] = new Array(batch.numRows)

  for (let i = 0; i < batch.numRows; i++) {
    const row: SQLValueJS[] = []

    for (let j = 0; j < batch.schema.fields.length; j++) {
      const field = batch.schema.fields[j]
      const column = batch.getChildAt(j)
      invariant(column, 'Incorrectly iterating over apache-arrow batch columns')
      row[j] = arrowIpcValueToJS(field, column.get(i))
    }

    res[i] = { row }
  }
  return res
}
