import { BigNumber } from 'bignumber.js'
import { Dayjs, isDayjs } from 'dayjs'
import JSONbig from 'true-json-bigint'
import { match } from 'ts-pattern'
import type { QueryResult } from '$lib/components/adhoc/Query.svelte'
import { nonNull } from '$lib/functions/common/function'
import type { ColumnType, SqlType } from '$lib/services/manager'
import type { SQLValueJS } from '$lib/types/sql'

/**
 * Render an `INTERVAL_*` wire value as its ISO SQL spelling, e.g.
 * `INTERVAL_DAY_HOUR` → `INTERVAL DAY TO HOUR` and `INTERVAL_DAY` → `INTERVAL DAY`.
 */
const isoInterval = (type: string): string =>
  'INTERVAL ' + type.slice('INTERVAL_'.length).split('_').join(' TO ')

/**
 * Map a Feldera `SqlType` to its ISO SQL display spelling. `SqlType` values are
 * the platform's uppercase wire encoding (`BIGINT`, `INTEGER`, `INTERVAL_DAY`,
 * …) — not valid SQL syntax — so this reconstructs the SQL spelling for the
 * column header: most pass through, but some differ (unsigned ints, `DOUBLE`,
 * `STRUCT`, intervals, timestamps with time zone).
 *
 * Used by `SQLColumnHeader` for both Change Stream headers (built from the
 * pipeline's program schema) and ad-hoc query headers (built from the Arrow
 * IPC schema in `apacheArrow.ts`); both deliver the same `SqlType` values.
 */
const sqlTypeToIsoSql = (type: SqlType): string =>
  match(type)
    .returnType<string>()
    .with('BOOLEAN', () => 'BOOLEAN')
    .with('TINYINT', () => 'TINYINT')
    .with('SMALLINT', () => 'SMALLINT')
    .with('INTEGER', () => 'INTEGER')
    .with('BIGINT', () => 'BIGINT')
    .with('UTINYINT', () => 'TINYINT UNSIGNED')
    .with('USMALLINT', () => 'SMALLINT UNSIGNED')
    .with('UINTEGER', () => 'INTEGER UNSIGNED')
    .with('UBIGINT', () => 'BIGINT UNSIGNED')
    .with('REAL', () => 'REAL')
    .with('DOUBLE', () => 'DOUBLE PRECISION')
    .with('DECIMAL', () => 'DECIMAL')
    .with('CHAR', () => 'CHAR')
    .with('VARCHAR', () => 'VARCHAR')
    .with('BINARY', () => 'BINARY')
    .with('VARBINARY', () => 'VARBINARY')
    .with('TIME', () => 'TIME')
    .with('DATE', () => 'DATE')
    .with('TIMESTAMP', () => 'TIMESTAMP')
    .with('TIMESTAMP_TZ', () => 'TIMESTAMP WITH TIME ZONE')
    .with(
      'INTERVAL_DAY',
      'INTERVAL_DAY_HOUR',
      'INTERVAL_DAY_MINUTE',
      'INTERVAL_DAY_SECOND',
      'INTERVAL_HOUR',
      'INTERVAL_HOUR_MINUTE',
      'INTERVAL_HOUR_SECOND',
      'INTERVAL_MINUTE',
      'INTERVAL_MINUTE_SECOND',
      'INTERVAL_MONTH',
      'INTERVAL_SECOND',
      'INTERVAL_YEAR',
      'INTERVAL_YEAR_MONTH',
      isoInterval
    )
    .with('ARRAY', () => 'ARRAY')
    .with('STRUCT', () => 'ROW')
    .with('MAP', () => 'MAP')
    .with('NULL', () => 'NULL')
    .with('UUID', () => 'UUID')
    .with('VARIANT', () => 'VARIANT')
    .exhaustive()

const displaySQLType = (columntype: ColumnType): string =>
  (columntype.component ? displaySQLType(columntype.component) + ' ' : '') +
  (columntype.type ? sqlTypeToIsoSql(columntype.type) : '') +
  ((p, s) => (p && p > 0 ? '(' + (nonNull(s) ? [p, s] : [p]).join(', ') + ')' : ''))(
    columntype.precision,
    columntype.scale
  )

export const displaySQLColumnType = ({ columntype }: { columntype: ColumnType }) =>
  columntype.type ? displaySQLType(columntype) + (columntype.nullable ? '' : ' NOT NULL') : ''

/**
 * Render binary (`VARBINARY`/`BINARY`) bytes as a lowercase hex string, e.g.
 * the bytes `de ad be ef` become `deadbeef`. This mirrors how SQL renders
 * binary literals (`x'deadbeef'`).
 */
export const bytesToHex = (bytes: Uint8Array): string => {
  let hex = ''
  for (const byte of bytes) {
    hex += byte.toString(16).padStart(2, '0')
  }
  return hex
}

/**
 * Render a non-finite IEEE-754 value (`NaN`, `Infinity`, `-Infinity`) as text,
 * or return `undefined` for finite numbers and non-numbers.
 *
 * JSON cannot represent these values, so `JSON.stringify`
 * collapses each of them to the literal `null` —
 * indistinguishable from a SQL NULL.
 */
export const formatNonFiniteNumber = (value: unknown): string | undefined =>
  typeof value === 'number' && !Number.isFinite(value) ? String(value) : undefined

export const displaySQLValue = (value: SQLValueJS) => {
  return value === null
    ? 'NULL'
    : typeof value === 'string'
      ? value
      : value instanceof Uint8Array
        ? bytesToHex(value)
        : BigNumber.isBigNumber(value)
          ? value.toFixed()
          : (formatNonFiniteNumber(value) ?? JSONbig.stringify(value, undefined, 1))
}

/**
 * Convert SQLValueJS to a JSON-serializable value
 * Handles recursive structures including Maps and nested arrays
 */
const toJSONValue = (value: SQLValueJS): unknown => {
  if (value === null) {
    return null
  }
  if (isDayjs(value)) {
    return value.toISOString()
  }
  if (value instanceof Map) {
    // Convert Map to plain object, recursively converting values
    const obj: Record<string, unknown> = {}
    for (const [key, val] of value) {
      obj[key] = toJSONValue(val)
    }
    return obj
  }
  if (Array.isArray(value)) {
    // Recursively convert array elements
    return value.map(toJSONValue)
  }
  // For primitives (string, number, boolean) and BigNumber, return as-is
  // JSONbig.stringify will handle BigNumber correctly
  return value
}

/**
 * Serialize SQLValueJS to a JSON string representation suitable for CSV export
 */
export const serializeSQLValue = (value: SQLValueJS): string => {
  return formatNonFiniteNumber(value) ?? JSONbig.stringify(toJSONValue(value))
}

/**
 * Convert QueryResult to CSJV (Comma-Separated JSON Values) format
 *
 * CSJV is like CSV, but each cell is serialized as a JSON value.
 * The first row contains column names as JSON strings.
 * Subsequent rows contain data cells as JSON values.
 *
 * @param result - The query result to serialize
 * @returns CSJV-formatted string
 */
export const tableToCSJV = (result: QueryResult): string => {
  const rows = result.rows()
  const columns = result.columns

  // Pre-allocate array for worst case (header + all rows)
  const lines = new Array<string>(rows.length + 1)

  // Build the header row with column names as JSON strings
  lines[0] = columns.map((col) => JSONbig.stringify(col.name)).join(',')

  // Build data rows in a single pass: data rows have a `cells` property,
  // while error/warning rows use `error` or `warning`. Only data rows are exported.
  let lineIndex = 0
  for (const row of rows) {
    if ('cells' in row) {
      lines[++lineIndex] = row.cells.map(serializeSQLValue).join(',')
    }
  }

  // Truncate array to actual size (in case some rows were errors/warnings)
  lines.length = lineIndex + 1
  return lines.join('\n')
}
