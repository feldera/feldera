import { BigNumber } from 'bignumber.js'
import { Dayjs, isDayjs } from 'dayjs'
import JSONbig from 'true-json-bigint'
import type { QueryResult } from '$lib/components/adhoc/Query.svelte'
import { nonNull } from '$lib/functions/common/function'
import type { ColumnType } from '$lib/services/manager'
import type { SQLValueJS } from '$lib/types/sql'

const displaySQLType = (columntype: ColumnType): string =>
  (columntype.component ? displaySQLType(columntype.component) + ' ' : '') +
  columntype.type +
  ((p, s) => (p && p > 0 ? '(' + (nonNull(s) ? [p, s] : [p]).join(', ') + ')' : ''))(
    columntype.precision,
    columntype.scale
  )

export const displaySQLColumnType = ({ columntype }: { columntype: ColumnType }) =>
  columntype.type ? displaySQLType(columntype) + (columntype.nullable ? '' : ' NOT NULL') : ''

export const displaySQLValue = (value: SQLValueJS) => {
  return value === null
    ? 'NULL'
    : typeof value === 'string'
      ? value
      : BigNumber.isBigNumber(value)
        ? value.toFixed()
        : JSONbig.stringify(value, undefined, 1)
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
  return JSONbig.stringify(toJSONValue(value))
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
