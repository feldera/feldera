// The CSV we get from the ingress rest API doesn't give us a concrete type.
//
// This is problematic because we can't just send all string back for the anchor
// type. They have to represent the original type, a number needs to be a number
// etc. This issue will go away once we support JSON for the data format. But
// until then we do the type conversion here.

import { nonNull } from '$lib/functions/common/function'
import { tuple } from '$lib/functions/common/tuple'
import { getCaseIndependentName } from '$lib/functions/felderaRelation'
import type { ColumnType, Relation } from '$lib/services/manager'
import { BigNumber } from 'bignumber.js/bignumber.js'
import dayjs, { Dayjs, isDayjs } from 'dayjs'
import invariant from 'tiny-invariant'
import JSONbig from 'true-json-bigint'
import { match, P } from 'ts-pattern'

export type SQLValueJS =
  | string
  | number
  | boolean
  | BigNumber
  | Dayjs
  | SQLValueJS[]
  | Map<string, SQLValueJS>
  | null

/**
 * The format we get back from the ingress rest API.
 */
export interface Row {
  genId: number
  weight: number
  record: Record<string, SQLValueJS>
}

export interface ValidationError {
  column: number
  message: string
}

/**
 * Returns a function which, for a given SQL type, converts the values of that
 * type to displayable strings. null value for a nullable SQL type is rendered to null.
 * @param columntype
 * @returns
 */
export function getValueFormatter(columntype: ColumnType) {
  return (value: SQLValueJS): string | null => {
    if ((value === null && columntype.nullable) || columntype.type === 'NULL') {
      return null
    }
    invariant(value !== null)
    const formatted = sqlValueToXgressJSON(columntype, value)
    return typeof formatted === 'string' ? formatted : JSONbig.stringify(formatted)
  }
}

/**
 * A type for a SQL value that can be unambiguously serialized into a body of a HTTP JSON ingress request
 */
export type JSONXgressValue = string | number | boolean | BigNumber | JSONXgressValue[] | null

/**
 * Render an SQL Value to a JSON value that can be unambiguously serialized into a body of a HTTP JSON ingress request
 */
export const sqlValueToXgressJSON = (type: ColumnType, value: SQLValueJS): JSONXgressValue => {
  if (value === null && type.nullable) {
    return null
  }
  invariant(value !== null)
  return match(type)
    .with({ type: 'BOOLEAN' }, () => {
      invariant(typeof value === 'boolean')
      return value
    })
    .with({ type: 'BIGINT' }, () => {
      invariant(BigNumber.isBigNumber(value))
      return value
    })
    .with({ type: 'DECIMAL' }, () => {
      invariant(BigNumber.isBigNumber(value))
      return value.toFixed()
    })
    .with(
      { type: 'TINYINT' },
      { type: 'SMALLINT' },
      { type: 'INTEGER' },
      { type: 'REAL' },
      { type: 'DOUBLE' },
      () => {
        invariant(typeof value === 'number' || BigNumber.isBigNumber(value))
        return value
      }
    )
    .with({ type: 'TIME' }, () => {
      invariant(typeof value === 'string' || isDayjs(value))
      if (typeof value === 'string') {
        return value
      }
      return value.format('HH:mm:ss')
    })
    .with({ type: 'DATE' }, () => {
      invariant(typeof value === 'string' || isDayjs(value))
      return dayjs(value).format('YYYY-MM-DD')
    })
    .with({ type: 'TIMESTAMP' }, () => {
      invariant(typeof value === 'string' || isDayjs(value))
      return dayjs(value).format('YYYY-MM-DD HH:mm:ss.SSS')
    })
    .with({ type: 'ARRAY' }, () => {
      invariant(Array.isArray(value))
      return value.map((v) => {
        invariant(nonNull(type.component))
        return sqlValueToXgressJSON(type.component, v)
      })
    })
    .with({ type: 'CHAR' }, { type: 'VARCHAR' }, () => {
      invariant(typeof value === 'string')
      return value
    })
    .with({ type: 'BINARY' }, () => {
      invariant(false, 'BINARY type is not implemented for ingress')
    })
    .with({ type: 'VARBINARY' }, () => {
      invariant(false, 'VARBINARY type is not implemented for ingress')
    })
    .with({ type: 'VARIANT' }, () => {
      invariant(false, 'VARIANT type is not implemented for ingress')
    })
    .with({ type: { Interval: P._ } }, () => {
      invariant(false, 'INTERVAL type is not supported for ingress')
    })
    .with({ type: 'STRUCT' }, () => {
      invariant(value instanceof Map)
      console.log('Ingress STRUCT', value)
      // return value.map(v => {
      //   invariant(nonNull(type.component))
      //   return sqlValueToXgressJSON(type.component, v)
      // })
      return ''
    })
    .with({ type: 'MAP' }, () => {
      invariant(value instanceof Map)
      console.log('Ingress STRUCT', value)
      // return value.map(v => {
      //   invariant(nonNull(type.component))
      //   return sqlValueToXgressJSON(type.component, v)
      // })
      return ''
    })
    .with({ type: 'NULL' }, () => {
      invariant(false, 'NULL type is not supported for ingress')
    })
    .exhaustive()
}

export const sqlRowToXgressJSON = (relation: Relation, row: Row) =>
  Object.fromEntries(
    relation.fields.map((field) =>
      tuple(
        getCaseIndependentName(field),
        sqlValueToXgressJSON(field.columntype, row.record[field.name])
      )
    )
  )

export const xgressJSONToSQLValue = (type: ColumnType, value: JSONXgressValue): SQLValueJS => {
  if (value === null && type.nullable) {
    return null
  }
  invariant(value !== null)
  return match(type)
    .with({ type: 'BOOLEAN' }, () => {
      invariant(typeof value === 'boolean')
      return value
    })
    .with({ type: 'BIGINT' }, () => {
      invariant(typeof value === 'number' || BigNumber.isBigNumber(value))
      return BigNumber(value)
    })
    .with({ type: 'DECIMAL' }, () => {
      invariant(typeof value === 'string')
      invariant(!/\.$/.test(value))
      const number = BigNumber(value)
      invariant(!number.isNaN())
      return number
    })
    .with(
      { type: 'TINYINT' },
      { type: 'SMALLINT' },
      { type: 'INTEGER' },
      { type: 'REAL' },
      { type: 'DOUBLE' },
      () => {
        invariant(typeof value === 'number' || BigNumber.isBigNumber(value))
        return typeof value === 'number' ? value : value.toNumber()
      }
    )
    .with({ type: 'TIME' }, () => {
      invariant(typeof value === 'string' && value.length === 8)
      const time = dayjs(value, 'HH:mm:ss')
      invariant(time.isValid())
      return time
    })
    .with({ type: 'DATE' }, () => {
      invariant(typeof value === 'string' && value.length === 10)
      const date = dayjs(value, 'YYYY-MM-DD')
      invariant(date.isValid())
      return date
    })
    .with({ type: 'TIMESTAMP' }, () => {
      invariant(typeof value === 'string' && value.length >= 19 && value.length <= 26)
      const date = dayjs(value.padEnd(23, '.000'), 'YYYY-MM-DD HH:mm:ss.SSS')
      invariant(date.isValid())
      return date
    })
    .with({ type: 'ARRAY' }, () => {
      invariant(Array.isArray(value))
      return value.map((v) => {
        invariant(nonNull(type.component))
        return xgressJSONToSQLValue(type.component, v)
      })
    })
    .with({ type: 'CHAR' }, { type: 'VARCHAR' }, () => {
      invariant(typeof value === 'string')
      return value
    })
    .with({ type: 'BINARY' }, () => {
      invariant(false, 'BINARY type is not implemented for ingress')
    })
    .with({ type: 'VARBINARY' }, () => {
      invariant(false, 'VARBINARY type is not implemented for ingress')
    })
    .with({ type: 'VARIANT' }, () => {
      invariant(false, 'VARIANT type is not implemented for ingress')
    })
    .with({ type: { Interval: P._ } }, () => {
      invariant(typeof value === 'number' || BigNumber.isBigNumber(value))
      return BigNumber(value)
    })
    .with({ type: 'STRUCT' }, () => {
      console.log('Ingress STRUCT', value)
      // return value.map(v => {
      //   invariant(nonNull(type.component))
      //   return sqlValueToXgressJSON(type.component, v)
      // })
      return new Map()
    })
    .with({ type: 'MAP' }, () => {
      console.log('Ingress STRUCT', value)
      // return value.map(v => {
      //   invariant(nonNull(type.component))
      //   return sqlValueToXgressJSON(type.component, v)
      // })
      return new Map()
    })
    .with({ type: 'NULL' }, () => {
      invariant(false, 'NULL type is not supported for ingress')
    })
    .exhaustive()
}

export const xgressJSONToSQLRecord = (
  relation: Relation,
  value: Record<string, JSONXgressValue>
): Record<string, SQLValueJS> =>
  Object.fromEntries(
    relation.fields.map((field) =>
      tuple(
        getCaseIndependentName(field),
        xgressJSONToSQLValue(field.columntype, value[getCaseIndependentName(field)])
      )
    )
  )

// Walk the type tree and find the base type.
//
// e.g., for a `VARCHAR ARRAY` type, this will return VARCHAR.
export const findBaseType = (type: ColumnType): ColumnType => {
  if (type.component) {
    return findBaseType(type.component)
  }

  return type
}

// Returns the [min, max] (inclusive) range for a SQL type where applicable.
//
// Throws an error if the type does not have a range.
export const numericRange = (sqlType: ColumnType) =>
  match(sqlType)
    .with({ type: 'TINYINT' }, () => ({ min: new BigNumber(-128), max: new BigNumber(127) }))
    .with({ type: 'SMALLINT' }, () => ({ min: new BigNumber(-32768), max: new BigNumber(32767) }))
    .with({ type: 'INTEGER' }, () => ({
      min: new BigNumber(-2147483648),
      max: new BigNumber(2147483647)
    }))
    .with({ type: 'BIGINT' }, () => ({
      min: new BigNumber(-2).pow(63),
      max: new BigNumber(2).pow(63).minus(1)
    }))
    .with({ type: 'REAL' }, () => ({
      min: new BigNumber('-3.402823466e+38'),
      max: new BigNumber('3.402823466e+38')
    }))
    .with({ type: 'DOUBLE' }, () => ({
      min: new BigNumber('-1.7976931348623158e+308'),
      max: new BigNumber('1.7976931348623158e+308')
    }))
    .with({ type: 'DECIMAL' }, (ct) => {
      invariant(nonNull(ct.precision))
      invariant(nonNull(ct.scale))
      const max = new BigNumber(10)
        .pow(ct.precision!)
        .minus(1)
        .div(new BigNumber(10).pow(ct.scale!))
      const min = max.negated()
      return { min, max }
    })
    // Limit array lengths to 0-5 for random generation.
    .with({ type: 'ARRAY' }, () => ({ min: new BigNumber(0), max: new BigNumber(5) }))
    .with(
      { type: 'BOOLEAN' },
      { type: 'CHAR' },
      { type: 'VARCHAR' },
      { type: 'BINARY' },
      { type: 'VARBINARY' },
      { type: 'DATE' },
      { type: 'TIME' },
      { type: 'TIMESTAMP' },
      { type: { Interval: P._ } },
      { type: 'STRUCT' },
      { type: 'VARIANT' },
      { type: 'MAP' },
      { type: 'NULL' },
      () => {
        throw new Error(`Not a numeric type: ${sqlType.type}`)
      }
    )
    .exhaustive()

export const dateTimeRange = (sqlType: ColumnType): Dayjs[] =>
  match(sqlType.type)
    // We can represent the date range going all the way to year zero but the
    // date-picker we use complains if we use dates less than 1000-01-01. There
    // is also a rendering issue if we have too many years for the datepicker
    // components so we're a bit more conservative than we need to be.
    // - https://github.com/mui/mui-x/issues/4746
    .with('DATE', 'TIMESTAMP', () => [
      dayjs(new Date('1500-01-01 00:00:00')),
      dayjs(new Date('2500-12-31 23:59:59'))
    ])
    .with('TIME', () => [
      dayjs(new Date('1500-01-01 00:00:00')),
      dayjs(new Date('1500-01-01 23:59:59'))
    ])
    .with(
      'BOOLEAN',
      'TINYINT',
      'SMALLINT',
      'INTEGER',
      'REAL',
      'DOUBLE',
      'BIGINT',
      'DECIMAL',
      { Interval: P._ },
      'CHAR',
      'VARCHAR',
      'BINARY',
      'VARBINARY',
      'ARRAY',
      'STRUCT',
      'MAP',
      'VARIANT',
      'NULL',
      undefined,
      () => {
        throw new Error('Not a date/time type')
      }
    )
    .exhaustive()

export const sqlValueComparator = (sqlType: ColumnType) => {
  const comparator = match(sqlType.type)
    .returnType<(a: SQLValueJS, b: SQLValueJS) => number>()
    .with('BOOLEAN', () => (a, b) => {
      invariant(typeof a === 'boolean' && typeof b === 'boolean')
      return b === a ? 0 : a ? 1 : -1
    })
    .with('TINYINT', 'SMALLINT', 'INTEGER', 'REAL', 'DOUBLE', () => (a, b) => {
      invariant(typeof a === 'number' && typeof b === 'number')
      return a - b
    })
    .with('BIGINT', 'DECIMAL', () => (a, b) => {
      invariant(BigNumber.isBigNumber(a) && BigNumber.isBigNumber(b))
      return a.comparedTo(b)
    })
    .with('CHAR', 'VARCHAR', () => (a, b) => {
      invariant(typeof a === 'string' && typeof b === 'string')
      return a.localeCompare(b)
    })
    .with('TIME', 'DATE', 'TIMESTAMP', () => (a, b) => {
      invariant(isDayjs(a) && isDayjs(b))
      return a.isSame(b) ? 0 : a.isAfter(b) ? 1 : -1
    })
    .with({ Interval: P._ }, () => (a, b) => {
      invariant(BigNumber.isBigNumber(a) && BigNumber.isBigNumber(b))
      return a.comparedTo(b)
    })
    .with('ARRAY', () => (a, b) => {
      invariant(Array.isArray(a) && Array.isArray(b))
      return a.length - b.length
    })
    .with('BINARY', () => () => 0)
    .with('VARBINARY', () => () => 0)
    .with('VARIANT', () => () => 0)
    .with('STRUCT', () => () => 0)
    .with('MAP', () => () => 0)
    .with('NULL', () => () => 0)
    .with(undefined, () => () => 0)
    .exhaustive()

  return (a: SQLValueJS, b: SQLValueJS) => {
    if (sqlType.nullable && (a === null || b === null)) {
      return a === null ? (b === null ? 0 : -1) : 1
    }
    return comparator(a, b)
  }
}
