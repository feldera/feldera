// The CSV we get from the ingress rest API doesn't give us a concrete type.
//
// This is problematic because we can't just send all string back for the anchor
// type. They have to represent the original type, a number needs to be a number
// etc. This issue will go away once we support JSON for the data format. But
// until then we do the type conversion here.

import { clampBigNumber } from '$lib/functions/common/bigNumber'
import { nonNull } from '$lib/functions/common/function'
import { ColumnType, Relation, SqlType } from '$lib/services/manager'
import assert from 'assert'
import { BigNumber } from 'bignumber.js'
import dayjs, { Dayjs, isDayjs } from 'dayjs'
import invariant from 'tiny-invariant'
import { match, P } from 'ts-pattern'

export type SQLValueJS = string | number | BigNumber | boolean | Dayjs | SQLValueJS[]

/**
 * The format we get back from the ingress rest API.
 */
export interface Row {
  genId: number
  weight: number
  record: Record<string, SQLValueJS | null>
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
  const formatter = match(columntype)
    .returnType<(value: SQLValueJS) => string>()
    .with({ type: SqlType.BIGINT }, { type: SqlType.DECIMAL }, () => {
      return value => {
        invariant(BigNumber.isBigNumber(value))
        return value.toFixed()
      }
    })
    .with(
      { type: SqlType.TINYINT },
      { type: SqlType.SMALLINT },
      { type: SqlType.INTEGER },
      { type: SqlType.REAL },
      { type: SqlType.DOUBLE },
      () => {
        return value => {
          invariant(typeof value === 'number' || BigNumber.isBigNumber(value))
          return value.toString()
        }
      }
    )
    .with({ type: SqlType.TIME }, () => {
      return value => {
        invariant(typeof value === 'string' || isDayjs(value))
        if (typeof value === 'string') {
          return value
        }
        return value.format('HH:mm:ss')
      }
    })
    .with({ type: SqlType.DATE }, () => {
      return value => {
        invariant(typeof value === 'string' && isDayjs(value))
        return dayjs(value).format('YYYY-MM-DD')
      }
    })
    .with({ type: SqlType.TIMESTAMP }, () => {
      return value => {
        invariant(typeof value === 'string' && isDayjs(value))
        return dayjs(value).format('YYYY-MM-DD HH:mm:ss')
      }
    })
    .with({ type: SqlType.ARRAY }, () => {
      return value => {
        invariant(Array.isArray(value))
        return JSON.stringify(
          value.map(v => {
            assert(columntype.component !== null && columntype.component !== undefined)
            return clampToSQL(columntype.component)(v)
          })
        )
      }
    })
    .with({ type: SqlType.CHAR }, { type: SqlType.VARCHAR }, () => {
      return value => {
        invariant(typeof value === 'string')
        return value
      }
    })
    .otherwise(() => {
      return value => {
        return value.toString()
      }
    })
  return (value: SQLValueJS) => {
    if (value === null && columntype.nullable) {
      return null
    }
    return formatter(value)
  }
}

// Generate a parser function for a field that converts a value to something
// that is close to the original value but also acceptable for the SQL type.
export function clampToSQL(columntype: ColumnType) {
  return match(columntype)
    .with(
      {
        type: SqlType.VARCHAR,
        precision: P.when(value => (value ?? -1) >= 0)
      },
      () => (value: string | string[]) => {
        invariant(typeof value === 'string' || Array.isArray(value), `clampToSQL VARCHAR: ${typeof value} ${value}`)
        invariant(nonNull(columntype.precision))
        return value.toString().substring(0, columntype.precision)
      }
    )
    .with(
      {
        type: SqlType.CHAR,
        precision: P.when(value => (value ?? -1) >= 0)
      },
      () => (value: string | string[]) => {
        invariant(typeof value === 'string' || Array.isArray(value), `clampToSQL CHAR: ${typeof value} ${value}`)
        invariant(nonNull(columntype.precision))
        return value.toString().substring(0, columntype.precision).padEnd(columntype.precision)
      }
    )
    .with(
      { type: SqlType.TINYINT },
      { type: SqlType.SMALLINT },
      { type: SqlType.INTEGER },
      () => (value: BigNumber) => {
        invariant(BigNumber.isBigNumber(value), `clampToSQL TINYINT: ${typeof value} ${value}`)
        const number = value
        const { min, max } = numericRange(columntype)
        return clampBigNumber(min, max, number.decimalPlaces(0, BigNumber.ROUND_HALF_UP))
      }
    )
    .with({ type: SqlType.BIGINT }, () => (value: BigNumber) => {
      invariant(BigNumber.isBigNumber(value), `clampToSQL BIGINT: ${typeof value} ${value}`)
      const number = value
      const { min, max } = numericRange(columntype)
      return clampBigNumber(min, max, number.decimalPlaces(0, BigNumber.ROUND_HALF_UP))
    })
    .with({ type: SqlType.REAL }, () => (value: BigNumber) => {
      invariant(BigNumber.isBigNumber(value), `clampToSQL FLOAT: ${typeof value} ${value}`)
      const { min, max } = numericRange(columntype)
      return new BigNumber(Float32Array.from([clampBigNumber(min, max, value).toNumber()])[0])
    })
    .with({ type: SqlType.DOUBLE }, () => (value: BigNumber) => {
      invariant(BigNumber.isBigNumber(value), `clampToSQL DOUBLE: ${typeof value} ${value}`)
      const { min, max } = numericRange(columntype)
      return new BigNumber(clampBigNumber(min, max, value).toNumber())
    })
    .with({ type: SqlType.DECIMAL }, () => (value: BigNumber) => {
      invariant(BigNumber.isBigNumber(value), `clampToSQL DECIMAL: ${typeof value} ${value}`)
      // const [precision, scale] = [columntype.precision ?? 1024, columntype.scale ?? 0]
      invariant(nonNull(columntype.precision))
      invariant(nonNull(columntype.scale))
      assert(columntype.precision >= columntype.scale, 'Precision must be greater or equal than scale')
      // We want to limit the number of digits that are displayed in the UI to
      // fit the column decimal type.
      return new BigNumber(value).decimalPlaces(columntype.scale, BigNumber.ROUND_HALF_UP)
    })
    .with({ type: SqlType.TIME }, () => (value: string | Dayjs) => {
      invariant(typeof value === 'string' || isDayjs(value), `clampToSQL TIME: ${typeof value} ${value}`)
      // We represent TIME as string for now because the data-grid doesn't
      // have native support for times yet.
      // See also
      return dayjs(value).format('HH:mm:ss')
    })
    .with({ type: SqlType.DATE }, { type: SqlType.TIMESTAMP }, () => (value: string | Dayjs) => {
      invariant(typeof value === 'string' || isDayjs(value), `clampToSQL DATE,TIMESTAMP: ${typeof value} ${value}`)
      return dayjs(value)
    })
    .with({ type: SqlType.ARRAY }, () => (value: SQLValueJS[]): SQLValueJS[] => {
      return value.map(v => {
        invariant(columntype.component)
        return clampToSQL(columntype.component)(v as never)
      })
    })
    .otherwise(
      () =>
        <T>(value: T) =>
          value
    ) as (value: SQLValueJS) => SQLValueJS
}

// Convert a row of strings to an object of typed values.
//
// The type conversion is important because for sending a row as an anchor later
// it needs to have proper types (a number can't be a string etc.)
export function csvLineToRow(relation: Relation, row: string[]): Row {
  const genId = Number(row[0])
  const weight = Number(row[row.length - 1])
  const records = row.slice(1, row.length - 1)

  const record: Record<string, SQLValueJS | null> = {}
  relation.fields.forEach((field, i) => {
    record[field.name] = parseCSVValue(field.columntype, records[i])
  })

  return {
    genId,
    weight,
    record
  }
}

// We convert fields to a tuple so that we can use it as an anchor in the REST
// API.
export function rowToAnchor(relation: Relation, obj: Row): any[] {
  const tuple: any[] = []
  relation.fields.map((col, i) => {
    tuple[i] = obj.record[col.name]
  })

  return tuple
}

// Walk the type tree and find the base type.
//
// e.g., for a `VARCHAR ARRAY` type, this will return VARCHAR.
export const findBaseType = (type: ColumnType): ColumnType => {
  if (type.component) {
    return findBaseType(type.component)
  }

  return type
}

/**
 * Parse a value for a SQL type. This is used for parsing values from the
 * backend directly that can be trusted to satisfy the type constraints.
 * @param sqlType
 * @param value
 * @returns
 */
export const parseCSVValue = (sqlType: ColumnType, value: string) => {
  if (sqlType.nullable && ['', 'null'].includes(value)) {
    return null
  }
  return match(sqlType)
    .returnType<SQLValueJS>()
    .with({ type: SqlType.BOOLEAN }, () => value === 'true')
    .with({ type: SqlType.TINYINT }, { type: SqlType.SMALLINT }, { type: SqlType.INTEGER }, () => parseInt(value))
    .with({ type: SqlType.REAL }, { type: SqlType.DOUBLE }, () => parseFloat(value))
    .with({ type: SqlType.BIGINT }, { type: SqlType.DECIMAL }, () => new BigNumber(value))
    .with({ type: SqlType.TIMESTAMP }, { type: SqlType.DATE }, { type: SqlType.TIME }, () => value)
    .otherwise(() => value)
}

// Returns the [min, max] (inclusive) range for a SQL type where applicable.
//
// Throws an error if the type does not have a range.
export const numericRange = (sqlType: ColumnType) =>
  match(sqlType)
    .with({ type: SqlType.TINYINT }, () => ({ min: new BigNumber(-128), max: new BigNumber(127) }))
    .with({ type: SqlType.SMALLINT }, () => ({ min: new BigNumber(-32768), max: new BigNumber(32767) }))
    .with({ type: SqlType.INTEGER }, () => ({ min: new BigNumber(-2147483648), max: new BigNumber(2147483647) }))
    .with({ type: SqlType.BIGINT }, () => ({ min: new BigNumber(-2).pow(63), max: new BigNumber(2).pow(63).minus(1) }))
    .with({ type: SqlType.REAL }, () => ({
      min: new BigNumber('-3.402823466e38'),
      max: new BigNumber('3.402823466e38')
    }))
    .with({ type: SqlType.DOUBLE }, () => ({
      min: new BigNumber('2.2250738585072014e-308'),
      max: new BigNumber('1.7976931348623158e+308')
    }))
    .with({ type: SqlType.DECIMAL }, ct => {
      invariant(nonNull(ct.precision))
      invariant(nonNull(ct.scale))
      const max = new BigNumber(10).pow(ct.precision!).minus(1).div(new BigNumber(10).pow(ct.scale!))
      const min = max.negated()
      return { min, max }
    })
    // Limit array lengths to 0-5 for random generation.
    .with({ type: SqlType.ARRAY }, () => ({ min: new BigNumber(0), max: new BigNumber(5) }))
    .otherwise(() => {
      throw new Error(`Not a numeric type: ${sqlType.type}`)
    })

export const dateTimeRange = (sqlType: ColumnType): Dayjs[] =>
  match(sqlType)
    // We can represent the date range going all the way to year zero but the
    // date-picker we use complains if we use dates less than 1000-01-01. There
    // is also a rendering issue if we have too many years for the datepicker
    // components so we're a bit more conservative than we need to be.
    // - https://github.com/mui/mui-x/issues/4746
    .with({ type: SqlType.DATE }, { type: SqlType.TIMESTAMP }, () => [
      dayjs(new Date('1500-01-01 00:00:00')),
      dayjs(new Date('2500-12-31 23:59:59'))
    ])
    .with({ type: SqlType.TIME }, () => [
      dayjs(new Date('1500-01-01 00:00:00')),
      dayjs(new Date('1500-01-01 23:59:59'))
    ])
    .otherwise(() => {
      throw new Error('Not a date/time type')
    })
