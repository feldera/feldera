// The CSV we get from the ingress rest API doesn't give us a concrete type.
//
// This is problematic because we can't just send all string back for the anchor
// type. They have to represent the original type, a number needs to be a number
// etc. This issue will go away once we support JSON for the data format. But
// until then we do the type conversion here.

import { clamp } from '$lib/functions/common/math'
import assert from 'assert'
import dayjs, { Dayjs } from 'dayjs'
import { match, P } from 'ts-pattern'

import { GridCellParams } from '@mui/x-data-grid-pro'

import { ColumnType, Field, Relation } from './manager'

// Representing we get back from the ingress rest API.
export interface Row {
  genId: number
  weight: number
  record: Record<string, any>
}

export interface ValidationError {
  column: number
  message: string
}

// Returns a function which, for a given SQL type, converts the values of that
// type to displayable strings.
export function getValueFormatter(columntype: ColumnType): (value: any, params?: GridCellParams) => string {
  return match(columntype)
    .with({ type: 'TIME' }, () => {
      return (value: any) => {
        if (dayjs.isDayjs(value)) {
          return value.format('HH:mm:ss')
        } else {
          return value
        }
      }
    })
    .with({ type: 'DATE' }, () => {
      return (value: any) => {
        return dayjs(value).format('YYYY-MM-DD')
      }
    })
    .with({ type: 'TIMESTAMP' }, () => {
      return (value: any) => {
        return dayjs(value).format('YYYY-MM-DD HH:mm:ss')
      }
    })
    .with({ type: 'ARRAY' }, () => {
      return (value: any) => {
        return JSON.stringify(
          value.map((v: any) => {
            assert(columntype.component !== null && columntype.component !== undefined)
            return getValueParser(columntype.component)(v)
          })
        )
      }
    })
    .otherwise(() => {
      return (value: any) => {
        return value.toString()
      }
    })
}

// Generate a parser function for a field that converts a value to something
// that is close to the original value but also acceptable for the SQL type.
export function getValueParser(columntype: ColumnType): (value: any) => any {
  return match(columntype)
    .with({ type: 'VARCHAR', precision: P.when(value => (value ?? -1) >= 0) }, () => {
      return (value: any) => {
        return value.toString().substring(0, columntype.precision)
      }
    })
    .with({ type: 'CHAR', precision: P.when(value => (value ?? -1) >= 0) }, () => {
      return (value: any) => {
        return value.toString().substring(0, columntype.precision).padEnd(columntype.precision)
      }
    })
    .with({ type: 'TINYINT' }, { type: 'SMALLINT' }, { type: 'INTEGER' }, { type: 'BIGINT' }, () => {
      return (value: any) => {
        const number = Number(value)
        const [min, max] = typeRange(columntype)
        return clamp(Math.round(number), min, max)
      }
    })
    .with({ type: 'FLOAT' }, () => {
      return (value: any) => {
        const [min, max] = typeRange(columntype)
        return clamp(Number(value), min, max)
      }
    })
    .with({ type: 'DOUBLE' }, () => {
      return (value: any) => {
        return Number(value)
      }
    })
    .with({ type: 'DECIMAL' }, () => {
      return (value: any) => {
        const [precision, scale] = [columntype.precision ?? 1024, columntype.scale ?? 0]
        assert(precision >= scale, 'Precision must be greater or equal than scale')
        // We want to limit the number of digits that are displayed in the UI to
        // fit the column decimal type.
        //
        // In the future it might be easier to just use a library like
        // decimal.js to handle this.
        const value_str = value.toString()
        const isFloat = /[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$/.test(value_str)

        if (isFloat) {
          const parts = value_str.split('.')
          assert(parts.length == 1 || parts.length == 2, 'Ensured by isFloat check')
          // Cutting the least significant digits to fit the precision.
          const first = parts[0].split('').reverse().slice(0, precision).reverse().join('')
          if (parts.length == 1 || scale === 0) {
            return first
          } else if (parts.length === 2 && scale > 0) {
            return `${first}.${parts[1].substring(0, Math.min(precision - first.length, scale))}`
          }
        } else {
          return '0'
        }
      }
    })
    .with({ type: 'TIME' }, () => {
      return (value: any) => {
        // We represent TIME as string for now because the data-grid doesn't
        // have native support for times yet.
        // See also
        return dayjs(value).format('HH:mm:ss')
      }
    })
    .with({ type: 'DATE' }, () => {
      return (value: any) => {
        return dayjs(value)
      }
    })
    .with({ type: 'TIMESTAMP' }, () => {
      return (value: any) => {
        return dayjs(value)
      }
    })
    .with({ type: 'ARRAY' }, () => {
      return (value: any) => {
        return value.map((v: any) => {
          assert(columntype.component !== null && columntype.component !== undefined)
          return getValueParser(columntype.component)(v)
        })
      }
    })
    .otherwise(() => {
      return (value: any) => value
    })
}

// Convert a row of strings to an object of typed values.
//
// The type conversion is important because for sending a row as an anchor later
// it needs to have proper types (a number can't be a string etc.)
export function csvLineToRow(relation: Relation, row: any[]): Row {
  const genId = Number(row[0])
  const weight = Number(row[row.length - 1])
  const records = row.slice(1, row.length - 1)

  const record: Record<string, any> = {}
  relation.fields.forEach((field, i) => {
    record[field.name] = parseValueSafe(field.columntype, records[i])
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
  console.log(obj, relation)
  relation.fields.map((col: Field, i: number) => {
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

// Parse a value for a SQL type. This is used for parsing values from the
// backend directly that can be trusted to satisfy the type constraints.
//
// TODO: Note that all Number in JS are 64-bit floating point numbers. When we
// switch to the JSON format we will just parse everything as BigInt which has
// arbitrary precision so we don't loose anything when displaying numbers from
// dbsp.
export const parseValueSafe = (sqlType: ColumnType, value: string) =>
  match(sqlType)
    .with({ type: 'BOOLEAN' }, () => {
      if (value == 'true') {
        return true
      } else {
        return false
      }
    })
    .with({ type: 'TINYINT' }, () => Number(value))
    .with({ type: 'SMALLINT' }, () => Number(value))
    .with({ type: 'INTEGER' }, () => Number(value))
    .with({ type: 'BIGINT' }, () => Number(value))
    .with({ type: 'FLOAT' }, () => Number(value))
    .with({ type: 'DOUBLE' }, () => Number(value))
    .with({ type: 'TIMESTAMP', nullable: P.select() }, (nullable: boolean) => {
      if (!value && nullable) return null
      else return value
    })
    .otherwise(() => value)

// Returns the [min, max] (inclusive) range for a SQL type where applicable.
//
// Throws an error if the type does not have a range.
export const typeRange = (sqlType: ColumnType): number[] =>
  match(sqlType)
    .with({ type: 'TINYINT' }, () => [-128, 127])
    .with({ type: 'SMALLINT' }, () => [-32768, 32767])
    .with({ type: 'INTEGER' }, () => [-2147483648, 2147483647])
    // TODO: this should be [-2^63, 2^63 − 1] but we need to switch to BigInt to
    // represent it see other comment in `parseValueSafe`
    .with({ type: 'BIGINT' }, () => [Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER])
    .with({ type: 'FLOAT' }, () => [-3.40282347e38, 3.40282347e38])
    .with({ type: 'DOUBLE' }, () => [-Number.MAX_VALUE, Number.MAX_VALUE])
    // TODO: maybe we can do something smarter here in the future (like use
    // decimal-js), but for now our RNG-methods need regular numbers to generate
    // values.
    .with({ type: 'DECIMAL' }, () => [Number.NEGATIVE_INFINITY, Number.POSITIVE_INFINITY])
    // Limit array lengths to 0-5 for random generation.
    .with({ type: 'ARRAY' }, () => [0, 5])
    .otherwise(() => {
      console.log('not a number type', sqlType)
      throw new Error('Not a number type')
    })

export const dateTimeRange = (sqlType: ColumnType): Dayjs[] =>
  match(sqlType)
    // We can represent the date range going all the way to year zero but the
    // date-picker we use complains if we use dates less than 1000-01-01. There
    // is also a rendering issue if we have too many years for the datepicker
    // components so we're a bit more conservative than we need to be.
    // - https://github.com/mui/mui-x/issues/4746
    .with({ type: 'DATE' }, { type: 'TIMESTAMP' }, () => [
      dayjs(new Date('1500-01-01 00:00:00')),
      dayjs(new Date('2500-12-31 23:59:59'))
    ])
    .with({ type: 'TIME' }, () => [dayjs(new Date('1500-01-01 00:00:00')), dayjs(new Date('1500-01-01 23:59:59'))])
    .otherwise(() => {
      throw new Error('Not a date/time type')
    })

// SQL to Data-grid type conversion
//
// The following are the native column types in the data-grid:
//
// - 'string' (default)
// - 'number'
// - 'date'
// - 'dateTime'
// - 'boolean'
// - 'singleSelect'
// - 'actions'
export const sqlTypeToDataGridType = (sqlType: Field) =>
  match(sqlType.columntype)
    .with({ type: 'BOOLEAN' }, { type: 'BOOL' }, () => 'boolean')
    .with({ type: 'TINYINT' }, () => 'number')
    .with({ type: 'SMALLINT' }, () => 'number')
    .with({ type: 'INTEGER' }, () => 'number')
    .with({ type: 'BIGINT' }, () => 'number')
    .with({ type: 'DECIMAL' }, () => 'string')
    .with({ type: 'FLOAT' }, () => 'number')
    .with({ type: 'DOUBLE' }, () => 'number')
    .with({ type: 'VARCHAR' }, () => 'string')
    .with({ type: 'CHAR' }, () => 'string')
    .with({ type: 'TIME' }, () => 'string')
    .with({ type: 'TIMESTAMP' }, () => 'dateTime')
    .with({ type: 'DATE' }, () => 'date')
    .with({ type: 'GEOMETRY' }, () => 'string')
    .with({ type: 'ARRAY' }, () => 'string')
    .otherwise(() => 'string')
