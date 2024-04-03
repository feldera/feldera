// Generates rows and inserts them into a table.

import { Row, SQLValueJS } from '$lib/functions/sqlValue'
import { ColumnType, Field, Relation } from '$lib/services/manager'
import { BigNumber } from 'bignumber.js/bignumber.js'
import dayjs from 'dayjs'
import { Dispatch, SetStateAction, useCallback } from 'react'
import invariant from 'tiny-invariant'
import { match, P } from 'ts-pattern'

export const getDefaultValue = (columntype: ColumnType): SQLValueJS =>
  match(columntype)
    .with({ type: undefined }, () => invariant(columntype.type) as never)
    .with({ type: 'BOOLEAN' }, () => false)
    .with({ type: 'TINYINT' }, { type: 'SMALLINT' }, { type: 'INTEGER' }, { type: 'REAL' }, { type: 'DOUBLE' }, () => 0)
    .with({ type: 'BIGINT' }, { type: 'DECIMAL' }, () => new BigNumber(0))
    .with({ type: 'VARCHAR' }, { type: 'CHAR' }, () => '')
    .with({ type: 'TIME' }, () => dayjs('00:00:00'))
    .with({ type: 'TIMESTAMP' }, () => dayjs('01.01.1970 00:00:00.000'))
    .with({ type: 'DATE' }, () => dayjs('01.01.1970'))
    .with({ type: 'ARRAY' }, () => [])
    .with({ type: 'BINARY' }, () => invariant(false, 'BINARY not implemented') as never)
    .with({ type: 'VARBINARY' }, () => invariant(false, 'VARBINARY not implemented') as never)
    .with({ type: { Interval: P._ } }, () => invariant(false, 'INTERVAL not supported for ingress') as never)
    .with({ type: 'STRUCT' }, () => new Map())
    .with({ type: 'NULL' }, () => invariant(false, 'NULL not supported for ingress') as never)
    .exhaustive()

function useDefaultRows(rowCount: number, setRows: Dispatch<SetStateAction<Row[]>>, relation: Relation) {
  const insertRows = useCallback(
    (count: number) => {
      if (relation) {
        const newRows: Row[] = []

        for (let i = 0; i < count; i++) {
          const row: Row = { genId: rowCount + i, weight: 1, record: {} }
          relation.fields.forEach((field: Field) => {
            row.record[field.name] = getDefaultValue(field.columntype)
          })
          newRows.push(row)
        }

        setRows(prevRows => [...prevRows, ...newRows])
      }
    },
    [rowCount, setRows, relation]
  )

  return insertRows
}

export default useDefaultRows
