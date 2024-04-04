// Generates rows and inserts them into a table.

import { Row, SQLValueJS } from '$lib/functions/ddl'
import { ColumnType, Field, Relation } from '$lib/services/manager'
import { BigNumber } from 'bignumber.js'
import dayjs from 'dayjs'
import { Dispatch, MutableRefObject, SetStateAction, useCallback } from 'react'
import invariant from 'tiny-invariant'
import { match, P } from 'ts-pattern'

import { GridApi } from '@mui/x-data-grid-pro'

export const getDefaultValue = (columntype: ColumnType): SQLValueJS =>
  match(columntype)
    .with({ type: undefined }, () => invariant(columntype.type) as never)
    .with({ type: 'BOOLEAN' }, () => false)
    .with({ type: 'TINYINT' }, { type: 'SMALLINT' }, { type: 'INTEGER' }, { type: 'REAL' }, { type: 'DOUBLE' }, () => 0)
    .with({ type: 'BIGINT' }, { type: 'DECIMAL' }, () => new BigNumber(0))
    .with({ type: 'VARCHAR' }, { type: 'CHAR' }, () => '')
    .with({ type: 'TIME' }, () => dayjs(new Date()))
    .with({ type: 'TIMESTAMP' }, () => dayjs(new Date()))
    .with({ type: 'DATE' }, () => dayjs(new Date()))
    .with({ type: 'ARRAY' }, () => [])
    .with({ type: 'BINARY' }, () => invariant(false, 'BINARY not implemented') as never)
    .with({ type: 'VARBINARY' }, () => invariant(false, 'VARBINARY not implemented') as never)
    .with({ type: { Interval: P._ } }, () => invariant(false, 'INTERVAL not supported for ingress') as never)
    .with({ type: 'STRUCT' }, () => new Map())
    .with({ type: 'NULL' }, () => invariant(false, 'NULL not supported for ingress') as never)
    .exhaustive()

function useDefaultRows(
  apiRef: MutableRefObject<GridApi>,
  setRows: Dispatch<SetStateAction<Row[]>>,
  relation: Relation
) {
  const insertRows = useCallback(
    (rowCount: number) => {
      if (relation) {
        const newRows: Row[] = []
        const curRowCount = apiRef.current?.getRowsCount()

        for (let i = 0; i < rowCount; i++) {
          const row: Row = { genId: curRowCount + i, weight: 1, record: {} }
          relation.fields.forEach((field: Field) => {
            row.record[field.name] = getDefaultValue(field.columntype)
          })
          newRows.push(row)
        }

        setRows(prevRows => [...prevRows, ...newRows])
      }
    },
    [apiRef, setRows, relation]
  )

  return insertRows
}

export default useDefaultRows
