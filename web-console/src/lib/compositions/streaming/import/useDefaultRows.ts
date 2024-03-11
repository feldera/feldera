// Generates rows and inserts them into a table.

import { Row, SQLValueJS } from '$lib/functions/ddl'
import { ColumnType, Field, Relation, SqlType } from '$lib/services/manager'
import { BigNumber } from 'bignumber.js'
import dayjs from 'dayjs'
import { Dispatch, MutableRefObject, SetStateAction, useCallback } from 'react'
import { match } from 'ts-pattern'

import { GridApi } from '@mui/x-data-grid-pro'

export const getDefaultValue = (columntype: ColumnType): SQLValueJS =>
  match(columntype)
    .with({ type: SqlType.BOOLEAN }, () => false)
    .with(
      { type: SqlType.TINYINT },
      { type: SqlType.SMALLINT },
      { type: SqlType.INTEGER },
      { type: SqlType.REAL },
      { type: SqlType.DOUBLE },
      () => 0
    )
    .with({ type: SqlType.BIGINT }, { type: SqlType.DECIMAL }, () => new BigNumber(0))
    .with({ type: SqlType.VARCHAR }, { type: SqlType.CHAR }, () => '')
    .with({ type: SqlType.TIME }, () => dayjs(new Date()))
    .with({ type: SqlType.TIMESTAMP }, () => dayjs(new Date()))
    .with({ type: SqlType.DATE }, () => dayjs(new Date()))
    .with({ type: SqlType.ARRAY }, () => '[]')
    .otherwise(() => '')

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
