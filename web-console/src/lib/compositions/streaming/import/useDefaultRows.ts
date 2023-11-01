// Generates rows and inserts them into a table.

import { Row } from '$lib/functions/ddl'
import { ColumnType, Field, Relation } from '$lib/services/manager'
import dayjs from 'dayjs'
import { Dispatch, MutableRefObject, SetStateAction, useCallback } from 'react'
import { match } from 'ts-pattern'

import { GridApi } from '@mui/x-data-grid-pro'

export const getDefaultValue = (columntype: ColumnType) =>
  match(columntype)
    .with({ type: 'BOOLEAN' }, () => false)
    .with({ type: 'TINYINT' }, { type: 'SMALLINT' }, { type: 'INTEGER' }, { type: 'BIGINT' }, () => 0)
    .with({ type: 'DECIMAL' }, { type: 'FLOAT' }, { type: 'DOUBLE' }, () => 0.0)
    .with({ type: 'VARCHAR' }, { type: 'CHAR' }, () => '')
    .with({ type: 'TIME' }, () => dayjs(new Date()))
    .with({ type: 'TIMESTAMP' }, () => dayjs(new Date()))
    .with({ type: 'DATE' }, () => dayjs(new Date()))
    .with({ type: 'GEOMETRY' }, () => 'st_point(0.0, 0.0)')
    .with({ type: 'ARRAY' }, () => '[]')
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
