// Generates rows and inserts them into a table.

import { GridApi } from '@mui/x-data-grid-pro'
import { Dispatch, MutableRefObject, SetStateAction, useCallback } from 'react'
import { Field, Relation } from '$lib/types/manager'
import { StoredFieldSettings } from '$lib/components/streaming/import/RngSettingsDialog'
import { Row, getValueParser } from '$lib/types/ddl'
import { getDefaultRngMethod, getRngMethodByName } from '$lib/components/streaming/import/randomData/generators'

function useGenerateRows(
  apiRef: MutableRefObject<GridApi>,
  setRows: Dispatch<SetStateAction<Row[]>>,
  relation: Relation,
  settings: Map<string, StoredFieldSettings>
) {
  const insertRows = useCallback(
    (rowCount: number) => {
      if (relation) {
        const newRows: Row[] = []
        const curRowCount = apiRef.current?.getRowsCount()

        for (let i = 0; i < rowCount; i++) {
          const row: Row = { genId: curRowCount + i, weight: 1, record: {} }
          relation.fields.forEach((field: Field) => {
            let rngMethod = getDefaultRngMethod(field.columntype)
            const fieldSettings = settings.get(field.name)
            if (fieldSettings && fieldSettings.method) {
              rngMethod = getRngMethodByName(fieldSettings.method, field.columntype) || rngMethod
            }
            const valueParser = getValueParser(field.columntype)
            row.record[field.name] = valueParser(rngMethod.generator(field.columntype, fieldSettings?.config))
          })
          newRows.push(row)
        }

        setRows(prevRows => [...prevRows, ...newRows])
      }
    },
    [apiRef, setRows, relation, settings]
  )

  return insertRows
}

export default useGenerateRows
