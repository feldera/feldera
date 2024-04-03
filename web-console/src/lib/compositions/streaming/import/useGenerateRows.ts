// Generates rows and inserts them into a table.

import { getDefaultRngMethod, getRngMethodByName } from '$lib/components/streaming/import/randomData/generators'
import { StoredFieldSettings } from '$lib/components/streaming/import/RngSettingsDialog'
import { Row } from '$lib/functions/sqlValue'
import { Field, Relation } from '$lib/services/manager'
import { Dispatch, SetStateAction, useCallback } from 'react'

function useGenerateRows(
  rowCount: number,
  setRows: Dispatch<SetStateAction<Row[]>>,
  relation: Relation,
  settings: Map<string, StoredFieldSettings>
) {
  const insertRows = useCallback(
    (count: number) => {
      if (!relation) {
        return
      }
      const newRows: Row[] = []
      for (let i = 0; i < count; i++) {
        const row: Row = { genId: rowCount + i, weight: 1, record: {} }
        relation.fields.forEach((field: Field) => {
          let rngMethod = getDefaultRngMethod(field.columntype)
          const fieldSettings = settings.get(field.name)
          if (fieldSettings && fieldSettings.method) {
            rngMethod = getRngMethodByName(fieldSettings.method, field.columntype) || rngMethod
          }
          row.record[field.name] = rngMethod.generator(field.columntype, fieldSettings?.config)
        })
        newRows.push(row)
      }

      setRows(prevRows => [...prevRows, ...newRows])
    },
    [rowCount, setRows, relation, settings]
  )

  return insertRows
}

export default useGenerateRows
