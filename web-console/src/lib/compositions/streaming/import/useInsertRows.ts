// Sends a set of rows to a pipeline table.

import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { getValueFormatter, Row } from '$lib/functions/ddl'
import { ApiError, Field, PipelineId, PipelinesService, Relation } from '$lib/services/manager'
import Papa from 'papaparse'
import { Dispatch, SetStateAction, useCallback } from 'react'

import { useMutation } from '@tanstack/react-query'

type Args = [pipelineId: PipelineId, relation: string, force: boolean, csvData: string]

// We convert fields to a tuple so that we can use it as a line in the CSV we're
// sending to the server.
export function rowToCsvLine(relation: Relation, obj: Row): any[] {
  const tuple: any[] = new Array(relation.fields.length)
  relation.fields.map((col: Field, i: number) => {
    tuple[i] = getValueFormatter(col.columntype)(obj.record[col.name])
  })

  return tuple
}

function useInsertRows() {
  const { pushMessage } = useStatusNotification()

  const { mutate: pipelineInsert, isPending: pipelineInsertLoading } = useMutation<string, ApiError, Args>({
    mutationFn: ([pipelineId, relation, force, csvData]) => {
      return PipelinesService.httpInput(pipelineId, relation, force, 'csv', csvData)
    }
  })

  const insertRows = useCallback(
    (
      pipelineId: PipelineId,
      relation: Relation,
      force: boolean,
      rows: Row[],
      setRows: Dispatch<SetStateAction<Row[]>>
    ) => {
      if (!pipelineInsertLoading) {
        const csvData = Papa.unparse(rows.map(row => rowToCsvLine(relation, row)))
        pipelineInsert([pipelineId, relation.name, force, csvData], {
          onSuccess: () => {
            setRows([])
            pushMessage({ message: `${rows.length} Row(s) inserted`, key: new Date().getTime(), color: 'success' })
          },
          onError: error => {
            pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
          }
        })
      }
    },
    [pipelineInsert, pipelineInsertLoading, pushMessage]
  )

  return insertRows
}

export default useInsertRows
