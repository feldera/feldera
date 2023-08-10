// Sends a set of rows to a pipeline table.

import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { getValueFormatter, Row } from '$lib/types/ddl'
import { ApiError, Field, PipelineId, PipelinesService, Relation } from '$lib/types/manager'
import Papa from 'papaparse'
import { useCallback } from 'react'

import { useMutation } from '@tanstack/react-query'

export interface TableInsert {
  pipeline_id: PipelineId
  relation: string
  csv_data: string
}

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

  const { mutate: pipelineInsert, isLoading: pipelineInsertLoading } = useMutation<string, ApiError, TableInsert>({
    mutationFn: (args: TableInsert) => {
      return PipelinesService.httpInput(args.pipeline_id, args.relation, 'csv', args.csv_data)
    }
  })

  const insertRows = useCallback(
    (pipeline_id: PipelineId, relation: Relation, rows: Row[]) => {
      if (!pipelineInsertLoading) {
        const csv_data = Papa.unparse(rows.map(row => rowToCsvLine(relation, row)))
        pipelineInsert(
          { pipeline_id, relation: relation.name, csv_data },
          {
            onSuccess: () => {
              pushMessage({ message: `${rows.length} Row(s) inserted`, key: new Date().getTime(), color: 'success' })
            },
            onError: error => {
              pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
            }
          }
        )
      }
    },
    [pipelineInsert, pipelineInsertLoading, pushMessage]
  )

  return insertRows
}

export default useInsertRows
