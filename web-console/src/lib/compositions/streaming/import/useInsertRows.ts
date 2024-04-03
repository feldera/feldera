// Sends a set of rows to a pipeline table.

import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { Row, sqlRowToXgressJSON } from '$lib/functions/sqlValue'
import { Relation } from '$lib/services/manager'
import { mutationHttpIngressJson } from '$lib/services/pipelineManagerQuery'
import { Dispatch, SetStateAction, useCallback } from 'react'

import { useMutation, useQueryClient } from '@tanstack/react-query'

function useInsertRows() {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()

  const { mutate: pipelineInsert, isPending: pipelineInsertLoading } = useMutation(mutationHttpIngressJson(queryClient))

  const insertRows = useCallback(
    (
      pipelineName: string,
      relation: Relation,
      force: boolean,
      rows: Row[],
      setRows: Dispatch<SetStateAction<Row[]>>
    ) => {
      if (pipelineInsertLoading) {
        return
      }
      pipelineInsert(
        { pipelineName, relation, force, data: rows.map(row => ({ insert: sqlRowToXgressJSON(relation, row) })) },
        {
          onSuccess: () => {
            setRows([])
            pushMessage({ message: `${rows.length} Row(s) inserted`, key: new Date().getTime(), color: 'success' })
          },
          onError: error => {
            pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
          }
        }
      )
    },
    [pipelineInsert, pipelineInsertLoading, pushMessage]
  )

  return insertRows
}

export default useInsertRows
