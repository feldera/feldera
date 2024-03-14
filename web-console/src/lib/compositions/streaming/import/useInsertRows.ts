// Sends a set of rows to a pipeline table.

import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { Row, SQLValueJS } from '$lib/functions/ddl'
import { Relation } from '$lib/services/manager'
import { mutationHttpIngressJson } from '$lib/services/pipelineManagerQuery'
import { Dispatch, SetStateAction, useCallback } from 'react'

import { useMutation, useQueryClient } from '@tanstack/react-query'

const rowToIngressJSON =
  <Action extends 'insert' | 'delete'>(action: Action) =>
  (row: Row) =>
    ({
      [action]: row.record
    }) as Record<Action, Record<string, SQLValueJS>>

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
        { pipelineName, relation, force, data: rows.map(rowToIngressJSON('insert')) },
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
