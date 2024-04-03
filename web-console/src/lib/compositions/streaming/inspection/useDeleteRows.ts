import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { Row, sqlRowToXgressJSON } from '$lib/functions/sqlValue'
import { Relation } from '$lib/services/manager'
import { mutationHttpIngressJson } from '$lib/services/pipelineManagerQuery'
import { useCallback } from 'react'

import { useMutation, useQueryClient } from '@tanstack/react-query'

export function useInsertDeleteRows() {
  const { pushMessage } = useStatusNotification()
  const queryClient = useQueryClient()

  const { mutate: pipelineDelete, isPending } = useMutation(mutationHttpIngressJson(queryClient))

  return useCallback(
    (pipelineName: string, relation: Relation, force: boolean, rows: ({ insert: Row } | { delete: Row })[]) => {
      const rowsLen = rows.length
      if (!isPending) {
        pipelineDelete(
          {
            pipelineName,
            relation,
            force,
            data: rows.map(entry =>
              (([action, row]) => ({ [action]: sqlRowToXgressJSON(relation, row) }))(Object.entries(entry)[0])
            )
          },
          {
            onSuccess: () => {
              pushMessage({
                message: `${rowsLen} ` + (rowsLen > 1 ? 'rows deleted' : 'row deleted'),
                key: new Date().getTime(),
                color: 'success'
              })
            },
            onError: error => {
              pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
            }
          }
        )
      }
    },
    [pipelineDelete, isPending, pushMessage]
  )
}
