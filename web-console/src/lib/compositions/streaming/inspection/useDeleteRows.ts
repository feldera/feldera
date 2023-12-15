import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { ApiError, HttpInputOutputService } from '$lib/services/manager'
import { useCallback } from 'react'
import JSONbig from 'true-json-bigint'

import { useMutation } from '@tanstack/react-query'

type Args = [
  pipelineId: string,
  relation: string,
  force: boolean,
  rows: Partial<Record<'insert' | 'delete', Record<string, unknown>>>[],
  isArray?: boolean
]

export function useInsertDeleteRows() {
  const { pushMessage } = useStatusNotification()

  const { mutate: pipelineDelete, isPending } = useMutation<string, ApiError, Args>({
    mutationFn: ([pipelineId, relation, force, rows, isArray]) => {
      return HttpInputOutputService.httpInput(
        pipelineId,
        relation,
        force,
        'json',
        isArray ? JSONbig.stringify(rows) : rows.map(row => JSONbig.stringify(row)).join(''),
        true
      )
    }
  })

  return useCallback(
    (...[pipelineId, relation, force, rows, isArray = false]: Args) => {
      const rowsLen = Object.keys(rows).length
      if (!isPending) {
        pipelineDelete([pipelineId, relation, force, rows, isArray], {
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
        })
      }
    },
    [pipelineDelete, isPending, pushMessage]
  )
}
