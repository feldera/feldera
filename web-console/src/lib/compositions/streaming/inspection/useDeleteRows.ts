import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { ApiError, PipelinesService } from '$lib/services/manager'
import { useCallback } from 'react'

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

  const { mutate: pipelineDelete, isLoading } = useMutation<string, ApiError, Args>({
    mutationFn: ([pipelineId, relation, force, rows, isArray]) => {
      return isArray
        ? PipelinesService.httpInput(pipelineId, relation, force, 'json', JSON.stringify(rows), true)
        : PipelinesService.httpInput(pipelineId, relation, force, 'json', rows.map(row => JSON.stringify(row)).join(''))
    }
  })

  return useCallback(
    (...[pipelineId, relation, force, rows, isArray = false]: Args) => {
      const rowsLen = Object.keys(rows).length
      if (!isLoading) {
        pipelineDelete([pipelineId, relation, force, rows, isArray], {
          onSuccess: () => {
            pushMessage({
              message: `${rowsLen}` + (rowsLen > 1 ? 'Rows deleted' : 'Row deleted'),
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
    [pipelineDelete, isLoading, pushMessage]
  )
}
