import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { ApiError, PipelineId } from '$lib/services/manager'
import { useCallback } from 'react'

import { QueryClient, useMutation, UseMutationOptions, useQueryClient } from '@tanstack/react-query'

export function usePipelineMutation(
  mutation: (queryClient: QueryClient) => UseMutationOptions<string, ApiError, PipelineId>
) {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()

  const { mutate: piplineAction } = useMutation(useCallback(() => mutation(queryClient), [mutation, queryClient])())

  const pipelineAction = useCallback(
    (pipelineId: PipelineId) => {
      piplineAction(pipelineId, {
        onError: error => {
          pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
        }
      })
    },
    [piplineAction, pushMessage]
  )

  return pipelineAction
}
