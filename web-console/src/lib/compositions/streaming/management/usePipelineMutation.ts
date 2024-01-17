import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { ApiError } from '$lib/services/manager'
import { useCallback } from 'react'

import { QueryClient, useMutation, UseMutationOptions, useQueryClient } from '@tanstack/react-query'

export function usePipelineMutation(
  mutation: (queryClient: QueryClient) => UseMutationOptions<string, ApiError, string>
) {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()

  const { mutate: piplineAction } = useMutation(useCallback(() => mutation(queryClient), [mutation, queryClient])())

  const pipelineAction = useCallback(
    (pipelineName: string) => {
      piplineAction(pipelineName, {
        onError: error => {
          pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
        }
      })
    },
    [piplineAction, pushMessage]
  )

  return pipelineAction
}
