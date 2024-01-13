import { EntitySyncIndicatorStatus } from '$lib/components/common/EntitySyncIndicator'
import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { UpdatePipelineRequest } from '$lib/services/manager'
import { mutationUpdatePipeline, pipelineQueryCacheUpdate } from '$lib/services/pipelineManagerQuery'
import { useRouter } from 'next/navigation'
import { Dispatch, SetStateAction, useCallback, useEffect } from 'react'
import { useDebouncedCallback } from 'use-debounce'
import { create } from 'zustand'

import { useMutation, useQueryClient } from '@tanstack/react-query'

const SAVE_DELAY = 1000

const useRequestAggregate = create<{
  requestAggregate: UpdatePipelineRequest
  setRequestAggregate: Dispatch<UpdatePipelineRequest>
}>(set => ({
  requestAggregate: {} as any,
  setRequestAggregate: requestAggregate => set({ requestAggregate })
}))

/**
 * Immediately updates cache of pipelineData query, and debounces mutation query to update server state
 * Aggregates requested updates between multiple calls within a single debounce
 * @param pipelineName
 * @param setStatus
 * @param setFormError
 * @returns
 */
export const useUpdatePipeline = <FormError extends { name?: { message: string } }>(
  pipelineName: string,
  setStatus: Dispatch<EntitySyncIndicatorStatus>,
  setFormError: Dispatch<FormError>
) => {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()
  const router = useRouter()

  const requestAggregate = useRequestAggregate(s => s.requestAggregate)
  const setRequestAggregate = useRequestAggregate(s => s.setRequestAggregate)

  const mutation = mutationUpdatePipeline(queryClient)
  const { mutate: updatePipeline, isPending } = useMutation({
    ...mutation,
    // onSuccess and onError are defined here because the body of callbacks passed in mutate() call
    // will be lost and not executed if owner component was unmounted
    onSuccess: (_data, variables, context) => {
      mutation.onSuccess?.(_data, variables, context)
      setRequestAggregate({} as any)
      setStatus('isUpToDate')
      setFormError({} as FormError)
      router.replace(`/streaming/builder/?pipeline_name=${variables.request.name ?? variables.pipelineName}`)
    },
    onError: (error, _args, context) => {
      mutation.onError?.(error, _args, context)
      // TODO: would be good to have error codes from the API
      if (error.message.includes('name already exists')) {
        setFormError({ name: { message: 'This name already exists. Enter a different name.' } } as FormError)
        setStatus('isUpToDate')
      } else {
        pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
        setStatus('isUpToDate')
      }
    }
  })

  const update = useCallback(
    (pipelineName: string) => {
      if (!pipelineName) {
        return
      }
      if (isPending) {
        return
      }

      setStatus('isSaving')
      updatePipeline({ pipelineName, request: requestAggregate })
    },
    [isPending, updatePipeline, requestAggregate, setStatus]
  )

  const debouncedUpdate = useDebouncedCallback(update, SAVE_DELAY)

  // If the component that owns the useUpdatePipeline hook unmounts, the debounced callback is lost.
  // To prevent that, we force flush the debounced callback on unmount via useEffect.
  useEffect(() => {
    return () => {
      debouncedUpdate.flush()
    }
  }, [debouncedUpdate])

  const mutateUpdatePipeline = (pipelineName: string, updateRequest: UpdatePipelineRequest) => {
    setRequestAggregate(updateRequest)
    pipelineQueryCacheUpdate(queryClient, pipelineName, updateRequest)
    if (pipelineName) {
      setStatus('isModified')
      debouncedUpdate(pipelineName)
    }
  }
  return (pipelineAction: SetStateAction<UpdatePipelineRequest>) => {
    const pipelineRequest = pipelineAction instanceof Function ? pipelineAction(requestAggregate) : pipelineAction
    return mutateUpdatePipeline(pipelineName, pipelineRequest)
  }
}
