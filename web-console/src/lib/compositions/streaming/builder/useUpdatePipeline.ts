import { EntitySyncIndicatorStatus } from '$lib/components/common/EntitySyncIndicator'
import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { isEmptyObject } from '$lib/functions/common/object'
import { ApiError, UpdatePipelineRequest, UpdatePipelineResponse } from '$lib/services/manager'
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

  const { mutate, isPending } = useMutation(mutationUpdatePipeline(queryClient))

  // Used to accumulate changes that are being debounced
  // const [requestAggregate, setRequestAggregate] = useState<UpdatePipelineRequest>({
  //   name: undefined!,
  //   description: undefined!
  // })
  const requestAggregate = useRequestAggregate(s => s.requestAggregate)
  const setRequestAggregate = useRequestAggregate(s => s.setRequestAggregate)
  const isEmptyRequest = isEmptyObject(requestAggregate)

  const update = useCallback((pipelineName: string) => {
    console.log('debouncedUpdate', requestAggregate)
    if (!pipelineName) {
      return
    }
    if (isPending) {
      return
    }
    // // Ideally this would not be needed, but dummy requests are sent when forcing request on unmount
    // if (isEmptyObject(requestAggregate)) {
    //   return
    // }

    setStatus('isSaving')
    mutate(
      { pipelineName, request: requestAggregate },
      {
        onSuccess: (_data: UpdatePipelineResponse, variables) => {
          setRequestAggregate({} as any)
          setStatus('isUpToDate')
          setFormError({} as FormError)
          router.replace(`/streaming/builder/?pipeline_name=${variables.request.name ?? variables.pipelineName}`)
        },
        onError: (error: ApiError) => {
          // TODO: would be good to have error codes from the API
          if (error.message.includes('name already exists')) {
            setFormError({ name: { message: 'This name already exists. Enter a different name.' } } as FormError)
            setStatus('isUpToDate')
          } else {
            pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
            setStatus('isUpToDate')
          }
        }
      }
    )
  }, [isPending, mutate, pushMessage, requestAggregate, router, setFormError, setRequestAggregate, setStatus])

  const debouncedUpdate = useDebouncedCallback(update, SAVE_DELAY)

  // If the component that owns the useUpdatePipeline hook unmounts, the debounced callback is lost.
  // To prevent that, we force run the callback on unmount via useEffect.
  useEffect(() => {
    return () => {
      // Ideally this would not be needed, but often unmount happens when debounce is not initiated
      if (isEmptyRequest) {
        return
      }
      update(pipelineName)
    }
  }, [pipelineName, isEmptyRequest, update])

  const mutateUpdatePipeline = (pipelineName: string, updateRequest: UpdatePipelineRequest) => {
    setRequestAggregate(updateRequest)
    pipelineQueryCacheUpdate(queryClient, pipelineName, updateRequest)
    if (pipelineName) {
      setStatus('isModified')
      debouncedUpdate(pipelineName)
    }
  }
  return (pipelineAction: SetStateAction<UpdatePipelineRequest>) => {
    // invariant(pipelineName, 'Tried to update pipeline, but pipelineName is empty')
    const pipelineRequest = pipelineAction instanceof Function ? pipelineAction(requestAggregate) : pipelineAction
    console.log(`update pipeline call <${pipelineName}>`, pipelineRequest)
    return mutateUpdatePipeline(pipelineName, pipelineRequest)
  }
}
