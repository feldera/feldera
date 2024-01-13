import { EntitySyncIndicatorStatus } from '$lib/components/common/EntitySyncIndicator'
import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { isEmptyObject } from '$lib/functions/common/object'
import { ApiError, UpdatePipelineRequest, UpdatePipelineResponse } from '$lib/services/manager'
import { mutationUpdatePipeline, pipelineQueryCacheUpdate } from '$lib/services/pipelineManagerQuery'
import { useRouter } from 'next/navigation'
import { Dispatch, SetStateAction, useCallback, useEffect } from 'react'
import invariant from 'tiny-invariant'
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

  // Used to accumulate changes that are being debounced
  // const [requestAggregate, setRequestAggregate] = useState<UpdatePipelineRequest>({
  //   name: undefined!,
  //   description: undefined!
  // })
  const requestAggregate = useRequestAggregate(s => s.requestAggregate)
  const setRequestAggregate = useRequestAggregate(s => s.setRequestAggregate)
  const isEmptyRequest = isEmptyObject(requestAggregate)

  const mutation = mutationUpdatePipeline(queryClient)
  const { mutate: updatePipeline, isPending } = useMutation({
    ...mutation,
    // onMutate: (args) => {
    //   setRequestAggregate({} as any)
    //   return args.request
    // },
    // onError(_error, _variables, context) {
    //   invariant(context, 'mutationUpdatePipeline here should have context')
    //   setRequestAggregate(context)
    // },

    // onSuccess and onError are defined here because the body of callbacks passed in mutate() call
    // will be lost and not executed if owner component was unmounted
    onSuccess: (_data, variables, context) => {
      mutation.onSuccess?.(_data, variables, context)
      console.log('updatePipeline onSuccess')
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

  const update = useCallback((pipelineName: string) => {
    console.log('debouncedUpdate', pipelineName, requestAggregate)
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
    updatePipeline(
      { pipelineName, request: requestAggregate },
      {
        // onSuccess: (_data, variables) => {
        //   console.log('updatePipeline onSuccess')
        //   setRequestAggregate({} as any)
        //   setStatus('isUpToDate')
        //   setFormError({} as FormError)
        //   router.replace(`/streaming/builder/?pipeline_name=${variables.request.name ?? variables.pipelineName}`)
        // },
        // onError: (error, _args) => {
        //   // TODO: would be good to have error codes from the API
        //   if (error.message.includes('name already exists')) {
        //     setFormError({ name: { message: 'This name already exists. Enter a different name.' } } as FormError)
        //     setStatus('isUpToDate')
        //   } else {
        //     pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
        //     setStatus('isUpToDate')
        //   }
        // }
      }
    )
  }, [isPending, updatePipeline, pushMessage, requestAggregate, router, setFormError, setRequestAggregate, setStatus])

  const debouncedUpdate = useDebouncedCallback(update, SAVE_DELAY)

  // If the component that owns the useUpdatePipeline hook unmounts, the debounced callback is lost.
  // To prevent that, we force run the callback on unmount via useEffect.
  // useEffect(() => {
  //   return () => {
  //     console.log('pipeline unmount update', isEmptyRequest)
  //     // Ideally this would not be needed, but often unmount happens when debounce is not initiated
  //     if (isEmptyRequest) {
  //       return
  //     }
  //     update(pipelineName)
  //   }
  // }, [pipelineName, isEmptyRequest, update, isPending])

  // If the component that owns the useUpdatePipeline hook unmounts, the debounced callback is lost.
  // To prevent that, we force flush the debounced callback on unmount via useEffect.
  useEffect(() => {
    return () => {
      console.log('pipeline unmount update', debouncedUpdate.isPending())
      debouncedUpdate.flush()
    }
  }, [debouncedUpdate])

  const mutateUpdatePipeline = (pipelineName: string, updateRequest: UpdatePipelineRequest) => {
    setRequestAggregate(updateRequest)
    pipelineQueryCacheUpdate(queryClient, pipelineName, updateRequest)
    if (pipelineName) {
      setStatus('isModified')
      console.log('called debouncedUpdate', updateRequest)
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
