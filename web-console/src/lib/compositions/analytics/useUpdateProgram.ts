import { EntitySyncIndicatorStatus } from '$lib/components/common/EntitySyncIndicator'
import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { ApiError, UpdateProgramRequest, UpdateProgramResponse } from '$lib/services/manager'
import { mutationUpdateProgram, programQueryCacheUpdate } from '$lib/services/pipelineManagerQuery'
import { useRouter } from 'next/navigation'
import { Dispatch, SetStateAction, useState } from 'react'
import { useDebouncedCallback } from 'use-debounce'

import { useMutation, useQueryClient } from '@tanstack/react-query'

// How many ms to wait until we save the project.
const SAVE_DELAY = 1000

// The error format for the editor form.
interface FormError {
  name?: { message?: string }
}

export const useUpdateProgram = (
  programName: string,
  setStatus: Dispatch<SetStateAction<EntitySyncIndicatorStatus>>,
  setFormError: Dispatch<SetStateAction<FormError>>
) => {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()
  const router = useRouter()

  const { mutate, isPending } = useMutation(mutationUpdateProgram(queryClient))

  // Used to accumulate changes that are being debounced
  const [requestAggregate, setRequestAggregate] = useState<UpdateProgramRequest>({})

  const debouncedUpdate = useDebouncedCallback((programName: string | undefined, request: UpdateProgramRequest) => {
    if (!programName) {
      return
    }
    if (request.name === '') {
      setFormError({ name: { message: 'Program needs to have a name.' } })
      return
    }
    if (isPending) {
      return
    }
    setStatus('isSaving')
    mutate(
      { programName, update_request: request },
      {
        onSuccess: (_data: UpdateProgramResponse, variables) => {
          setRequestAggregate({})
          setStatus('isUpToDate')
          setFormError({})
          router.replace(`/analytics/editor/?program_name=${variables.update_request.name ?? variables.programName}`)
        },
        onError: (error: ApiError) => {
          // TODO: would be good to have error codes from the API
          if (error.message.includes('name already exists')) {
            setFormError({ name: { message: 'This name is already in use. Enter a different name.' } })
            setStatus('isModified')
          } else {
            pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
          }
        }
      }
    )
  }, SAVE_DELAY)
  const mutateUpdateProgram = (programName: string, updateRequest: UpdateProgramRequest) => {
    setRequestAggregate(updateRequest)
    programQueryCacheUpdate(queryClient, programName, updateRequest)
    if (programName) {
      setStatus('isModified')
      debouncedUpdate(programName, updateRequest)
    }
  }
  return (programAction: SetStateAction<UpdateProgramRequest>) => {
    const programRequest = programAction instanceof Function ? programAction(requestAggregate) : programAction
    return mutateUpdateProgram(programName, programRequest)
  }
}
