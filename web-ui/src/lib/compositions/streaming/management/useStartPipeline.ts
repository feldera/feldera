// Logic to start a given pipeline.

import { useCallback } from 'react'
import { ClientPipelineStatus, usePipelineStateStore } from '$lib/compositions/streaming/management/StatusContext'
import { PipelinesService, ApiError, PipelineId } from '$lib/types/manager'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { invalidatePipeline } from '$lib/types/defaultQueryFn'
import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { PipelineAction } from '$lib/types/pipeline'

function useStartPipeline() {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()
  const pipelineStatus = usePipelineStateStore(state => state.clientStatus)
  const setPipelineStatus = usePipelineStateStore(state => state.setStatus)

  const { mutate: piplineAction, isLoading: pipelineActionLoading } = useMutation<string, ApiError, PipelineAction>({
    mutationFn: (action: PipelineAction) => {
      return PipelinesService.pipelineAction(action.pipeline_id, action.command)
    }
  })

  const startPipelineClick = useCallback(
    (pipeline_id: PipelineId) => {
      if (!pipelineActionLoading && pipelineStatus.get(pipeline_id) != ClientPipelineStatus.RUNNING) {
        setPipelineStatus(pipeline_id, ClientPipelineStatus.STARTING)
        piplineAction(
          {
            pipeline_id: pipeline_id,
            command: 'start' as const
          },
          {
            onSettled: () => {
              invalidatePipeline(queryClient, pipeline_id)
            },
            onError: error => {
              pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
              setPipelineStatus(pipeline_id, ClientPipelineStatus.STARTUP_FAILURE)
            }
          }
        )
      }
    },
    [piplineAction, pipelineActionLoading, queryClient, pushMessage, setPipelineStatus, pipelineStatus]
  )

  return startPipelineClick
}

export default useStartPipeline
