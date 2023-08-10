// Logic to shutdown a stopped pipeline.

import { useCallback } from 'react'
import { ClientPipelineStatus, usePipelineStateStore } from '$lib/compositions/streaming/management/StatusContext'
import { PipelinesService, ApiError, PipelineId } from '$lib/types/manager'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { PipelineAction } from '$lib/types/pipeline'
import { invalidatePipeline } from '$lib/types/defaultQueryFn'

function useShutdownPipeline() {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()
  const pipelineStatus = usePipelineStateStore(state => state.clientStatus)
  const setPipelineStatus = usePipelineStateStore(state => state.setStatus)

  const { mutate: piplineAction, isLoading: pipelineActionLoading } = useMutation<string, ApiError, PipelineAction>({
    mutationFn: (action: PipelineAction) => {
      return PipelinesService.pipelineAction(action.pipeline_id, action.command)
    }
  })

  const shutdownPipelineClick = useCallback(
    (pipeline_id: PipelineId) => {
      if (
        !pipelineActionLoading &&
        (pipelineStatus.get(pipeline_id) == ClientPipelineStatus.PAUSED ||
          pipelineStatus.get(pipeline_id) == ClientPipelineStatus.RUNNING ||
          pipelineStatus.get(pipeline_id) == ClientPipelineStatus.FAILED)
      ) {
        setPipelineStatus(pipeline_id, ClientPipelineStatus.SHUTTING_DOWN)
        piplineAction(
          { pipeline_id: pipeline_id, command: 'shutdown' as const },
          {
            onSettled: () => {
              invalidatePipeline(queryClient, pipeline_id)
            },
            onError: error => {
              pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
              setPipelineStatus(pipeline_id, ClientPipelineStatus.PAUSED)
            }
          }
        )
      }
    },
    [pipelineActionLoading, pushMessage, queryClient, piplineAction, pipelineStatus, setPipelineStatus]
  )

  return shutdownPipelineClick
}

export default useShutdownPipeline
