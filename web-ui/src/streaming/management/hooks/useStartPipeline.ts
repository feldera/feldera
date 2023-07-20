// Logic to deploy (and then start) a given pipeline.

import { useCallback } from 'react'
import { ClientPipelineStatus, usePipelineStateStore } from '../StatusContext'
import { PipelineService, CancelError, PipelineId } from 'src/types/manager'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { invalidatePipeline } from 'src/types/defaultQueryFn'
import useStatusNotification from 'src/components/errors/useStatusNotification'
import { PipelineAction } from 'src/types/pipeline'

function useStartPipeline() {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()
  const pipelineStatus = usePipelineStateStore(state => state.clientStatus)
  const setPipelineStatus = usePipelineStateStore(state => state.setStatus)

  const { mutate: piplineAction, isLoading: pipelineActionLoading } = useMutation<string, CancelError, PipelineAction>({
    mutationFn: (action: PipelineAction) => {
      return PipelineService.pipelineAction(action.pipeline_id, action.command)
    }
  })

  const startPipelineClick = useCallback(
    (pipeline_id: PipelineId) => {
      if (
        !pipelineActionLoading &&
        (pipelineStatus.get(pipeline_id) != ClientPipelineStatus.PAUSED ||
          pipelineStatus.get(pipeline_id) != ClientPipelineStatus.DEPLOYED)
      ) {
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
              pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
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
