// Logic to pause a running pipeline.

import { useCallback } from 'react'
import { ClientPipelineStatus, usePipelineStateStore } from '../StatusContext'
import { PipelinesService, CancelError, PipelineId } from 'src/types/manager'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { invalidatePipeline } from 'src/types/defaultQueryFn'
import useStatusNotification from 'src/components/errors/useStatusNotification'
import { PipelineAction } from 'src/types/pipeline'

function usePausePipeline() {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()
  const pipelineStatus = usePipelineStateStore(state => state.clientStatus)
  const setPipelineStatus = usePipelineStateStore(state => state.setStatus)

  const { mutate: piplineAction, isLoading: pipelineActionLoading } = useMutation<string, CancelError, PipelineAction>({
    mutationFn: (action: PipelineAction) => {
      return PipelinesService.pipelineAction(action.pipeline_id, action.command)
    }
  })

  const pausePipelineClick = useCallback(
    (pipeline_id: PipelineId) => {
      if (!pipelineActionLoading && pipelineStatus.get(pipeline_id) == ClientPipelineStatus.RUNNING) {
        setPipelineStatus(pipeline_id, ClientPipelineStatus.PAUSING)
        piplineAction(
          { pipeline_id: pipeline_id, command: 'pause' },
          {
            onSettled: () => {
              invalidatePipeline(queryClient, pipeline_id)
            },
            onError: error => {
              pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
              setPipelineStatus(pipeline_id, ClientPipelineStatus.RUNNING)
            }
          }
        )
      }
    },
    [pipelineActionLoading, pushMessage, queryClient, piplineAction, pipelineStatus, setPipelineStatus]
  )

  return pausePipelineClick
}

export default usePausePipeline
