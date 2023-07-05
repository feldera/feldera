// Logic to shutdown a stopped pipeline.

import { useCallback } from 'react'
import { ClientPipelineStatus, usePipelineStateStore } from '../StatusContext'
import { PipelineService, CancelError, PipelineId, PipelineStatus } from 'src/types/manager'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { pipelineStatusQueryCacheUpdate } from 'src/types/defaultQueryFn'
import useStatusNotification from 'src/components/errors/useStatusNotification'
import { PipelineAction } from 'src/types/pipeline'

function useShutdownPipeline() {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()
  const pipelineStatus = usePipelineStateStore(state => state.clientStatus)
  const setPipelineStatus = usePipelineStateStore(state => state.setStatus)

  const { mutate: piplineAction, isLoading: pipelineActionLoading } = useMutation<string, CancelError, PipelineAction>({
    mutationFn: (action: PipelineAction) => {
      return PipelineService.pipelineAction(action.pipeline_id, action.command)
    }
  })

  const shutdownPipelineClick = useCallback(
    (pipeline_id: PipelineId) => {
      if (!pipelineActionLoading && pipelineStatus.get(pipeline_id) == ClientPipelineStatus.PAUSED) {
        setPipelineStatus(pipeline_id, ClientPipelineStatus.SHUTTING_DOWN)
        piplineAction(
          { pipeline_id: pipeline_id, command: 'shutdown' as const },
          {
            onSettled: () => {
              queryClient.invalidateQueries(['pipeline'])
              queryClient.invalidateQueries(['pipelineStatus', { pipeline_id: pipeline_id }])
            },
            onSuccess: () => {
              setPipelineStatus(pipeline_id, ClientPipelineStatus.INACTIVE)
              pipelineStatusQueryCacheUpdate(queryClient, pipeline_id, PipelineStatus.SHUTDOWN)
            },
            onError: error => {
              pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
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
