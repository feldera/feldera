// Logic to delete a pipeline.

import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { ApiError, PipelineId, PipelinesService } from '$lib/types/manager'
import { useCallback } from 'react'

import { useMutation, useQueryClient } from '@tanstack/react-query'

import { ClientPipelineStatus, usePipelineStateStore } from './StatusContext'

function useDeletePipeline() {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()
  const pipelineStatus = usePipelineStateStore(state => state.clientStatus)
  const { mutate: deletePipeline, isLoading: deletePipelineLoading } = useMutation<string, ApiError, string>(
    PipelinesService.pipelineDelete
  )

  const deletePipelineClick = useCallback(
    (pipeline_id: PipelineId) => {
      if (!deletePipelineLoading && pipelineStatus.get(pipeline_id) == ClientPipelineStatus.INACTIVE) {
        deletePipeline(pipeline_id, {
          onSettled: () => {
            queryClient.invalidateQueries(['pipeline'])
            queryClient.invalidateQueries(['pipelineStatus', { pipeline_id: pipeline_id }])
          },
          onError: error => {
            pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
          }
        })
      }
    },
    [deletePipelineLoading, pushMessage, queryClient, deletePipeline, pipelineStatus]
  )

  return deletePipelineClick
}

export default useDeletePipeline
