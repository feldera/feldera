// Logic to delete a pipeline.

import { useCallback } from 'react'
import { ClientPipelineStatus, usePipelineStateStore } from '../StatusContext'
import { PipelinesService, ApiError, PipelineId } from 'src/types/manager'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import useStatusNotification from 'src/components/errors/useStatusNotification'

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
