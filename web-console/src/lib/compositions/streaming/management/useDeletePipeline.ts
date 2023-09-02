// Logic to delete a pipeline.

import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { ClientPipelineStatus, usePipelineStateStore } from '$lib/compositions/streaming/management/StatusContext'
import { invalidateQuery } from '$lib/functions/common/tanstack'
import { ApiError, PipelineId, PipelinesService } from '$lib/services/manager'
import { PipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import { useCallback } from 'react'

import { useMutation, useQueryClient } from '@tanstack/react-query'

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
            invalidateQuery(queryClient, PipelineManagerQuery.pipeline())
            invalidateQuery(queryClient, PipelineManagerQuery.pipelineStatus(pipeline_id))
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
