import { groupBy } from '$lib/functions/common/array'
import { setQueryData } from '$lib/functions/common/tanstack'
import { accumulatePipelineMetrics, PipelineMetrics } from '$lib/functions/pipelineMetrics'
import { ReportDetails } from '$lib/services/feedback'
import { PipelinesService } from '$lib/services/manager'
import { GlobalMetricsTimestamp, InputEndpointMetrics, OutputEndpointMetrics } from '$lib/types/pipeline'

import { QueryClient } from '@tanstack/react-query'

export type SystemError = Error & {
  message: string
  cause: {
    source: string
    report: ReportDetails
    tag: string
    body: any
  }
}

export const pipelineManagerAggregateQuery = {
  systemErrors: () => ({
    queryKey: ['systemErrors'],
    queryFn: async () => [] as SystemError[]
  }),
  pipelineMetrics: (pipelineName: string, refetchMs: number, keepMs?: number) => ({
    queryKey: ['pipelineMetrics', pipelineName, refetchMs, keepMs],
    queryFn: () => PipelinesService.pipelineStats(pipelineName).then(status => ({ status })) as any,
    refetchInterval: refetchMs,
    initialData: {
      input: new Map<string, InputEndpointMetrics>(),
      output: new Map<string, OutputEndpointMetrics>(),
      global: [] as GlobalMetricsTimestamp[]
    } as any,
    select: undefined as unknown as (data: PipelineMetrics) => PipelineMetrics,
    structuralSharing: accumulatePipelineMetrics(refetchMs, keepMs)
  })
}

/**
 * Drop any errors with a given tag in cache with a set of the new errors with the same tag
 */
export const updateSystemErrorsCache = (queryClient: QueryClient, systemErrors: SystemError[]) => {
  for (const [tag, errors] of groupBy(systemErrors, error => error.cause.tag)) {
    setQueryData(queryClient, pipelineManagerAggregateQuery.systemErrors(), old => {
      if (!old) {
        return errors
      }
      const begin = old.findIndex(oldError => oldError.cause.tag === tag)
      const end = old.findLastIndex(oldError => oldError.cause.tag === tag)
      begin === -1 ? old.splice(0, 0, ...errors) : old.splice(begin, end - begin + 1, ...errors)
      return old
    })
  }
}
