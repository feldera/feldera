import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { nonNull } from '$lib/functions/common/function'
import { tuple } from '$lib/functions/common/tuple'
import { ControllerStatus, InputEndpointMetrics, OutputEndpointMetrics, PipelineStatus } from '$lib/types/pipeline'
import { useState } from 'react'

import { useQuery } from '@tanstack/react-query'

const toMetrics = (data: ControllerStatus, { timeMs }: { timeMs: number }) => ({
  input: new Map(data.inputs.map(cs => tuple(cs.config.stream, cs.metrics))),
  output: new Map(data.outputs.map(cs => tuple(cs.config.stream, cs.metrics))),
  global: [{ ...data.global_metrics, timeMs }]
})

const emptyData = {
  input: new Map<string, InputEndpointMetrics>(),
  output: new Map<string, OutputEndpointMetrics>(),
  global: []
}

export function usePipelineMetrics(props: {
  pipelineName: string
  status: PipelineStatus
  refetchMs: number
  keepMs?: number
}) {
  const pipelineManagerQuery = usePipelineManagerQuery()
  const [lastTimestamp, setLastTimestamp] = useState<number>()
  const pipelineStatsQuery = useQuery({
    ...pipelineManagerQuery.pipelineStats(props.pipelineName),
    enabled: props.status === PipelineStatus.RUNNING,
    refetchInterval: props.refetchMs,
    refetchIntervalInBackground: true,
    refetchOnWindowFocus: false,
    structuralSharing: (oldData: any, _newData: any) => {
      if (!_newData) {
        setLastTimestamp(undefined)
        return emptyData
      }
      const now = Date.now()
      if (!lastTimestamp) {
        setLastTimestamp(now)
      }
      const newData = toMetrics(_newData, { timeMs: lastTimestamp ? now - lastTimestamp : 0 })
      const oldGlobal = oldData?.global ?? []

      const keepElems = nonNull(props.keepMs) ? Math.ceil(props.keepMs / props.refetchMs) : oldGlobal.length
      return {
        input: newData.input,
        output: newData.output,
        // global includes one more element than needed to satisfy keepMs
        global: [...oldGlobal.slice(-keepElems), newData.global[0]]
      } as any
    }
    // select: x => x as unknown as ReturnType<typeof toMetrics>
  })

  if (!pipelineStatsQuery.data) {
    return emptyData
  }
  return pipelineStatsQuery.data as unknown as ReturnType<typeof toMetrics>
}
