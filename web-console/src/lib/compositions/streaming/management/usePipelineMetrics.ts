import { nonNull } from '$lib/functions/common/function'
import { PipelineStatus } from '$lib/services/manager'
import { PipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import { ConnectorStatus, GlobalMetrics, InputConnectorMetrics, OutputConnectorMetrics } from '$lib/types/pipeline'
import { useEffect, useState } from 'react'

import { useQuery } from '@tanstack/react-query'

export function usePipelineMetrics(props: {
  pipelineId: string
  status: PipelineStatus
  refetchMs: number
  keepMs?: number
}) {
  const [metrics, setMetrics] = useState({
    global: [] as GlobalMetrics[],
    input: new Map<string, InputConnectorMetrics>(),
    output: new Map<string, OutputConnectorMetrics>()
  })
  const pipelineStatsQuery = useQuery({
    ...PipelineManagerQuery.pipelineStats(props.pipelineId),
    enabled: props.status == PipelineStatus.RUNNING,
    refetchInterval: props.refetchMs
  })

  useEffect(() => {
    if (!pipelineStatsQuery.isLoading && !pipelineStatsQuery.isError) {
      const metrics = pipelineStatsQuery.data['global_metrics']

      const newInputMetrics = new Map<string, InputConnectorMetrics>()
      pipelineStatsQuery.data['inputs'].forEach((cs: ConnectorStatus) => {
        // @ts-ignore (config is untyped needs backend fix)
        newInputMetrics.set(cs.config['stream'], cs.metrics as InputConnectorMetrics)
      })

      const newOutputMetrics = new Map<string, OutputConnectorMetrics>()
      pipelineStatsQuery.data['outputs'].forEach((cs: ConnectorStatus) => {
        // @ts-ignore (config is untyped needs backend fix)
        newOutputMetrics.set(cs.config['stream'], cs.metrics as OutputConnectorMetrics)
      })

      setMetrics(old => {
        const keepElems = nonNull(props.keepMs) ? Math.ceil(props.keepMs / props.refetchMs) - 1 : old.global.length
        return {
          global: [...old.global.slice(-keepElems), metrics],
          input: newInputMetrics,
          output: newOutputMetrics
        }
      })
    }
    if (props.status == PipelineStatus.SHUTDOWN) {
      setMetrics({
        global: [],
        input: new Map(),
        output: new Map()
      })
    }
  }, [
    pipelineStatsQuery.isLoading,
    pipelineStatsQuery.isError,
    pipelineStatsQuery.data,
    props.status,
    props.keepMs,
    props.refetchMs
  ])

  return metrics
}
