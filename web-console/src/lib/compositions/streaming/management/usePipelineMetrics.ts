import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { nonNull } from '$lib/functions/common/function'
import {
  ConnectorStatus,
  GlobalMetrics,
  InputConnectorMetrics,
  OutputConnectorMetrics,
  PipelineStatus
} from '$lib/types/pipeline'
import { useEffect, useState } from 'react'

import { useQuery } from '@tanstack/react-query'

export function usePipelineMetrics(props: {
  pipelineName: string
  status: PipelineStatus
  refetchMs: number
  keepMs?: number
}) {
  const [metrics, setMetrics] = useState({
    global: [] as GlobalMetrics[],
    input: new Map<string, InputConnectorMetrics>(),
    output: new Map<string, OutputConnectorMetrics>()
  })
  const pipelineManagerQuery = usePipelineManagerQuery()
  const pipelineStatsQuery = useQuery({
    ...pipelineManagerQuery.pipelineStats(props.pipelineName),
    enabled: props.status == PipelineStatus.RUNNING,
    refetchInterval: props.refetchMs,
    refetchIntervalInBackground: true,
    refetchOnWindowFocus: false
  })

  useEffect(() => {
    if (!pipelineStatsQuery.isPending && !pipelineStatsQuery.isError) {
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
    pipelineStatsQuery.isPending,
    pipelineStatsQuery.isError,
    pipelineStatsQuery.data,
    props.status,
    props.keepMs,
    props.refetchMs
  ])

  return { ...metrics, periodMs: props.refetchMs }
}
