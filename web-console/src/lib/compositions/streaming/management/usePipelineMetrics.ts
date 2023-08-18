import { useEffect, useState } from 'react'
import { ConnectorStatus, GlobalMetrics, InputConnectorMetrics, OutputConnectorMetrics } from '$lib/types/pipeline'
import { useQuery } from '@tanstack/react-query'
import { PipelineManagerQuery } from 'src/lib/services/defaultQueryFn'
import { PipelineStatus } from 'src/lib/services/manager'
import { nonNull } from '$lib/functions/common/function'

export function usePipelineMetrics(props: {
  pipelineId: string
  status: PipelineStatus
  refetchMs: number
  keepMs?: number
}) {
  const [globalMetrics, setGlobalMetrics] = useState<GlobalMetrics[]>([])
  const [inputMetrics, setInputMetrics] = useState<Map<string, InputConnectorMetrics>>(new Map())
  const [outputMetrics, setOutputMetrics] = useState<Map<string, OutputConnectorMetrics>>(new Map())
  const pipelineStatsQuery = useQuery({
    ...PipelineManagerQuery.pipelineStats(props.pipelineId),
    enabled: props.status == PipelineStatus.RUNNING,
    refetchInterval: props.refetchMs
  })
  const keepElems = nonNull(props.keepMs) ? Math.ceil(props.keepMs / props.refetchMs) - 1 : globalMetrics.length

  useEffect(() => {
    if (!pipelineStatsQuery.isLoading && !pipelineStatsQuery.isError) {
      const metrics = pipelineStatsQuery.data['global_metrics']
      setGlobalMetrics(oldMetrics => [...oldMetrics.slice(-keepElems), metrics])

      const newInputMetrics = new Map<string, InputConnectorMetrics>()
      pipelineStatsQuery.data['inputs'].forEach((cs: ConnectorStatus) => {
        // @ts-ignore (config is untyped needs backend fix)
        newInputMetrics.set(cs.config['stream'], cs.metrics as InputConnectorMetrics)
      })
      setInputMetrics(newInputMetrics)

      const newOutputMetrics = new Map<string, OutputConnectorMetrics>()
      pipelineStatsQuery.data['outputs'].forEach((cs: ConnectorStatus) => {
        // @ts-ignore (config is untyped needs backend fix)
        newOutputMetrics.set(cs.config['stream'], cs.metrics as OutputConnectorMetrics)
      })
      setOutputMetrics(newOutputMetrics)
    }
    if (props.status == PipelineStatus.SHUTDOWN) {
      setGlobalMetrics([])
      setInputMetrics(new Map())
      setOutputMetrics(new Map())
    }
  }, [pipelineStatsQuery.isLoading, pipelineStatsQuery.isError, pipelineStatsQuery.data, props.status, keepElems])

  return { globalMetrics, inputMetrics, outputMetrics }
}
