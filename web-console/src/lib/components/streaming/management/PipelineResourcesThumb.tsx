import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'

import { Box } from '@mui/material'
import { useQuery } from '@tanstack/react-query'

const printRange = (min: number | null | undefined, max: number | null | undefined, suffix: string, none: string) =>
  min && max ? `${min} - ${max} ${suffix}` : min ? `min ${min} ${suffix}` : max ? `up to ${max} ${suffix}` : none

export const PipelineResourcesThumb = (props: { pipelineName: string }) => {
  const PipelineManagerQuery = usePipelineManagerQuery()
  const configQuery = useQuery({ ...PipelineManagerQuery.pipelineConfig(props.pipelineName) })
  if (!configQuery.data) {
    return <></>
  }
  const res = configQuery.data.resources
  return (
    <Box sx={{ display: 'flex', flexWrap: 'wrap', width: '100%' }}>
      <Box sx={{ whiteSpace: 'nowrap' }}>{configQuery.data.workers} workers |&nbsp;</Box>
      <Box sx={{ whiteSpace: 'nowrap' }}>
        CPU: {printRange(res?.cpu_cores_min, res?.cpu_cores_max, 'cores', 'default')} |&nbsp;
      </Box>
      <Box sx={{ whiteSpace: 'nowrap' }}>
        RAM: {printRange(res?.memory_mb_min, res?.memory_mb_max, 'MB', 'default')} |&nbsp;
      </Box>
      <Box sx={{ whiteSpace: 'nowrap' }}>
        Storage:{' '}
        {configQuery.data.storage
          ? printRange(
              undefined,
              res?.storage_mb_max ? res?.storage_mb_max / 1000 : undefined,
              'GB',
              'default capacity'
            )
          : 'disabled'}
      </Box>
    </Box>
  )
}
