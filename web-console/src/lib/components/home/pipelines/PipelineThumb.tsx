import { Pipeline } from '$lib/services/manager/models/Pipeline'
import { ApexOptions } from 'apexcharts'
import ReactApexcharts from 'src/@core/components/react-apexcharts'
import { ThemeColor } from 'src/@core/layouts/types'

import { Box, Link, Typography } from '@mui/material'
import { discreteDerivative } from 'ts-practical-fp'
import { usePipelineMetrics } from '$lib/compositions/streaming/management/usePipelineMetrics'
import { format } from 'numerable'

interface DataType {
  name: string
  designation: string
  tput: number
  chartColor?: ThemeColor
}

export const PipelineThumb = (props: Pipeline & { apexOptions: ApexOptions }) => {
  const { globalMetrics } = usePipelineMetrics({
    pipelineId: props.descriptor.pipeline_id,
    status: props.state.current_status,
    refetchMs: 1000,
    keepMs: 10000
  })

  const totalProcessed = discreteDerivative(globalMetrics, m => m.total_processed_records).filter(x => x != 0)
  const throughput = discreteDerivative(totalProcessed, (n1, n0) => n1 - n0)

  const series = [
    {
      data: throughput
    }
  ]
  const item: DataType = {
    name: props.descriptor.name,
    designation: props.descriptor.description,
    tput: throughput.at(-1) || 0,
    chartColor: 'secondary'
  }
  return (
    <Box sx={{ flexGrow: 1, display: 'flex', alignItems: 'center' }}>
      <Box
        sx={{
          mr: 5,
          flexGrow: 1,
          display: 'flex',
          flexWrap: 'wrap',
          alignItems: 'center',
          justifyContent: 'space-between'
        }}
      >
        <Box sx={{ mr: 2, display: 'flex', flexDirection: 'column' }}>
          <Typography sx={{ mb: 0.5, fontWeight: 500 }}>
            <Link href={'/streaming/management/#' + props.descriptor.pipeline_id} target='_blank' rel='noreferrer'>
              {item.name}
            </Link>
          </Typography>
          <Typography variant='body2' sx={{ color: 'text.disabled' }}>
            {item.designation}
          </Typography>
        </Box>
        <Typography sx={{ fontWeight: 500, color: 'text.disabled' }}>
          {format(item.tput, '0.0\u00A0aOps/s', { zeroFormat: '0\u00A0Ops/s' }) || '0 '}
        </Typography>
      </Box>
      <Box sx={{ display: 'flex', position: 'relative', alignItems: 'center' }}>
        <ReactApexcharts type='line' width={80} height={40} options={props.apexOptions} series={series} />
      </Box>
    </Box>
  )
}
