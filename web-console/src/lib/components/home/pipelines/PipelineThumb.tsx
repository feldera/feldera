import { Pipeline } from '$lib/services/manager/models/Pipeline'
import { ApexOptions } from 'apexcharts'
import ReactApexcharts from 'src/@core/components/react-apexcharts'

import { Box, Button, Collapse, Link, Typography } from '@mui/material'
import { discreteDerivative } from 'ts-practical-fp'
import { usePipelineMetrics } from '$lib/compositions/streaming/management/usePipelineMetrics'
import { format } from 'numerable'
import { PipelineStatus } from 'src/lib/services/manager'
import { Icon } from '@iconify/react'
import { useState } from 'react'

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
  const item = {
    name: props.descriptor.name,
    description: props.descriptor.description,
    tput: throughput.at(-1) || 0,
    chartColor: 'secondary',
    active: props.state.current_status === PipelineStatus.RUNNING
  }
  const [sqlHover, setSqlHover] = useState(false)
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
            {item.description}
          </Typography>
        </Box>
        <Box sx={{ display: 'flex', position: 'relative', alignItems: 'center' }}>
          <ReactApexcharts type='line' width={80} height={40} options={props.apexOptions} series={series} />
        </Box>
      </Box>

      {item.active ? (
        <>
          <Button
            variant={sqlHover ? 'outlined' : 'text'}
            sx={{ textTransform: 'none', flex: 'none' }}
            size='small'
            disabled={!props.descriptor.program_id}
            href={`/analytics/editor/?program_id=${props.descriptor.program_id}`}
            target='_blank'
            rel='noreferrer'
            onMouseEnter={() => setSqlHover(true)}
            onMouseLeave={() => setSqlHover(false)}
          >
            <Collapse orientation='horizontal' in={!sqlHover}>
              <Box sx={{ display: 'flex', flexWrap: 'nowrap', alignItems: 'center' }}>
                <Typography sx={{ fontWeight: 500, color: 'text.disabled' }}>
                  {format(item.tput, '0.0\u00A0aOps/s', { zeroFormat: '0\u00A0Ops/s' }) || '0 '}
                </Typography>
                <Icon icon='bx:dots-vertical-rounded' fontSize={28} style={{ margin: -4, marginRight: -16 }}></Icon>
              </Box>
            </Collapse>
            <Collapse orientation='horizontal' in={sqlHover}>
              SQL
            </Collapse>
          </Button>
        </>
      ) : (
        <Button
          variant='outlined'
          size='small'
          href={`/analytics/editor/?program_id=${props.descriptor.program_id}`}
          target='_blank'
          rel='noreferrer'
        >
          SQL
        </Button>
      )}
    </Box>
  )
}
