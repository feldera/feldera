'use client'

import { usePipelineMetrics } from '$lib/compositions/streaming/management/usePipelineMetrics'
import { discreteDerivative } from '$lib/functions/common/math'
import { InputConnectorMetrics, OutputConnectorMetrics, Pipeline, PipelineStatus } from '$lib/types/pipeline'
import { ApexOptions } from 'apexcharts'
import { format } from 'numerable'
import { useState } from 'react'
import ReactApexcharts from 'src/@core/components/react-apexcharts'
import IconDotsVerticalRounded from '~icons/bx/dots-vertical-rounded'

import { Box, Button, Collapse, Link, Typography } from '@mui/material'
import { alpha, useTheme } from '@mui/material/styles'

export const PipelineThumb = (props: Pipeline & { apexOptions: ApexOptions }) => {
  const metrics = usePipelineMetrics({
    pipelineId: props.descriptor.pipeline_id,
    status: props.state.current_status,
    refetchMs: 3000,
    keepMs: 10000
  })

  const totalProcessed = discreteDerivative(metrics.global, m => m.total_processed_records).filter(x => x != 0)
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

  const errorsNumber = (aggregate => aggregate(metrics.input) + aggregate(metrics.output))(
    (map: Map<string, InputConnectorMetrics | OutputConnectorMetrics>) =>
      Array.from(map.values()).reduce(
        (acc, cur) =>
          acc +
          ('num_parse_errors' in cur
            ? cur.num_parse_errors + cur.num_transport_errors
            : cur.num_encode_errors + cur.num_transport_errors),
        0
      )
  )
  const theme = useTheme()

  return (
    <Box>
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
              <Link href={'/streaming/management/#' + props.descriptor.pipeline_id}>{item.name}</Link>
            </Typography>
            <Typography variant='body2' sx={{ color: 'text.disabled' }}>
              {item.description}
            </Typography>
          </Box>
          {item.active && (
            <Box sx={{ display: 'flex', position: 'relative', alignItems: 'center' }}>
              <ReactApexcharts type='line' width={80} height={40} options={props.apexOptions} series={series} />
            </Box>
          )}
        </Box>

        {item.active ? (
          <>
            <Button
              variant={sqlHover ? 'outlined' : 'text'}
              sx={{ textTransform: 'none', flex: 'none' }}
              disabled={!props.descriptor.program_id}
              size='small'
              href={`/analytics/editor/?program_id=${props.descriptor.program_id}`}
              onMouseEnter={() => setSqlHover(true)}
              onMouseLeave={() => setSqlHover(false)}
            >
              <Collapse orientation='horizontal' in={!sqlHover}>
                <Box sx={{ display: 'flex', flexWrap: 'nowrap', alignItems: 'center' }}>
                  <Typography sx={{ fontWeight: 500, whiteSpace: 'nowrap' }}>
                    {format(item.tput, '0.0 ar', { zeroFormat: '0 r' }) + 'ows/s'}
                  </Typography>
                  <IconDotsVerticalRounded fontSize={28} style={{ margin: -4, marginRight: -16 }} />
                </Box>
              </Collapse>
              <Collapse orientation='horizontal' in={sqlHover}>
                SQL
              </Collapse>
            </Button>
          </>
        ) : (
          <Button variant='outlined' size='small' href={`/analytics/editor/?program_id=${props.descriptor.program_id}`}>
            SQL
          </Button>
        )}
      </Box>
      {item.active &&
        (errorsNumber ? (
          <Box
            sx={{
              borderRadius: '0 0 6px 6px',
              px: 2,
              backgroundColor: alpha(theme.palette.warning.main, 0.5)
            }}
          >
            <Typography variant='subtitle2' component='span'>
              connector errors:{' '}
            </Typography>
            {format(errorsNumber, '0,0')}
          </Box>
        ) : (
          <Box
            sx={{
              minHeight: 8,
              borderRadius: '0 0 6px 6px',
              backgroundColor: alpha(theme.palette.success.main, 0.5)
            }}
          ></Box>
        ))}
    </Box>
  )
}
