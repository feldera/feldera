'use client'

import { calcPipelineThroughput, usePipelineMetrics } from '$lib/compositions/streaming/management/usePipelineMetrics'
import { InputEndpointMetrics, OutputEndpointMetrics, Pipeline, PipelineStatus } from '$lib/types/pipeline'
import { format } from 'd3-format'
import { useState } from 'react'

import ReactApexcharts from '@core/components/react-apexcharts'
import { hexToRGBA } from '@core/utils/hex-to-rgba'
import { Box, Button, Collapse, Link, Typography } from '@mui/material'
import { alpha, useTheme } from '@mui/material/styles'

const keepMetricsMs = 30000

export const PipelineThumb = (props: Pipeline) => {
  const metrics = usePipelineMetrics({
    pipelineName: props.descriptor.name,
    status: props.state.current_status,
    refetchMs: 2000,
    keepMs: keepMetricsMs
  })

  const { throughput, yMin, yMax } = calcPipelineThroughput(metrics)

  const series = [
    {
      data: throughput
    }
  ]
  const item = {
    name: props.descriptor.name,
    description: props.descriptor.description,
    tput: throughput.at(-1) || [0, 0],
    chartColor: 'secondary',
    active: props.state.current_status === PipelineStatus.RUNNING
  }
  const [sqlHover, setSqlHover] = useState(false)

  const errorsNumber = (aggregate => aggregate(metrics.input) + aggregate(metrics.output))(
    (map: Map<string, InputEndpointMetrics | OutputEndpointMetrics>) =>
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
  const apexOptions = {
    chart: {
      parentHeightOffset: 0,
      toolbar: { show: false },
      animations: {
        enabled: false
      }
    },
    grid: {
      show: false,
      padding: {
        top: -30,
        left: -10,
        bottom: -30
      }
    },
    tooltip: { enabled: false },
    colors: [hexToRGBA(theme.palette.primary.main, 1)],
    markers: {
      size: 6,
      offsetX: -2,
      offsetY: -1,
      strokeWidth: 5,
      strokeOpacity: 1,
      colors: ['transparent'],
      strokeColors: 'transparent'
    },
    stroke: {
      width: 3,
      curve: 'smooth',
      lineCap: 'round'
    },
    xaxis: {
      type: 'numeric',
      labels: { show: false },
      axisTicks: { show: false },
      axisBorder: { show: false },
      range: keepMetricsMs
    },
    yaxis: {
      labels: { show: false },
      min: yMin,
      max: yMax
    }
  }

  return (
    <Box sx={item.active ? { border: 1, borderColor: alpha(theme.palette.grey[500], 0.5), borderRadius: 1.5 } : {}}>
      <Box sx={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', p: 1 }}>
        <Box sx={{ display: 'flex' }}>
          {item.active && (
            <Box sx={{ display: 'flex', position: 'relative', alignItems: 'center' }}>
              <ReactApexcharts type='line' width={80} height={40} options={apexOptions} series={series} />
            </Box>
          )}
          <Box
            sx={{
              mr: 5,
              flexGrow: 1,
              display: 'flex',
              flexWrap: 'wrap',
              alignItems: 'center'
            }}
          >
            <Box sx={{ mr: 2, display: 'flex', flexDirection: 'column' }}>
              <Typography sx={{ mb: 0.5, fontWeight: 500 }}>
                <Link href={'/streaming/management/#' + props.descriptor.name}>{item.name}</Link>
              </Typography>
              <Typography variant='body2' sx={{ wordBreak: 'break-word', color: 'text.disabled' }}>
                {item.description}
              </Typography>
            </Box>
          </Box>
        </Box>
        <Box sx={{ ml: 'auto' }}>
          {item.active ? (
            <>
              <Button
                variant={sqlHover ? 'outlined' : 'text'}
                sx={{ textTransform: 'none', flex: 'none', minWidth: '9rem', width: 'auto' }}
                disabled={!props.descriptor.program_name}
                size='small'
                href={`/analytics/editor/?program_name=${props.descriptor.program_name}`}
                onMouseEnter={() => setSqlHover(true)}
                onMouseLeave={() => setSqlHover(false)}
              >
                <Collapse orientation='horizontal' in={!sqlHover}>
                  <Box sx={{ display: 'flex', flexWrap: 'nowrap', alignItems: 'center' }}>
                    <Typography sx={{ fontWeight: 500, whiteSpace: 'nowrap' }}>
                      {format(item.tput[1] >= 1000 ? '.3s' : '.0f')(item.tput[1]) + ' rows/s'}
                    </Typography>
                    <i className={`bx bx-dots-vertical-rounded`} style={{ fontSize: 32 }} />
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
              href={`/analytics/editor/?program_name=${props.descriptor.program_name}`}
            >
              SQL
            </Button>
          )}
        </Box>
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
            <Typography color={'gray'} component={'span'}>
              connector errors:{' '}
            </Typography>
            <Typography component={'span'}>{format(',.0f')(errorsNumber)}</Typography>
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
