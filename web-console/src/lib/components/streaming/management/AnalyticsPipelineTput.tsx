// Shows the throughput of the pipeline in a graph.
//
// Also smooths the throughput over a few seconds.

import { calcPipelineThroughput } from '$lib/compositions/streaming/management/usePipelineMetrics'
import { GlobalMetrics } from '$lib/types/pipeline'
import { ApexOptions } from 'apexcharts'
import { format } from 'd3-format'

import ReactApexcharts from '@core/components/react-apexcharts'
import { Box } from '@mui/material'
import Card from '@mui/material/Card'
import { useTheme } from '@mui/material/styles'
import Typography from '@mui/material/Typography'

export const AnalyticsPipelineTput = (props: {
  metrics: { global: (GlobalMetrics & { timeMs: number })[] }
  keepMs: number
}) => {
  const theme = useTheme()

  const { throughput, smoothTput, yMin, yMax } = calcPipelineThroughput(props.metrics)
  const series = [
    {
      data: throughput
    }
  ]

  const options: ApexOptions = {
    chart: {
      id: 'chart',
      parentHeightOffset: 0,
      toolbar: { show: false },
      animations: {
        enabled: false
      }
    },
    tooltip: { enabled: false },
    dataLabels: { enabled: false },
    stroke: {
      width: 3,
      curve: 'smooth',
      lineCap: 'round'
    },
    grid: {
      show: true,
      padding: {
        left: 20,
        right: 20
      },
      borderColor: theme.palette.text.disabled
    },
    fill: {
      type: 'gradient',
      gradient: {
        opacityTo: 0.7,
        opacityFrom: 0.5,
        shadeIntensity: 1,
        stops: [0, 90, 100],
        colorStops: [
          [
            {
              offset: 0,
              opacity: 0.6,
              color: theme.palette.success.main
            },
            {
              offset: 100,
              opacity: 0.1,
              color: theme.palette.background.paper
            }
          ]
        ]
      }
    },
    theme: {
      monochrome: {
        enabled: true,
        shadeTo: 'light',
        shadeIntensity: 1,
        color: theme.palette.success.main
      }
    },
    xaxis: {
      type: 'numeric',
      labels: {
        show: true,
        formatter: v => {
          const n = Number(v)
          if (n < 0) {
            return ''
          }
          const offset = throughput.at(-1)?.[0]
          if (!offset) {
            return ''
          }
          const ms = offset - n
          if (ms === 0) {
            return '00:00'
          }
          const time = new Date(0)
          time.setMilliseconds(ms)
          return '-' + time.toISOString().substring(14, 19)
        },
        style: {
          colors: theme.palette.text.primary
        }
      },
      tickAmount: 3,
      axisTicks: { show: true },
      axisBorder: { show: false },
      range: props.keepMs
    },
    yaxis: {
      show: true,
      min: yMin,
      max: yMax,
      axisBorder: { show: false },
      tickAmount: 2,
      labels: {
        show: true,
        formatter(val) {
          return format(val >= 1000 ? '.3s' : '.0f')(val)
        },
        style: {
          colors: theme.palette.text.primary
        }
      }
    }
  }

  return (
    <Card sx={{ height: '100%' }}>
      <Box sx={{ px: '1rem', pt: '0.5rem', width: '100%' }} data-testid='box-pipeline-throughput-value'>
        <Typography sx={{ fontWeight: 600, color: 'text.secondary' }}>Throughput</Typography>
        <Typography variant='h5'>{format(smoothTput >= 1000 ? '.3s' : '.0f')(smoothTput)} rows/s</Typography>
      </Box>
      <ReactApexcharts
        type='area'
        height={140}
        width='100%'
        options={options}
        series={series}
        data-testid='box-pipeline-throughput-graph'
      />
    </Card>
  )
}
