// Shows the throughput of the pipeline in a graph.
//
// Also smooths the throughput over a few seconds.

import { discreteDerivative } from '$lib/functions/common/math'
import { tuple } from '$lib/functions/common/tuple'
import { GlobalMetrics } from '$lib/types/pipeline'
import { ApexOptions } from 'apexcharts'
import { format } from 'd3-format'
import ReactApexcharts from 'src/@core/components/react-apexcharts'

import { Box } from '@mui/material'
import Card from '@mui/material/Card'
import { useTheme } from '@mui/material/styles'
import Typography from '@mui/material/Typography'

export const AnalyticsPipelineTput = (props: {
  metrics: { global: (GlobalMetrics & { timeMs: number })[] }
  keepMs: number
}) => {
  const theme = useTheme()

  const totalProcessed = props.metrics.global.map(m => tuple(m.timeMs, m.total_processed_records))
  const throughput = discreteDerivative(totalProcessed, (n1, n0) =>
    tuple(n1[0], ((n1[1] - n0[1]) * 1000) / (n1[0] - n0[0]))
  )
  const smoothTput = ((n1, n0) => n1[1] * 0.5 + n0[1] * 0.5)(
    throughput.at(-2) ?? throughput.at(-1) ?? tuple(0, 0),
    throughput.at(-1) ?? tuple(0, 0)
  )
  const series = [
    {
      data: throughput
    }
  ]

  const valueMax = throughput.length ? Math.max(...throughput.map(v => v[1])) : 0
  const yMaxStep = Math.pow(10, Math.ceil(Math.log10(valueMax))) / 5
  const yMax = valueMax !== 0 ? Math.ceil((valueMax * 1.25) / yMaxStep) * yMaxStep : 100
  const yMin = 0
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
      }
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
      labels: { show: true, formatter: v => (n => (n < 0 ? '' : Math.round(n / 1000).toString()))(Number(v)) },
      tickAmount: 6,
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
        }
      }
    }
  }

  return (
    <Card>
      <Box sx={{ px: '1rem', pt: '0.5rem' }}>
        <Typography sx={{ fontWeight: 600, color: 'text.secondary' }}>Throughput</Typography>
        <Typography variant='h5'>{format(smoothTput >= 1000 ? '.3s' : '.0f')(smoothTput)} rows/sec</Typography>
      </Box>
      <ReactApexcharts type='area' height={140} width='100%' options={options} series={series} />
    </Card>
  )
}
