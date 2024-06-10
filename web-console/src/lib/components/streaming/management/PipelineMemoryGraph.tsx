import { humanSize } from '$lib/functions/common/string'
import { tuple } from '$lib/functions/common/tuple'
import { GlobalMetrics } from '$lib/types/pipeline'
import { ApexOptions } from 'apexcharts'

import ReactApexcharts from '@core/components/react-apexcharts'
import { Box } from '@mui/material'
import Card from '@mui/material/Card'
import { useTheme } from '@mui/material/styles'
import Typography from '@mui/material/Typography'

export const PipelineMemoryGraph = (props: {
  metrics: { global: (GlobalMetrics & { timeMs: number })[] }
  keepMs: number
}) => {
  const theme = useTheme()

  const memUsed = props.metrics.global.map(m => tuple(m.timeMs, m.rss_bytes ?? 0))
  const smoothMemUsed = ((n1, n0) => n1[1] * 0.5 + n0[1] * 0.5)(
    memUsed.at(-2) ?? tuple(0, 0),
    memUsed.at(-1) ?? tuple(0, 0)
  )

  const series = [
    {
      data: memUsed
    }
  ]

  const valueMax = memUsed.length ? Math.max(...memUsed.map(v => v[1])) : 0
  const yMaxStep = Math.pow(2, Math.ceil(Math.log2(valueMax * 1.25)))
  const yMax = valueMax !== 0 ? yMaxStep : 1024 * 2048
  const yMin = 0
  const options: ApexOptions = {
    chart: {
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
              color: theme.palette.primary.main
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
        color: theme.palette.primary.main
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
          const offset = memUsed.at(-1)?.[0]
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
      tickAmount: 1,
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
        style: {
          colors: theme.palette.text.primary
        },
        formatter(val) {
          return humanSize(val)
        }
      }
    }
  }

  return (
    <Card sx={{ height: '100%' }}>
      <Box sx={{ px: '1rem', pt: '0.5rem' }}>
        <Typography sx={{ fontWeight: 600, color: 'text.secondary' }}>Memory used</Typography>
        <Typography variant='h5' data-testid='box-pipeline-memory-value'>
          {humanSize(smoothMemUsed)}
        </Typography>
      </Box>
      <ReactApexcharts
        type='area'
        height={140}
        width='100%'
        options={options}
        series={series}
        data-testid='box-pipeline-memory-graph'
      />
    </Card>
  )
}
