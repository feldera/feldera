import { discreteDerivative } from '$lib/functions/common/math'
import { humanSize } from '$lib/functions/common/string'
import { GlobalMetrics } from '$lib/types/pipeline'
import { ApexOptions } from 'apexcharts'
import ReactApexcharts from 'src/@core/components/react-apexcharts'

import Card from '@mui/material/Card'
import CardContent from '@mui/material/CardContent'
import { useTheme } from '@mui/material/styles'
import Typography from '@mui/material/Typography'

export const PipelineMemoryGraph = (metrics: { global: GlobalMetrics[]; periodMs: number }) => {
  const theme = useTheme()

  const perSecond = metrics.periodMs / 1000
  const memUsed = metrics.global.map(m => m.rss_bytes ?? 0)
  const smoothMemUsed = discreteDerivative(memUsed, (n1, n0) => (n1 * 0.6) / perSecond + (n0 * 0.4) / perSecond)

  const series = [
    {
      data: memUsed
    }
  ]

  const options: ApexOptions = {
    chart: {
      parentHeightOffset: 0,
      toolbar: { show: false }
    },
    tooltip: { enabled: false },
    dataLabels: { enabled: false },
    stroke: {
      width: 3,
      curve: 'smooth',
      lineCap: 'round'
    },
    grid: {
      show: false,
      padding: {
        left: 0,
        top: -25,
        right: 17
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
      labels: { show: false },
      axisTicks: { show: false },
      axisBorder: { show: false }
    },
    yaxis: { show: false },
    markers: {
      size: 1,
      offsetY: 2,
      offsetX: -4,
      strokeWidth: 4,
      strokeOpacity: 1,
      colors: ['transparent'],
      strokeColors: 'transparent',
      discrete: [
        {
          size: 6,
          seriesIndex: 0,
          fillColor: theme.palette.common.white,
          strokeColor: theme.palette.primary.main,
          dataPointIndex: series[0].data.length - 1
        }
      ]
    }
  }

  return (
    <Card>
      <CardContent>
        <Typography sx={{ fontWeight: 600, color: 'text.secondary' }}>Memory used</Typography>
        <Typography variant='h5'>{humanSize(smoothMemUsed[smoothMemUsed.length - 1] ?? 0)}</Typography>
      </CardContent>
      <ReactApexcharts
        type='area'
        height={110}
        width='100%'
        options={options}
        series={series}
        data-testid='box-pipeline-memory-graph'
      />
    </Card>
  )
}
