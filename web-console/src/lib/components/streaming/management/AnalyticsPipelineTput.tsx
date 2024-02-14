// Shows the throughput of the pipeline in a graph.
//
// Also smooths the throughput over a few seconds.

import { discreteDerivative } from '$lib/functions/common/math'
import { GlobalMetrics } from '$lib/types/pipeline'
import { ApexOptions } from 'apexcharts'
import { format } from 'd3-format'
import ReactApexcharts from 'src/@core/components/react-apexcharts'

import Card from '@mui/material/Card'
import CardContent from '@mui/material/CardContent'
import { useTheme } from '@mui/material/styles'
import Typography from '@mui/material/Typography'

export const AnalyticsPipelineTput = (metrics: { global: GlobalMetrics[]; periodMs: number }) => {
  const theme = useTheme()

  const perSecond = metrics.periodMs / 1000
  const totalProcessed = discreteDerivative(metrics.global, m => m.total_processed_records).filter(x => x != 0)
  const throughput = discreteDerivative(totalProcessed, (n1, n0) => n1 - n0)
  const smoothTput = discreteDerivative(throughput, (n1, n0) => (n1 * 0.6) / perSecond + (n0 * 0.4) / perSecond)

  const series = [
    {
      data: throughput
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
          strokeColor: theme.palette.success.main,
          dataPointIndex: series[0].data.length - 1
        }
      ]
    }
  }

  return (
    <Card>
      <CardContent>
        <Typography sx={{ fontWeight: 600, color: 'text.secondary' }}>Throughput</Typography>
        <Typography variant='h5'>{format('.1s')(smoothTput[smoothTput.length - 1] || 0)} rows/sec</Typography>
      </CardContent>
      <ReactApexcharts type='area' height={110} width='100%' options={options} series={series} />
    </Card>
  )
}
