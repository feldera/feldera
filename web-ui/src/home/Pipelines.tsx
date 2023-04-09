// ** MUI Imports
import Box from '@mui/material/Box'
import Card from '@mui/material/Card'
import Typography from '@mui/material/Typography'
import CardHeader from '@mui/material/CardHeader'
import CardContent from '@mui/material/CardContent'
import { hexToRGBA } from 'src/@core/utils/hex-to-rgba'

// ** Types
import { ThemeColor } from 'src/@core/layouts/types'

import { useTheme } from '@mui/material/styles'

import { ApexOptions } from 'apexcharts'

import ReactApexcharts from 'src/@core/components/react-apexcharts'

const series = [
  [{ data: [19, 58, 45, 11] }],
  [{ data: [12, 58, 45, 13] }],
  [{ data: [30, 40, 50, 55] }],
  [{ data: [1, 2, 2, 2] }]
]

interface DataType {
  name: string
  totalTput: number
  chartValue: number
  designation: string
  chartColor?: ThemeColor
}

const data: DataType[] = [
  {
    chartValue: 85,
    name: 'Tech Averages',
    totalTput: 138,
    chartColor: 'secondary',
    designation: 'Calculate 30 day rolling...'
  },
  {
    name: 'S&P 500 Pairs',
    chartValue: 70,
    totalTput: 149,
    chartColor: 'warning',
    designation: 'Finds pairs in S&P500 stocks.'
  },
  {
    name: 'NYSE <> CME Arbitrage',
    chartValue: 25,
    totalTput: 160,
    designation: 'Exploit price differences...'
  },
  {
    name: 'Mean Reversion',
    chartValue: 75,
    totalTput: 166,
    chartColor: 'error',
    designation: 'Find tickers outside...'
  }
]

const Pipelines = () => {
  const theme = useTheme()

  const options: ApexOptions = {
    chart: {
      parentHeightOffset: 0,
      toolbar: { show: false },
      dropShadow: {
        top: 8,
        blur: 3,
        left: 3,
        enabled: true,
        opacity: 0.14,
        color: theme.palette.primary.main
      }
    },
    grid: {
      show: false,
      padding: {
        top: -21,
        left: -5,
        bottom: -8
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
      width: 5,
      curve: 'smooth',
      lineCap: 'round'
    },
    xaxis: {
      labels: { show: false },
      axisTicks: { show: false },
      axisBorder: { show: false }
    },
    yaxis: {
      labels: { show: false }
    }
  }

  return (
    <Card>
      <CardHeader title='Active Pipelines' />
      <CardContent>
        {data.map((item: DataType, index: number) => {
          return (
            <Box
              key={index}
              sx={{
                display: 'flex',
                alignItems: 'center',
                mb: index !== data.length - 1 ? 6 : undefined
              }}
            >
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
                    <Typography sx={{ mb: 0.5, fontWeight: 500 }}>{item.name}</Typography>
                    <Typography variant='body2' sx={{ color: 'text.disabled' }}>
                      {item.designation}
                    </Typography>
                  </Box>
                  <Typography sx={{ fontWeight: 500 }}>
                    <Typography component='span' sx={{ ml: 1.5, color: 'text.disabled' }}>
                      {`${item.totalTput} kOp/s`}
                    </Typography>
                  </Typography>
                </Box>
                <Box sx={{ display: 'flex', position: 'relative', alignItems: 'center' }}>
                  <ReactApexcharts type='line' width={80} height={40} options={options} series={series[index]} />
                </Box>
              </Box>
            </Box>
          )
        })}
      </CardContent>
    </Card>
  )
}

export default Pipelines
