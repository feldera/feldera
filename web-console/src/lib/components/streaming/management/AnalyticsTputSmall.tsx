// Shows a small throughput chart for use in overviews.
//
// Currently this is just a placeholder.

import Card from '@mui/material/Card'
import { ApexOptions } from 'apexcharts'
import ReactApexcharts from 'src/@core/components/react-apexcharts'

const series = [{ data: [30, 70, 35, 55, 45, 70] }]

const AnalyticsTputSmall = () => {
  const options: ApexOptions = {
    chart: {
      toolbar: { show: false }
    },
    xaxis: {
      labels: { show: false },
      axisTicks: { show: false },
      axisBorder: { show: false }
    },
    yaxis: { show: false }
  }

  return (
    <Card>
      <ReactApexcharts height='4' options={options} series={series} />
    </Card>
  )
}

export default AnalyticsTputSmall
