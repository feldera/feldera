// Should display a list of active pipelines with stats, just a placeholder
// right now.

import { ApexOptions } from 'apexcharts'
import { hexToRGBA } from 'src/@core/utils/hex-to-rgba'

import Card from '@mui/material/Card'
import CardContent from '@mui/material/CardContent'
import CardHeader from '@mui/material/CardHeader'
import { Theme, useTheme } from '@mui/material/styles'
import Typography from '@mui/material/Typography'
import { useQuery } from '@tanstack/react-query'
import { PipelineManagerQuery } from '$lib/services/defaultQueryFn'
import { PipelineThumb } from '$lib/components/home/pipelines/PipelineThumb'
import { PipelineStatus } from 'src/lib/services/manager'
import { partition } from '$lib/functions/common/array'
import { Accordion, AccordionDetails, AccordionSummary, ListItem } from '@mui/material'
import { Icon } from '@iconify/react'

const Pipelines = () => {
  const theme = useTheme()
  const apexOptions = apexChartsOptions(theme)

  const fetchQuery = useQuery({
    ...PipelineManagerQuery.pipeline(),
    refetchInterval: 1000
  })
  const [active, inactive] = partition(fetchQuery.data ?? [], p => p.state.current_status === PipelineStatus.RUNNING)
  const thumbs = active.map(p => (
    <PipelineThumb {...p} apexOptions={apexOptions} key={p.descriptor.pipeline_id}></PipelineThumb>
  ))

  return (
    <Card>
      <CardHeader title={thumbs.length ? 'Active Pipelines' : 'No Active Pipelines'} />
      <CardContent>
        {thumbs}
        <ListItem></ListItem>
      </CardContent>
      <Accordion disableGutters>
        <AccordionSummary expandIcon={<Icon icon='bx:chevron-down' fontSize={32} />}>
          <Typography color='text.secondary'>and {inactive.length} inactive</Typography>
        </AccordionSummary>
        <AccordionDetails>
          {inactive.map(p => (
            <PipelineThumb {...p} apexOptions={apexOptions} key={p.descriptor.pipeline_id}></PipelineThumb>
          ))}
        </AccordionDetails>
      </Accordion>
    </Card>
  )
}

export default Pipelines

const apexChartsOptions = (theme: Theme): ApexOptions => ({
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
})
