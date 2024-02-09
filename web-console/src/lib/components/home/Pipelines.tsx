// Should display a list of active pipelines with stats, just a placeholder
// right now.

import { PipelineThumb } from '$lib/components/home/pipelines/PipelineThumb'
import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { partition } from '$lib/functions/common/array'
import { PipelineStatus } from '$lib/types/pipeline'
import { ApexOptions } from 'apexcharts'
import { hexToRGBA } from 'src/@core/utils/hex-to-rgba'
import IconChevronDown from '~icons/bx/chevron-down'

import { Accordion, AccordionDetails, AccordionSummary, Stack } from '@mui/material'
import Card from '@mui/material/Card'
import CardContent from '@mui/material/CardContent'
import CardHeader from '@mui/material/CardHeader'
import { Theme, useTheme } from '@mui/material/styles'
import Typography from '@mui/material/Typography'
import { useQuery } from '@tanstack/react-query'

const Pipelines = () => {
  const theme = useTheme()
  const apexOptions = apexChartsOptions(theme)
  const pipelineManagerQuery = usePipelineManagerQuery()

  const fetchQuery = useQuery({
    ...pipelineManagerQuery.pipelines(),
    refetchInterval: 2000
  })
  const [active, inactive] = partition(fetchQuery.data ?? [], p => p.state.current_status === PipelineStatus.RUNNING)

  return (
    <Card>
      <CardHeader title={active.length ? 'Active Pipelines' : 'No Active Pipelines'} />
      <CardContent>
        <Stack spacing={2}>
          {active.map(p => (
            <PipelineThumb {...p} apexOptions={apexOptions} key={p.descriptor.name}></PipelineThumb>
          ))}
        </Stack>
      </CardContent>
      <Accordion disableGutters>
        <AccordionSummary expandIcon={<IconChevronDown fontSize={24} />} data-testid='button-expand-inactive-pipelines'>
          <Typography color='text.secondary'>and {inactive.length} inactive</Typography>
        </AccordionSummary>
        <AccordionDetails>
          <Stack spacing={2}>
            {inactive.map(p => (
              <PipelineThumb {...p} apexOptions={apexOptions} key={p.descriptor.name}></PipelineThumb>
            ))}
          </Stack>
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
