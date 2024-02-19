// Should display a list of active pipelines with stats, just a placeholder
// right now.

import { PipelineThumb } from '$lib/components/home/pipelines/PipelineThumb'
import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { partition } from '$lib/functions/common/array'
import { PipelineStatus } from '$lib/types/pipeline'
import IconChevronDown from '~icons/bx/chevron-down'

import { Accordion, AccordionDetails, AccordionSummary, Stack } from '@mui/material'
import Card from '@mui/material/Card'
import CardContent from '@mui/material/CardContent'
import CardHeader from '@mui/material/CardHeader'
import Typography from '@mui/material/Typography'
import { useQuery } from '@tanstack/react-query'

export const Pipelines = () => {
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
            <PipelineThumb {...p} key={p.descriptor.name}></PipelineThumb>
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
              <PipelineThumb {...p} key={p.descriptor.name}></PipelineThumb>
            ))}
          </Stack>
        </AccordionDetails>
      </Accordion>
    </Card>
  )
}
