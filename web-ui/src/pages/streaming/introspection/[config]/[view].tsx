// See the status of a input table or output view.
//
// Note: This is still a work in progress and currently does not work as well as
// it should or is not very flexible in displaying what a user wants.
import Grid from '@mui/material/Grid'
import Typography from '@mui/material/Typography'
import { useQuery } from '@tanstack/react-query'
import { useRouter } from 'next/router'
import { useEffect, useState } from 'react'
import PageHeader from 'src/layouts/components/page-header'
import { Pipeline, PipelineId } from 'src/types/manager'
import { IntrospectionTable } from 'src/streaming/introspection/IntrospectionTable'

const IntrospectInputOutput = () => {
  const [pipelineId, setPipelineId] = useState<PipelineId | undefined>(undefined)
  const [tableOrView, setTableOrView] = useState<string | undefined>(undefined)
  const router = useRouter()
  const { config, view } = router.query

  useEffect(() => {
    if (typeof config === 'string') {
      setPipelineId(config)
    }
    if (typeof view === 'string') {
      setTableOrView(view)
    }
  }, [pipelineId, setPipelineId, config, view, setTableOrView])
  const [pipeline, setPipeline] = useState<Pipeline | undefined>(undefined)

  const configQuery = useQuery<Pipeline>(['pipelineStatus', { pipeline_id: pipelineId }], {
    enabled: pipelineId !== undefined
  })
  useEffect(() => {
    if (!configQuery.isLoading && !configQuery.isError) {
      setPipeline(configQuery.data)
    }
  }, [configQuery.isLoading, configQuery.isError, configQuery.data, setPipeline])

  return (
    !configQuery.isLoading &&
    !configQuery.isError &&
    pipeline &&
    tableOrView && (
      <Grid container spacing={6} className='match-height'>
        <PageHeader
          title={
            <Typography variant='h5'>
              {pipeline?.descriptor.name} / {tableOrView}
            </Typography>
          }
          subtitle={<Typography variant='body2'>Introspection</Typography>}
        />

        <Grid item xs={12}>
          <IntrospectionTable pipeline={pipeline} name={tableOrView} />
        </Grid>
      </Grid>
    )
  )
}

export default IntrospectInputOutput
