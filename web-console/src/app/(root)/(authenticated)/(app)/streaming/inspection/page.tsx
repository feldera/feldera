// Browse tables and views & insert data into tables.
'use client'

import { BreadcrumbsHeader } from '$lib/components/common/BreadcrumbsHeader'
import { ErrorOverlay } from '$lib/components/common/table/ErrorOverlay'
import { InsertionTable } from '$lib/components/streaming/import/InsertionTable'
import { InspectionTable } from '$lib/components/streaming/inspection/InspectionTable'
import { PipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import { Pipeline, PipelineStatus } from '$lib/types/pipeline'
import { useSearchParams } from 'next/navigation'
import { useEffect, useState } from 'react'
import { ErrorBoundary } from 'react-error-boundary'
import { useHashPart } from 'src/lib/compositions/useHashPart'

import TabContext from '@mui/lab/TabContext'
import TabList from '@mui/lab/TabList'
import TabPanel from '@mui/lab/TabPanel'
import { Alert, AlertTitle, Autocomplete, Box, FormControl, Link, MenuItem, TextField } from '@mui/material'
import Grid from '@mui/material/Grid'
import Tab from '@mui/material/Tab'
import { useQuery } from '@tanstack/react-query'

import type { Row } from '$lib/components/streaming/import/InsertionTable'
const TablesBreadcrumb = (props: { pipeline: Pipeline; relation: string; tables: string[]; views: string[] }) => {
  const [tab] = useHashPart()
  return (
    <Box sx={{ mb: '-1rem' }}>
      <FormControl sx={{ mt: '-1rem' }}>
        <Autocomplete
          size='small'
          options={props.tables
            .map(name => ({ type: 'Tables', name }))
            .concat(props.views.map(name => ({ type: 'Views', name })))}
          groupBy={option => option.type}
          getOptionLabel={o => o.name}
          sx={{ width: 400 }}
          renderInput={params => <TextField {...params} value={props.relation} label='Tables and Views' />}
          value={{ name: props.relation, type: '' }}
          renderOption={(_props, item) => (
            <MenuItem
              key={item.name}
              value={item.name}
              {...{
                component: Link,
                href: `?pipeline_id=${props.pipeline.descriptor.pipeline_id}&relation=${item.name}#${tab}`
              }}
            >
              {item.name}
            </MenuItem>
          )}
        />
      </FormControl>
    </Box>
  )
}

type Tab = 'browse' | 'insert'

const TableInspector = ({
  pipeline,
  setTab,
  tab,
  relation
}: {
  pipeline: Pipeline
  setTab: (tab: Tab) => void
  tab: string
  relation: string
}) => {
  const logError = (error: Error) => {
    console.error('InspectionTable error: ', error)
  }
  const [rows, setRows] = useState<Row[]>([])

  return (
    <TabContext value={tab}>
      <TabList
        centered
        variant='fullWidth'
        onChange={(_e, tab) => setTab(tab)}
        aria-label='tabs to insert and browse relations'
      >
        <Tab value='browse' label={`Browse ${relation}`} />
        <Tab value='insert' label='Insert New Rows' />
      </TabList>
      <TabPanel value='browse'>
        <ViewInspector pipeline={pipeline} relation={relation} />
      </TabPanel>
      <TabPanel value='insert'>
        {pipeline.state.current_status === PipelineStatus.RUNNING ? (
          <ErrorBoundary FallbackComponent={ErrorOverlay} onError={logError} key={location.pathname}>
            <InsertionTable pipeline={pipeline} name={relation} insert={{ rows, setRows }} />
          </ErrorBoundary>
        ) : (
          <Alert severity='info'>
            <AlertTitle>Pipeline not running</AlertTitle>
            Pipeline must be running to insert data. Try starting it.
          </Alert>
        )}
      </TabPanel>
    </TabContext>
  )
}

const ViewInspector = (props: { pipeline: Pipeline; relation: string }) => {
  const { pipeline, relation } = props
  const logError = (error: Error) => {
    console.error('InspectionTable error: ', error)
  }

  return pipeline.state.current_status === PipelineStatus.RUNNING ||
    pipeline.state.current_status === PipelineStatus.PAUSED ? (
    <ErrorBoundary FallbackComponent={ErrorOverlay} onError={logError} key={location.pathname}>
      <InspectionTable pipeline={pipeline} name={relation} />
    </ErrorBoundary>
  ) : (
    <ErrorOverlay error={new Error(`'${pipeline.descriptor.name}' is not deployed.`)} />
  )
}

export default () => {
  const [tab, setTab] = (([tab, setTab]) => [tab || 'browse', setTab])(useHashPart<Tab>())

  // Parse config, view, tab arguments from router query
  const query = useSearchParams()

  const pipelineId = query.get('pipeline_id')
  const relation = query.get('relation')

  // Load the pipeline
  const { data: pipeline } = useQuery({
    ...PipelineManagerQuery.pipelineStatus(pipelineId!),
    enabled: pipelineId !== undefined
  })

  // Load the last revision of the pipeline
  const { data: pipelineRevision } = useQuery({
    ...PipelineManagerQuery.pipelineLastRevision(pipelineId!),
    enabled: pipelineId !== undefined,
    select(pipelineRevision) {
      const program = pipelineRevision?.program
      const tables = program?.schema?.inputs.map(v => v.name) || []
      const views = program?.schema?.outputs.map(v => v.name) || []
      return {
        ...pipelineRevision,
        tables,
        views
      }
    }
  })

  // If we request to be on the insert tab for a view, we force-switch to the
  // browse tab.
  {
    const views = pipelineRevision?.views
    useEffect(() => {
      if (relation && views && views.includes(relation) && tab == 'insert') {
        setTab('browse')
      }
    }, [setTab, relation, views, tab])
  }

  if (!relation || !pipeline || !pipelineRevision) {
    return <></>
  }
  const { tables, views } = pipelineRevision
  const relationType = tables.includes(relation) ? 'table' : views.includes(relation) ? 'view' : undefined

  if (!relationType) {
    return (
      <Alert severity='error'>
        <AlertTitle>Relation not found</AlertTitle>
        Unknown table or view: {relation}
      </Alert>
    )
  }
  return (
    <>
      <BreadcrumbsHeader>
        <Link href={`/streaming/management`}>Pipelines</Link>
        <Link href={`/streaming/management/#${pipeline.descriptor.pipeline_id}`}>{pipeline.descriptor.name}</Link>
        <TablesBreadcrumb pipeline={pipeline} relation={relation} tables={tables} views={views}></TablesBreadcrumb>
      </BreadcrumbsHeader>
      <Grid item xs={12}>
        {relationType === 'table' && (
          <TableInspector pipeline={pipeline} setTab={setTab} tab={tab} relation={relation} />
        )}
        {relationType === 'view' && <ViewInspector pipeline={pipeline} relation={relation} />}
      </Grid>
    </>
  )
}
