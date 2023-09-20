// Browse tables and views & insert data into tables.
'use client'

import { BreadcrumbSelect } from '$lib/components/common/BreadcrumbSelect'
import { ErrorOverlay } from '$lib/components/common/table/ErrorOverlay'
import { InsertionTable } from '$lib/components/streaming/import/InsertionTable'
import { InspectionTable } from '$lib/components/streaming/inspection/InspectionTable'
import { Pipeline, PipelineId, PipelineStatus } from '$lib/services/manager'
import { PipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import { useSearchParams } from 'next/navigation'
import { SyntheticEvent, useEffect, useState } from 'react'
import { ErrorBoundary } from 'react-error-boundary'
import { BreadcrumbsHeader } from 'src/lib/components/common/BreadcrumbsHeader'

import TabContext from '@mui/lab/TabContext'
import TabList from '@mui/lab/TabList'
import TabPanel from '@mui/lab/TabPanel'
import { Alert, AlertTitle, Link, ListSubheader, MenuItem } from '@mui/material'
import Grid from '@mui/material/Grid'
import Tab from '@mui/material/Tab'
import { useQuery } from '@tanstack/react-query'

const TablesBreadcrumb = (props: { pipeline: Pipeline; relation: string; tables: string[]; views: string[] }) => {
  return (
    <BreadcrumbSelect label='Relation' value={props.relation}>
      <ListSubheader>Tables</ListSubheader>
      {props.tables.map(item => (
        <MenuItem key={item} value={item}>
          <Link href={`?pipeline_id=${props.pipeline.descriptor.pipeline_id}&relation=${item}`}>{item}</Link>
        </MenuItem>
      ))}
      <ListSubheader>Views</ListSubheader>
      {props.views.map(item => (
        <MenuItem key={item} value={item}>
          <Link href={`?pipeline_id=${props.pipeline.descriptor.pipeline_id}&relation=${item}`}>{item}</Link>
        </MenuItem>
      ))}
    </BreadcrumbSelect>
  )
}

const TableWithInsertTab = (props: {
  pipeline: Pipeline
  handleChange: ((event: SyntheticEvent<Element, Event>, value: any) => void) | undefined
  tab: string
  relation: string
}) => {
  const logError = (error: Error) => {
    console.error('InspectionTable error: ', error)
  }

  const { pipeline, handleChange, tab, relation } = props
  return (
    <TabContext value={tab}>
      <TabList centered variant='fullWidth' onChange={handleChange} aria-label='tabs to insert and browse relations'>
        <Tab value='browse' label={`Browse ${relation}`} />
        <Tab value='insert' label='Insert New Rows' />
      </TabList>
      <TabPanel value='browse'>
        <ViewDataTable pipeline={pipeline} relation={relation} />
      </TabPanel>
      <TabPanel value='insert'>
        {pipeline.state.current_status === PipelineStatus.RUNNING ? (
          <ErrorBoundary FallbackComponent={ErrorOverlay} onError={logError} key={location.pathname}>
            <InsertionTable pipeline={pipeline} name={relation} />
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

const ViewDataTable = (props: { pipeline: Pipeline; relation: string }) => {
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

const IntrospectInputOutput = () => {
  const [pipelineId, setPipelineId] = useState<PipelineId | undefined>(undefined)
  const [relation, setRelation] = useState<string | undefined>(undefined)
  const [tab, setTab] = useState<'browse' | 'insert'>('browse')
  const [tables, setTables] = useState<string[] | undefined>(undefined)
  const [views, setViews] = useState<string[] | undefined>(undefined)

  const handleChange = (event: SyntheticEvent, newValue: 'browse' | 'insert') => {
    setTab(newValue)
  }

  // Parse config, view, tab arguments from router query
  const query = Object.fromEntries(useSearchParams().entries())
  useEffect(() => {
    const { pipeline_id, relation, tab } = query
    if (typeof tab === 'string' && (tab === 'browse' || tab === 'insert')) {
      setTab(tab)
    }
    if (typeof pipeline_id === 'string') {
      setPipelineId(pipeline_id)
    }
    if (typeof relation === 'string') {
      setRelation(relation)
    }
  }, [query, pipelineId, setPipelineId, setRelation])

  // Load the pipeline
  const [pipeline, setPipeline] = useState<Pipeline | undefined>(undefined)
  const configQuery = useQuery({
    ...PipelineManagerQuery.pipelineStatus(pipelineId!),
    enabled: pipelineId !== undefined
  })
  useEffect(() => {
    if (!configQuery.isLoading && !configQuery.isError) {
      setPipeline(configQuery.data)
    }
  }, [configQuery.isLoading, configQuery.isError, configQuery.data, setPipeline])

  // Load the last revision of the pipeline
  const pipelineRevisionQuery = useQuery({
    ...PipelineManagerQuery.pipelineLastRevision(pipelineId!),
    enabled: pipelineId !== undefined
  })
  useEffect(() => {
    if (!pipelineRevisionQuery.isLoading && !pipelineRevisionQuery.isError) {
      const pipelineRevision = pipelineRevisionQuery.data
      const program = pipelineRevision?.program
      setTables(program?.schema?.inputs.map(v => v.name) || [])
      setViews(program?.schema?.outputs.map(v => v.name) || [])
    }
  }, [pipelineRevisionQuery.isLoading, pipelineRevisionQuery.isError, pipelineRevisionQuery.data])

  // If we request to be on the insert tab for a view, we force-switch to the
  // browse tab.
  useEffect(() => {
    if (relation && views && views.includes(relation) && tab == 'insert') {
      setTab('browse')
    }
  }, [setTab, relation, views, tab])

  const relationValid =
    pipeline && relation && tables && views && (tables.includes(relation) || views.includes(relation))

  return pipelineId !== undefined &&
    !configQuery.isLoading &&
    !configQuery.isError &&
    !pipelineRevisionQuery.isLoading &&
    !pipelineRevisionQuery.isError &&
    relationValid ? (
    <>
      <BreadcrumbsHeader>
        <Link href={`/streaming/management`}>Pipelines</Link>
        <Link href={`/streaming/management/#${pipeline.descriptor.pipeline_id}`}>{pipeline.descriptor.name}</Link>
        <TablesBreadcrumb pipeline={pipeline} relation={relation} tables={tables} views={views}></TablesBreadcrumb>
      </BreadcrumbsHeader>
      <Grid item xs={12}>
        {tables.includes(relation) && (
          <TableWithInsertTab pipeline={pipeline} handleChange={handleChange} tab={tab} relation={relation} />
        )}
        {views.includes(relation) && <ViewDataTable pipeline={pipeline} relation={relation} />}
      </Grid>
    </>
  ) : (
    relation && tables && views && !(tables.includes(relation) || views.includes(relation)) && (
      <Alert severity='error'>
        <AlertTitle>Relation not found</AlertTitle>
        Unknown table or view: {relation}
      </Alert>
    )
  )
}

export default IntrospectInputOutput
