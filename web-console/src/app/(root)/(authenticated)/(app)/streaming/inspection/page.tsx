// Browse tables and views & insert data into tables.
'use client'

import { Breadcrumbs } from '$lib/components/common/BreadcrumbsHeader'
import { ErrorOverlay } from '$lib/components/common/table/ErrorOverlay'
import { InsertionTable } from '$lib/components/streaming/import/InsertionTable'
import { InspectionTable } from '$lib/components/streaming/inspection/InspectionTable'
import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { caseDependentNameEq, getCaseDependentName, getCaseIndependentName } from '$lib/functions/felderaRelation'
import { Relation } from '$lib/services/manager'
import { Pipeline, PipelineStatus } from '$lib/types/pipeline'
import { useSearchParams } from 'next/navigation'
import { SetStateAction, useEffect, useState } from 'react'
import { ErrorBoundary } from 'react-error-boundary'
import { nonNull } from 'src/lib/functions/common/function'

import { useHash } from '@mantine/hooks'
import TabContext from '@mui/lab/TabContext'
import TabList from '@mui/lab/TabList'
import TabPanel from '@mui/lab/TabPanel'
import { Alert, AlertTitle, Autocomplete, Box, FormControl, Link, MenuItem, TextField } from '@mui/material'
import Grid from '@mui/material/Grid'
import Tab from '@mui/material/Tab'
import { useQuery } from '@tanstack/react-query'

import type { Row } from '$lib/functions/ddl'
const TablesBreadcrumb = (props: {
  pipeline: Pipeline
  caseIndependentName: string
  relationType: 'table' | 'view'
  tables: Relation[]
  views: Relation[]
}) => {
  const [tab] = useHash()
  const options = props.tables
    .map(relation => ({ type: 'Tables', name: getCaseIndependentName(relation) }))
    .concat(props.views.map(relation => ({ type: 'Views', name: getCaseIndependentName(relation) })))
  return (
    <Box sx={{ mb: '-1rem' }}>
      <FormControl sx={{ mt: '-1rem' }}>
        <Autocomplete
          isOptionEqualToValue={(a, b) => a.name === b.name && a.type === b.type}
          key={props.caseIndependentName} // Changing the key forces autocomplete to close when relation is changed
          size='small'
          options={options}
          groupBy={option => option.type}
          getOptionLabel={o => o.name}
          sx={{ width: 400 }}
          slotProps={{ popupIndicator: { 'data-testid': 'button-expand-relations' } as any }}
          ListboxProps={{ 'data-testid': 'box-relation-options' } as any}
          renderInput={params => <TextField {...params} value={props.caseIndependentName} label='Tables and Views' />}
          value={{ name: props.caseIndependentName, type: props.relationType === 'table' ? 'Tables' : 'Views' }}
          renderOption={(_props, item) => (
            <MenuItem
              key={item.name}
              value={item.name}
              {...{
                component: Link,
                href: `?pipeline_name=${props.pipeline.descriptor.name}&relation=${item.name}${tab}`
              }}
              data-testid={`button-option-relation-${item.name}`}
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
  caseIndependentName
}: {
  pipeline: Pipeline
  setTab: (tab: Tab) => void
  tab: string
  caseIndependentName: string
}) => {
  const logError = (error: Error) => {
    console.error('InspectionTable error: ', error)
  }
  const [relationsRows, setRelationsRows] = useState<Record<string, Row[]>>({})
  const rows = relationsRows[caseIndependentName] ?? []
  const setRows = (rows: SetStateAction<Row[]>) =>
    setRelationsRows(old => ({
      ...old,
      [caseIndependentName]: rows instanceof Function ? rows(old[caseIndependentName] ?? []) : rows
    }))

  return (
    <TabContext value={tab}>
      <TabList
        centered
        variant='fullWidth'
        onChange={(_e, tab) => setTab(tab)}
        aria-label='tabs to insert and browse relations'
      >
        <Tab value='browse' label={`Browse ${caseIndependentName}`} data-testid='button-tab-browse' />
        <Tab value='insert' label='Insert New Rows' data-testid='button-tab-insert' />
      </TabList>
      <TabPanel value='browse'>
        <ViewInspector pipeline={pipeline} caseIndependentName={caseIndependentName} />
      </TabPanel>
      <TabPanel value='insert'>
        {pipeline.state.current_status === PipelineStatus.RUNNING ? (
          <ErrorBoundary FallbackComponent={ErrorOverlay} onError={logError} key={location.pathname}>
            <InsertionTable pipeline={pipeline} caseIndependentName={caseIndependentName} insert={{ rows, setRows }} />
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

const ViewInspector = ({ pipeline, caseIndependentName }: { pipeline: Pipeline; caseIndependentName: string }) => {
  const logError = (error: Error) => {
    console.error('InspectionTable error: ', error)
  }

  return pipeline.state.current_status === PipelineStatus.RUNNING ||
    pipeline.state.current_status === PipelineStatus.PAUSED ? (
    <ErrorBoundary FallbackComponent={ErrorOverlay} onError={logError} key={location.pathname}>
      <InspectionTable pipeline={pipeline} caseIndependentName={caseIndependentName} />
    </ErrorBoundary>
  ) : (
    <ErrorOverlay error={new Error(`'${pipeline.descriptor.name}' is not deployed.`)} />
  )
}

export default () => {
  const [tab, setTab] = (([tab, setTab]) => [tab.slice(1) || 'browse', setTab])(useHash())

  // Parse config, view, tab arguments from router query
  const query = useSearchParams()

  const pipelineName = query.get('pipeline_name')
  const caseIndependentName = query.get('relation')

  const pipelineManagerQuery = usePipelineManagerQuery()

  // Load the pipeline
  const { data: pipeline } = useQuery({
    ...pipelineManagerQuery.pipelineStatus(pipelineName!),
    enabled: nonNull(pipelineName)
  })

  // Load the last revision of the pipeline
  const { data: pipelineRevision } = useQuery({
    ...pipelineManagerQuery.pipelineLastRevision(pipelineName!),
    enabled: pipelineName !== undefined,
    select(pipelineRevision) {
      const program = pipelineRevision?.program
      const tables = program?.schema?.inputs || []
      const views = program?.schema?.outputs || []
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
      if (
        caseIndependentName &&
        views &&
        views.find(caseDependentNameEq(getCaseDependentName(caseIndependentName))) &&
        tab === 'insert'
      ) {
        setTab('browse')
      }
    }, [setTab, caseIndependentName, views, tab])
  }
  if (!caseIndependentName || !pipeline || !pipelineRevision) {
    return <></>
  }
  const { tables, views } = pipelineRevision
  const relationType = (() => {
    const validTable = tables.find(caseDependentNameEq(getCaseDependentName(caseIndependentName)))
    if (validTable) {
      return 'table'
    }
    const validView = views.find(caseDependentNameEq(getCaseDependentName(caseIndependentName)))
    if (validView) {
      return 'view'
    }
    return undefined
  })()

  if (!relationType) {
    return (
      <Alert severity='error'>
        <AlertTitle>Relation not found</AlertTitle>
        Unknown table or view: {caseIndependentName}
      </Alert>
    )
  }
  return (
    <>
      <Breadcrumbs.Header>
        <Breadcrumbs.Link href={`/streaming/management`} data-testid='button-breadcrumb-pipelines'>
          Pipelines
        </Breadcrumbs.Link>
        <Breadcrumbs.Link
          href={`/streaming/management/#${pipeline.descriptor.name}`}
          data-testid='button-current-pipeline'
        >
          {pipeline.descriptor.name}
        </Breadcrumbs.Link>
        <TablesBreadcrumb
          pipeline={pipeline}
          caseIndependentName={caseIndependentName}
          relationType={relationType}
          tables={tables}
          views={views}
        ></TablesBreadcrumb>
      </Breadcrumbs.Header>
      <Box data-testid='box-inspection-background' sx={{ width: 2, height: 2 }}></Box>
      <Grid item xs={12}>
        {relationType === 'table' && (
          <TableInspector pipeline={pipeline} setTab={setTab} tab={tab} caseIndependentName={caseIndependentName} />
        )}
        {relationType === 'view' && <ViewInspector pipeline={pipeline} caseIndependentName={caseIndependentName} />}
      </Grid>
    </>
  )
}
