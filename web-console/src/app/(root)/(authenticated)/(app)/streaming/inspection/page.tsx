// Browse tables and views & insert data into tables.
'use client'

import { Breadcrumbs } from '$lib/components/common/BreadcrumbsHeader'
import { TableInspectionTab } from '$lib/components/streaming/inspection/TableInspectionTab'
import { ViewInspectionTab } from '$lib/components/streaming/inspection/ViewInspectionTab'
import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { caseDependentNameEq, getCaseDependentName, getCaseIndependentName } from '$lib/functions/felderaRelation'
import { Relation } from '$lib/services/manager'
import { Pipeline } from '$lib/types/pipeline'
import { useSearchParams } from 'next/navigation'
import { useEffect } from 'react'
import { nonNull } from 'src/lib/functions/common/function'

import { useHash } from '@mantine/hooks'
import { Alert, AlertTitle, Autocomplete, Box, FormControl, Link, MenuItem, TextField } from '@mui/material'
import Grid from '@mui/material/Grid'
import { useQuery } from '@tanstack/react-query'

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
        ...pipelineRevision!,
        tables,
        views
      }
    }
  })

  {
    // If we request to be on the insert tab for a view, we force-switch to the browse tab.
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
  const { relation, relationType } = (() => {
    const table = tables.find(caseDependentNameEq(getCaseDependentName(caseIndependentName)))
    if (table) {
      return { relation: table, relationType: 'table' } as const
    }
    const view = views.find(caseDependentNameEq(getCaseDependentName(caseIndependentName)))
    if (view) {
      return { relation: view, relationType: 'view' } as const
    }
    return { relation: undefined, relationType: undefined }
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
          <TableInspectionTab
            pipeline={pipeline}
            setTab={setTab}
            tab={tab}
            pipelineRevision={pipelineRevision}
            caseIndependentName={caseIndependentName}
            relation={relation}
          />
        )}
        {relationType === 'view' && <ViewInspectionTab pipeline={pipeline} caseIndependentName={caseIndependentName} />}
      </Grid>
    </>
  )
}
