// Browse tables and views & insert data into tables.

import { ErrorOverlay } from '$lib/components/common/table/ErrorOverlay'
import { InsertionTable } from '$lib/components/streaming/import/InsertionTable'
import { InspectionTable } from '$lib/components/streaming/inspection/InspectionTable'
import { Pipeline, PipelineId, PipelineRevision, PipelineStatus } from '$lib/services/manager'
import { useRouter } from 'next/router'
import { SyntheticEvent, useEffect, useState } from 'react'
import { ErrorBoundary } from 'react-error-boundary'
import { Controller, useForm } from 'react-hook-form'
import PageHeader from 'src/lib/components/layouts/pageHeader'

import { Icon } from '@iconify/react'
import TabContext from '@mui/lab/TabContext'
import TabList from '@mui/lab/TabList'
import TabPanel from '@mui/lab/TabPanel'
import {
  Alert,
  AlertTitle,
  Breadcrumbs,
  FormControl,
  InputLabel,
  Link,
  ListSubheader,
  MenuItem,
  Select,
  SelectChangeEvent
} from '@mui/material'
import Grid from '@mui/material/Grid'
import Tab from '@mui/material/Tab'
import { useQuery } from '@tanstack/react-query'

const TitleBreadCrumb = (props: { pipeline: Pipeline; relation: string; tables: string[]; views: string[] }) => {
  const { tables, views, relation } = props
  const { descriptor } = props.pipeline
  const pipeline_id = descriptor.pipeline_id
  const router = useRouter()

  const switchRelation = (e: SelectChangeEvent<string>) => {
    router.push(`/streaming/inspection/?pipeline_id=${pipeline_id}&relation=${e.target.value}`)
  }

  interface IFormInputs {
    relation: string
  }

  const { control } = useForm<IFormInputs>({
    defaultValues: {
      relation: relation
    }
  })

  return typeof relation === 'string' && (tables.length > 0 || views.length > 0) ? (
    <Breadcrumbs separator={<Icon icon='bx:chevron-right' fontSize={20} />} aria-label='breadcrumb'>
      <Link href={`/streaming/management/?pipeline_id=${descriptor.pipeline_id}`}>{descriptor.name}</Link>
      <Controller
        name='relation'
        control={control}
        defaultValue={relation}
        render={({ field: { onChange, value } }) => {
          return (
            <FormControl>
              <InputLabel htmlFor='relation-select'>Relation</InputLabel>
              <Select
                label='Select Relation'
                id='relation-select'
                onChange={e => {
                  e.preventDefault()
                  switchRelation(e)
                  onChange(e)
                }}
                value={value}
              >
                <ListSubheader>Tables</ListSubheader>
                {tables.map(item => (
                  <MenuItem key={item} value={item}>
                    {item}
                  </MenuItem>
                ))}
                <ListSubheader>Views</ListSubheader>
                {views.map(item => (
                  <MenuItem key={item} value={item}>
                    {item}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          )
        }}
      />
    </Breadcrumbs>
  ) : (
    <></>
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
  const router = useRouter()
  const [tab, setTab] = useState<'browse' | 'insert'>('browse')
  const [tables, setTables] = useState<string[] | undefined>(undefined)
  const [views, setViews] = useState<string[] | undefined>(undefined)

  const handleChange = (event: SyntheticEvent, newValue: 'browse' | 'insert') => {
    setTab(newValue)
  }

  // Parse config, view, tab arguments from router query
  useEffect(() => {
    if (!router.isReady) {
      return
    }
    const { pipeline_id, relation, tab } = router.query
    if (typeof tab === 'string' && (tab == 'browse' || tab == 'insert')) {
      setTab(tab)
    }
    if (typeof pipeline_id === 'string') {
      setPipelineId(pipeline_id)
    }
    if (typeof relation === 'string') {
      setRelation(relation)
    }
  }, [pipelineId, setPipelineId, setRelation, router])

  // Load the pipeline
  const [pipeline, setPipeline] = useState<Pipeline | undefined>(undefined)
  const configQuery = useQuery<Pipeline>(['pipelineStatus', { pipeline_id: pipelineId }], {
    enabled: pipelineId !== undefined
  })
  useEffect(() => {
    if (!configQuery.isLoading && !configQuery.isError) {
      setPipeline(configQuery.data)
    }
  }, [configQuery.isLoading, configQuery.isError, configQuery.data, setPipeline])

  // Load the last revision of the pipeline
  const pipelineRevisionQuery = useQuery<PipelineRevision>(['pipelineLastRevision', { pipeline_id: pipelineId }], {
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
    <Grid container spacing={6} className='match-height'>
      <PageHeader title={<TitleBreadCrumb pipeline={pipeline} relation={relation} tables={tables} views={views} />} />
      <Grid item xs={12}>
        {tables.includes(relation) && (
          <TableWithInsertTab pipeline={pipeline} handleChange={handleChange} tab={tab} relation={relation} />
        )}
        {views.includes(relation) && <ViewDataTable pipeline={pipeline} relation={relation} />}
      </Grid>
    </Grid>
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
