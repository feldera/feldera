import { ErrorOverlay } from '$lib/components/common/table/ErrorOverlay'
import { InsertionTable } from '$lib/components/streaming/import/InsertionTable'
import { ViewInspectionTab } from '$lib/components/streaming/inspection/ViewInspectionTab'
import { Row } from '$lib/functions/sqlValue'
import { PipelineRevision, Relation } from '$lib/services/manager'
import { Pipeline, PipelineStatus } from '$lib/types/pipeline'
import { SetStateAction, useEffect, useReducer, useState } from 'react'
import { ErrorBoundary } from 'react-error-boundary'

import TabContext from '@mui/lab/TabContext'
import TabList from '@mui/lab/TabList'
import TabPanel from '@mui/lab/TabPanel'
import { Alert, AlertTitle } from '@mui/material'
import Tab from '@mui/material/Tab'

type Tab = 'browse' | 'insert'

export const TableInspectionTab = ({
  pipeline,
  setTab,
  tab,
  pipelineRevision,
  caseIndependentName,
  relation
}: {
  pipeline: Pipeline
  setTab: (tab: Tab) => void
  tab: string
  pipelineRevision: PipelineRevision
  caseIndependentName: string
  relation: Relation
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

  {
    // If the revision of the pipeline has changed we drop the rows ready to be imported
    // as they may no longer be valid as program schema might have changed
    const [, updateRevision] = useReducer((oldRevision: string, newRevision: string) => {
      if (oldRevision !== newRevision) {
        setRows([])
      }
      return newRevision
    }, pipelineRevision.revision)

    useEffect(() => {
      updateRevision(pipelineRevision.revision)
    }, [pipelineRevision.revision, updateRevision])
  }

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
        <ViewInspectionTab pipeline={pipeline} caseIndependentName={caseIndependentName} />
      </TabPanel>
      <TabPanel value='insert'>
        {pipeline.state.current_status === PipelineStatus.RUNNING ? (
          <ErrorBoundary FallbackComponent={ErrorOverlay} onError={logError} key={location.pathname}>
            <InsertionTable pipelineRevision={pipelineRevision} relation={relation} insert={{ rows, setRows }} />
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
