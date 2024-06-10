// Table showing the list of pipelines for a user.
//
// The rows of the table can be expanded for even more details.
'use client'

import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { DataGridFooter } from '$lib/components/common/table/DataGridFooter'
import { DataGridPro } from '$lib/components/common/table/DataGridProDeclarative'
import DataGridSearch from '$lib/components/common/table/DataGridSearch'
import DataGridToolbar from '$lib/components/common/table/DataGridToolbar'
import { ResetColumnViewButton } from '$lib/components/common/table/ResetColumnViewButton'
import { TextIcon } from '$lib/components/common/TextIcon'
import { AnalyticsPipelineTput } from '$lib/components/streaming/management/AnalyticsPipelineTput'
import { PipelineMemoryGraph } from '$lib/components/streaming/management/PipelineMemoryGraph'
import { PipelineRevisionStatusChip } from '$lib/components/streaming/management/RevisionStatus'
import { useDataGridPresentationLocalStorage } from '$lib/compositions/persistence/dataGrid'
import { usePipelineMetrics } from '$lib/compositions/streaming/management/usePipelineMetrics'
import { usePipelineMutation } from '$lib/compositions/streaming/management/usePipelineMutation'
import { useDeleteDialog } from '$lib/compositions/useDialog'
import { useHashPart } from '$lib/compositions/useHashPart'
import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { humanSize } from '$lib/functions/common/string'
import { invalidateQuery } from '$lib/functions/common/tanstack'
import { tuple } from '$lib/functions/common/tuple'
import { caseDependentNameEq, getCaseDependentName, getCaseIndependentName } from '$lib/functions/felderaRelation'
import { ApiError, AttachedConnector, ConnectorDescr, PipelineRevision, Relation } from '$lib/services/manager'
import {
  mutationDeletePipeline,
  mutationPausePipeline,
  mutationShutdownPipeline,
  mutationStartPipeline,
  mutationUpdatePipeline,
  PipelineManagerQueryKey
} from '$lib/services/pipelineManagerQuery'
import { LS_PREFIX } from '$lib/types/localStorage'
import { Pipeline, PipelineStatus } from '$lib/types/pipeline'
import { format } from 'd3-format'
import dayjs from 'dayjs'
import Link from 'next/link'
import React, { useCallback, useEffect, useState } from 'react'
import { TwoSeventyRingWithBg } from 'react-svg-spinners'
import invariant from 'tiny-invariant'
import { match, P } from 'ts-pattern'

import CustomChip from '@core/components/mui/chip'
import { useLocalStorage } from '@mantine/hooks'
import ExpandMoreIcon from '@mui/icons-material/ExpandMore'
import { alpha, Button, ChipProps, Typography, useTheme } from '@mui/material'
import Badge from '@mui/material/Badge'
import Box from '@mui/material/Box'
import Card from '@mui/material/Card'
import Grid from '@mui/material/Grid'
import IconButton from '@mui/material/IconButton'
import List from '@mui/material/List'
import ListItem from '@mui/material/ListItem'
import ListItemIcon from '@mui/material/ListItemIcon'
import ListItemText from '@mui/material/ListItemText'
import ListSubheader from '@mui/material/ListSubheader'
import Paper from '@mui/material/Paper'
import Tooltip from '@mui/material/Tooltip'
import {
  DataGridProProps,
  GRID_DETAIL_PANEL_TOGGLE_COL_DEF,
  GridColDef,
  GridRenderCellParams,
  GridRow,
  GridRowId,
  GridRowProps,
  GridToolbarFilterButton,
  useGridApiRef
} from '@mui/x-data-grid-pro'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

interface ConnectorData {
  relation: Relation
  connections: [AttachedConnector, ConnectorDescr][]
}

type InputOrOutput = 'input' | 'output'

// Joins the relation with attached connectors and connectors and returns it as
// a list of ConnectorData that has a list of `connections` for each `relation`.
function getConnectorData(revision: PipelineRevision, direction: InputOrOutput): ConnectorData[] {
  const schema = revision.program.schema
  if (!schema) {
    // This means the backend sent invalid data,
    // revisions should always have a schema
    throw Error('Pipeline revision has no schema.')
  }

  const relations = direction === ('input' as const) ? schema.inputs : schema.outputs
  const attachedConnectors = revision.pipeline.attached_connectors
  const connectors = revision.connectors

  return relations.map(relation => {
    const connections = attachedConnectors
      .filter(ac => caseDependentNameEq(getCaseDependentName(ac.relation_name))(relation))
      .map(ac => {
        const connector = connectors.find(c => c.name === ac?.connector_name)
        invariant(connector, 'Attached connector has no connector.') // This can't happen in a revision
        return tuple(ac, connector)
      })

    return { relation, connections }
  })
}

const keepMetricsMs = 30000

const DetailPanelContent = (props: { row: Pipeline }) => {
  const theme = useTheme()
  const [inputs, setInputs] = useState<ConnectorData[]>([])
  const [outputs, setOutputs] = useState<ConnectorData[]>([])
  const { descriptor, state } = props.row
  const pipelineManagerQuery = usePipelineManagerQuery()
  const pipelineRevisionQuery = useQuery(pipelineManagerQuery.pipelineLastRevision(descriptor.name))
  useEffect(() => {
    if (!pipelineRevisionQuery.isPending && !pipelineRevisionQuery.isError && pipelineRevisionQuery.data) {
      setInputs(getConnectorData(pipelineRevisionQuery.data, 'input'))
      setOutputs(getConnectorData(pipelineRevisionQuery.data, 'output'))
    }
  }, [
    pipelineRevisionQuery.isPending,
    pipelineRevisionQuery.isError,
    pipelineRevisionQuery.data,
    setInputs,
    setOutputs
  ])

  const metrics = usePipelineMetrics({
    pipelineName: descriptor.name,
    status: state.current_status,
    refetchMs: 1000,
    keepMs: keepMetricsMs + 1000
  })

  function getRelationColumns(direction: InputOrOutput): GridColDef<ConnectorData>[] {
    return [
      {
        field: 'name',
        headerName: direction === 'input' ? 'Input' : 'Output',
        flex: 0.4,
        display: 'flex',
        renderCell: params =>
          params.row.connections.length > 0 ? (
            params.row.connections.map(c => c[1].name).join(', ')
          ) : (
            <Box sx={{ fontStyle: 'italic' }}>No connection.</Box>
          )
      },
      {
        field: 'config',
        headerName: direction === 'input' ? 'Table' : 'View',
        flex: 0.6,
        valueGetter: (_, row) => getCaseIndependentName(row.relation),
        display: 'flex'
      },
      {
        field: 'records',
        headerName: 'Records',
        flex: 0.3,
        display: 'flex',
        renderCell: params =>
          (() => {
            if (params.row.connections.length > 0) {
              const records =
                (direction === 'input'
                  ? metrics.input.get(getCaseIndependentName(params.row.relation))?.total_records
                  : metrics.output.get(getCaseIndependentName(params.row.relation))?.transmitted_records) || 0
              return format(records >= 1000 ? '.3s' : '~s')(records)
            } else {
              // TODO: we need to count records also when relation doesn't have
              // connections in the backend.
              return '-'
            }
          })()
      },
      {
        field: 'traffic',
        headerName: 'Traffic',
        flex: 0.15,
        display: 'flex',
        renderCell: params =>
          (() => {
            const bytes =
              direction === 'input'
                ? metrics.input.get(getCaseIndependentName(params.row.relation))?.total_bytes
                : metrics.output.get(getCaseIndependentName(params.row.relation))?.transmitted_bytes
            return humanSize(bytes || 0)
          })()
      },
      {
        field: 'errors',
        headerName: 'Errors',
        flex: 0.15,
        display: 'flex',
        renderCell: params =>
          (() => {
            const errors =
              direction === 'input'
                ? (m => (m ? m.num_parse_errors + m.num_transport_errors : 0))(
                    metrics.input.get(getCaseIndependentName(params.row.relation))
                  )
                : (m => (m ? m.num_encode_errors + m.num_transport_errors : 0))(
                    metrics.output.get(getCaseIndependentName(params.row.relation))
                  )
            return (
              <Box
                sx={{
                  width: '100%',
                  height: '200%',
                  display: 'flex',
                  px: 2,
                  alignItems: 'center',
                  backgroundColor: errors > 0 ? alpha(theme.palette.warning.main, 0.5) : undefined
                }}
              >
                {format(',')(errors)}
              </Box>
            )
          })()
      },
      {
        field: 'action',
        headerName: 'Action',
        flex: 0.15,
        display: 'flex',
        renderCell: params => (
          <Box data-testid={`box-relation-actions-${params.row.relation.name}`}>
            <Tooltip title={direction === 'input' ? 'Inspect Table' : 'Inspect View'}>
              <IconButton
                size='small'
                href={`/streaming/inspection/?pipeline_name=${descriptor.name}&relation=${getCaseIndependentName(
                  params.row.relation
                )}`}
                data-testid='button-inspect'
              >
                <i className={`bx bx-show`} style={{ fontSize: 24 }} />
              </IconButton>
            </Tooltip>
            {direction === 'input' && state.current_status == PipelineStatus.RUNNING && (
              <Tooltip title='Import Data'>
                <IconButton
                  size='small'
                  href={`/streaming/inspection/?pipeline_name=${descriptor.name}&relation=${getCaseIndependentName(
                    params.row.relation
                  )}#insert`}
                  data-testid='button-import'
                >
                  <i className={`bx bx-download`} style={{ fontSize: 24 }} />
                </IconButton>
              </Tooltip>
            )}
          </Box>
        )
      }
    ]
  }

  const gridPersistence = useDataGridPresentationLocalStorage({
    key: LS_PREFIX + 'settings/streaming/management/details/grid'
  })

  return (
    <Box display='flex' sx={{ m: 2 }} justifyContent='center' data-testid={`box-details-${props.row.descriptor.name}`}>
      <Grid container spacing={3} sx={{ height: 1 }} alignItems='stretch'>
        <Grid item xs={4}>
          <Card sx={{ height: '100%' }}>
            <List subheader={<ListSubheader>Configuration</ListSubheader>} dense>
              <ListItem>
                <ListItemIcon>
                  <TextIcon size={24} fontSize={10} text='SQL' />
                </ListItemIcon>
                <ListItemText
                  primary={props.row.descriptor.program_name || 'not set'}
                  secondary={props.row.descriptor.pipeline_id || 'not set'}
                  secondaryTypographyProps={
                    {
                      'data-testid': 'box-pipeline-id',
                      sx: { width: '100%' },
                      variant: 'caption'
                    } as any
                  }
                  sx={{ mr: '-1rem' }}
                />
              </ListItem>
              {state.current_status == PipelineStatus.RUNNING && (
                <>
                  <ListItem>
                    <Tooltip title='Pipeline Running Since'>
                      <ListItemIcon>
                        <i className={`bx bx-calendar`} style={{ fontSize: 24 }} />
                      </ListItemIcon>
                    </Tooltip>
                    <ListItemText
                      primary={state.created ? dayjs(state.created).format('MM/DD/YYYY HH:MM') : 'Not running'}
                      data-testid='box-pipeline-date-created'
                    />
                  </ListItem>
                  <ListItem>
                    <Tooltip title='Pipeline Port'>
                      <ListItemIcon>
                        <i className={`bx bx-log-in-circle`} style={{ fontSize: 24 }} />
                      </ListItemIcon>
                    </Tooltip>
                    <ListItemText
                      className='pipelinePort'
                      primary={state.location || '0000'}
                      data-testid='box-pipeline-port'
                    />
                  </ListItem>
                </>
              )}
            </List>
          </Card>
        </Grid>
        <Grid item xs={5}>
          <AnalyticsPipelineTput metrics={metrics} keepMs={keepMetricsMs} />
        </Grid>
        <Grid item xs={3}>
          <PipelineMemoryGraph metrics={metrics} keepMs={keepMetricsMs} />
        </Grid>
        <Grid item xs={12}>
          <Paper>
            <DataGridPro
              autoHeight
              getRowId={(row: ConnectorData) => row.relation.name}
              columns={getRelationColumns('input')}
              rows={inputs}
              sx={{ flex: 1 }}
              hideFooter
              {...gridPersistence}
            />
          </Paper>
        </Grid>

        <Grid item xs={12}>
          <Paper>
            <DataGridPro
              autoHeight
              getRowId={(row: ConnectorData) => row.relation.name}
              columns={getRelationColumns('output')}
              rows={outputs}
              sx={{ flex: 1 }}
              hideFooter
              {...gridPersistence}
            />
          </Paper>
        </Grid>
      </Grid>
    </Box>
  )
}

// Only show the details tab button if this pipeline has a revision
function CustomDetailPanelToggleCell({ value: isExpanded, row: row }: GridRenderCellParams<Pipeline, boolean>) {
  const [hadRevision, setHadRevision] = useState<boolean>(false)
  const pipelineManagerQuery = usePipelineManagerQuery()
  const pipelineRevisionQuery = useQuery(pipelineManagerQuery.pipelineLastRevision(row.descriptor.name))
  const hasRevision =
    !pipelineRevisionQuery.isPending && !pipelineRevisionQuery.isError && pipelineRevisionQuery.data !== null
  useEffect(() => {
    if (hasRevision) {
      setHadRevision(true)
    }
  }, [hasRevision, setHadRevision])

  if (
    (!isExpanded &&
      row.state.current_status !== PipelineStatus.RUNNING &&
      row.state.current_status !== PipelineStatus.PAUSED) ||
    !hadRevision
  ) {
    return <></>
  }

  // Display a temporary chevron although full details are not yet available
  if (!pipelineRevisionQuery.data) {
    return (
      <IconButton size='small' data-testid={`button-expand-pipeline-temporary`}>
        <ExpandMoreIcon
          sx={{
            transform: `rotateZ(${isExpanded ? 180 : 0}deg)`
          }}
        />
      </IconButton>
    )
  }

  return (
    <IconButton
      size='small'
      tabIndex={-1}
      aria-label={isExpanded ? 'Close' : 'Open'}
      data-testid={`button-expand-pipeline-${pipelineRevisionQuery.data.pipeline.name}`}
    >
      <ExpandMoreIcon
        sx={{
          transform: `rotateZ(${isExpanded ? 180 : 0}deg)`,
          transition: theme =>
            theme.transitions.create('transform', {
              duration: theme.transitions.duration.shortest
            })
        }}
      />
    </IconButton>
  )
}

export default function PipelineTable() {
  const [rows, setRows] = useState<Pipeline[]>([])

  const [filteredData, setFilteredData] = useState<Pipeline[]>([])
  const [paginationModel, setPaginationModel] = useState({
    pageSize: 7,
    page: 0
  })

  const pipelineManagerQuery = usePipelineManagerQuery()
  const pipelineQuery = useQuery({
    ...pipelineManagerQuery.pipelines(),
    refetchInterval: 2000
  })
  const { isPending, isError, data, error } = pipelineQuery
  useEffect(() => {
    if (!isPending && !isError) {
      setRows(data)
    }
    if (isError) {
      throw error
    }
  }, [isPending, isError, data, setRows, error])
  const getDetailPanelContent = useCallback<NonNullable<DataGridProProps['getDetailPanelContent']>>(
    ({ row }) => <DetailPanelContent row={row} />,
    []
  )
  const columns: GridColDef<Pipeline>[] = [
    {
      ...GRID_DETAIL_PANEL_TOGGLE_COL_DEF,
      display: 'flex',
      renderCell: CustomDetailPanelToggleCell
    },
    {
      field: 'name',
      headerName: 'Name',
      editable: true,
      flex: 0.3,
      display: 'flex',
      valueGetter: (_, row) => row.descriptor.name,
      valueSetter: (value, row) => {
        return { ...row, descriptor: { ...row.descriptor, name: value } }
      },
      renderCell: (params: GridRenderCellParams<Pipeline>) => (
        <Typography
          variant='body2'
          sx={{ color: 'text.primary' }}
          data-testid={`box-pipeline-name-${params.row.descriptor.name}`}
        >
          {params.row.descriptor.name}
        </Typography>
      ),
      renderHeader(params) {
        return (
          <Typography
            fontSize={12}
            sx={{ textTransform: 'uppercase', fontWeight: '530' }}
            data-testid={`box-column-header-${params.field}`}
          >
            {params.field}
          </Typography>
        )
      }
    },
    {
      field: 'description',
      headerName: 'Description',
      editable: true,
      type: 'string',
      flex: 0.4,
      display: 'flex',
      valueGetter: (_, row) => row.descriptor.description,
      valueSetter: (value, row) => {
        return { ...row, descriptor: { ...row.descriptor, description: value } }
      }
    },
    {
      field: 'storage',
      headerName: 'Storage',
      width: 90,
      editable: true,
      type: 'boolean',
      display: 'flex',
      valueGetter: (_, row) => row.descriptor.config.storage,
      valueSetter: (value, row) => {
        return {
          ...row,
          descriptor: { ...row.descriptor, config: { ...row.descriptor.config, storage: value } }
        }
      }
    },
    {
      field: 'modification',
      headerName: 'Changes',
      width: 145,
      display: 'flex',
      renderCell: (params: GridRenderCellParams<Pipeline>) => <PipelineRevisionStatusChip pipeline={params.row} />
    },
    {
      field: 'status',
      headerName: 'Status',
      width: 145,
      display: 'flex',
      renderCell: PipelineStatusCell
    },
    {
      field: 'actions',
      headerName: 'Actions',
      width: 120,
      display: 'flex',
      renderCell: PipelineActionsCell
    }
  ]

  // Makes sure we can edit name and description in the table
  const apiRef = useGridApiRef()
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()
  const { mutate: updatePipeline } = useMutation(mutationUpdatePipeline(queryClient))
  const onUpdateRow = (newRow: Pipeline, oldRow: Pipeline) => {
    updatePipeline(
      {
        pipelineName: oldRow.descriptor.name,
        request: {
          name: newRow.descriptor.name,
          description: newRow.descriptor.description,
          program_name: newRow.descriptor.program_name,
          config: {
            ...newRow.descriptor.config,
            storage: newRow.descriptor.config.storage
          }
        }
      },
      {
        onError: (error: ApiError) => {
          invalidateQuery(queryClient, PipelineManagerQueryKey.pipelines())
          invalidateQuery(queryClient, PipelineManagerQueryKey.pipelineStatus(oldRow.descriptor.name))
          pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
          apiRef.current.updateRows([oldRow])
        }
      }
    )

    return newRow
  }

  const btnAdd = (
    <Button variant='contained' size='small' href='/streaming/builder/' data-testid='button-add-pipeline' key='0'>
      Add pipeline
    </Button>
  )

  const [expandedRows, setExpandedRows] = useLocalStorage({
    key: LS_PREFIX + 'pipelines/expanded',
    defaultValue: [] as GridRowId[]
  })
  const [hash, setHash] = useHashPart()
  const anchorPipelineId = (filteredData.length ? filteredData : rows).find(
    pipeline => pipeline.descriptor.name === decodeURI(hash)
  )?.descriptor.pipeline_id

  // Cannot initialize in useState because hash is not available during SSR
  useEffect(() => {
    if (!anchorPipelineId) {
      return
    }
    setExpandedRows(expandedRows =>
      (expandedRows.includes(anchorPipelineId) ? expandedRows : [...expandedRows, anchorPipelineId]).filter(row =>
        data?.find(
          p =>
            p.descriptor.pipeline_id === row &&
            [
              PipelineStatus.PROVISIONING,
              PipelineStatus.INITIALIZING,
              PipelineStatus.PAUSED,
              PipelineStatus.RUNNING,
              PipelineStatus.SHUTTING_DOWN
            ].includes(p.state.current_status)
        )
      )
    )
  }, [anchorPipelineId, setExpandedRows, data])

  const updateExpandedRows = (newExpandedRows: GridRowId[]) => {
    if (newExpandedRows.length < expandedRows.length && !newExpandedRows.includes(anchorPipelineId || '')) {
      setHash('')
    }
    setExpandedRows(newExpandedRows)
  }

  const gridPersistence = useDataGridPresentationLocalStorage({
    key: LS_PREFIX + 'settings/streaming/management/grid'
  })

  return (
    <Card>
      <DataGridPro
        autoHeight
        apiRef={apiRef}
        getRowId={(row: Pipeline) => row.descriptor.pipeline_id}
        columns={columns}
        getDetailPanelHeight={() => 'auto'}
        getDetailPanelContent={getDetailPanelContent}
        slots={{
          toolbar: DataGridToolbar,
          footer: DataGridFooter,
          row: DataGridRow
        }}
        rows={filteredData.length ? filteredData : rows}
        pageSizeOptions={[7, 10, 25, 50]}
        paginationModel={paginationModel}
        onPaginationModelChange={setPaginationModel}
        processRowUpdate={onUpdateRow}
        loading={isPending}
        slotProps={{
          baseButton: {
            variant: 'outlined'
          },
          toolbar: {
            children: [
              btnAdd,
              <GridToolbarFilterButton key='1' data-testid='button-filter' />,
              <ResetColumnViewButton key='2' {...gridPersistence} />,
              <div style={{ marginLeft: 'auto' }} key='3' />,
              <DataGridSearch fetchRows={pipelineQuery} setFilteredData={setFilteredData} key='99' />
            ]
          },
          footer: {
            children: btnAdd
          }
        }}
        detailPanelExpandedRowIds={expandedRows}
        onDetailPanelExpandedRowIdsChange={updateExpandedRows}
        {...gridPersistence}
      />
    </Card>
  )
}

const DataGridRow = (props: GridRowProps) => {
  return <GridRow data-testid={`box-grid-row-${props.row!.descriptor.name}`} {...props}></GridRow>
}

const usePipelineStatus = (params: { row: Pipeline }) => {
  const pipelineManagerQuery = usePipelineManagerQuery()
  const { data: pipelines } = useQuery({
    ...pipelineManagerQuery.pipelines()
  })
  const pipeline = params.row.descriptor
  const curProgramQuery = useQuery({
    ...pipelineManagerQuery.programs(),
    enabled: pipeline.program_name !== null,
    refetchInterval: 2000
  })

  const programStatus = (() => {
    if (pipeline.program_name === null) {
      return 'NoProgram' as const
    }
    if (curProgramQuery.isPending || curProgramQuery.isError) {
      return 'NotReady' as const
    }
    // Corresponding programData may not be found by name if a program name was changed,
    // and pipelines() and program() queries did not update simultaneously.
    // In that case, 'NoProgram' status will be shortly shown until all queries update to reflect the change.
    const programData = curProgramQuery.data.find(p => p.name === pipeline.program_name)
    if (!programData) {
      return 'NoProgram' as const
    }
    return match(programData.status)
      .with('Success', () => 'Ready' as const)
      .with('CompilingRust', () => 'CompilingRust' as const)
      .with('CompilingSql', () => 'NotReady' as const)
      .with('Pending', () => 'Pending' as const)
      .otherwise(() => 'Error' as const)
  })()

  // Row's pipeline will not be found within pipelines() query if it was just deleted
  const currentStatus =
    pipelines?.find(p => p.descriptor.pipeline_id === pipeline.pipeline_id)?.state.current_status ??
    PipelineStatus.UNKNOWN
  return tuple(currentStatus, programStatus)
}

const PipelineStatusCell = (params: GridRenderCellParams<Pipeline>) => {
  const [status, programStatus] = usePipelineStatus(params)

  const shutdownPipelineClick = usePipelineMutation(mutationShutdownPipeline)
  const testIdPrefix = `box-status-pipeline-${params.row.descriptor.name}-`
  const chipProps = match([status, programStatus])
    .returnType<ChipProps & { tooltip?: string }>()
    .with([PipelineStatus.UNKNOWN, P._], () => ({ label: status }))
    .with([PipelineStatus.SHUTDOWN, 'NotReady'], () => ({
      color: 'primary',
      label: 'Compiling',
      'data-testid': testIdPrefix + 'Compiling'
    }))
    .with([PipelineStatus.SHUTDOWN, 'Pending'], () => ({
      color: 'info',
      label: 'Queued',
      'data-testid': testIdPrefix + 'Queued'
    }))
    .with([PipelineStatus.SHUTDOWN, 'CompilingRust'], () => ({
      color: 'primary',
      label: 'Compiling bin',
      'data-testid': testIdPrefix + 'Compiling binary'
    }))
    .with([PipelineStatus.SHUTDOWN, 'Error'], () => ({
      color: 'error',
      label: 'Program err',
      'data-testid': testIdPrefix + 'Program error'
    }))
    .with([PipelineStatus.SHUTDOWN, 'NoProgram'], () => ({
      tooltip: 'A pipeline requires a program to be set in order to run. Please set one by editing the pipeline.',
      label: 'No program',
      'data-testid': testIdPrefix + 'No program'
    }))
    .with([PipelineStatus.SHUTDOWN, 'Ready'], () => ({
      label: status,
      'data-testid': testIdPrefix + status
    }))
    .with([PipelineStatus.INITIALIZING, P._], () => ({
      color: 'secondary',
      label: status,
      'data-testid': testIdPrefix + status
    }))
    .with([PipelineStatus.PROVISIONING, P._], () => ({
      color: 'secondary',
      label: status,
      'data-testid': testIdPrefix + status
    }))
    .with([PipelineStatus.CREATE_FAILURE, P._], () => ({
      color: 'error',
      label: status,
      'data-testid': testIdPrefix + status
    }))
    .with([PipelineStatus.STARTING, P._], () => ({
      color: 'secondary',
      label: status,
      'data-testid': testIdPrefix + status
    }))
    .with([PipelineStatus.STARTUP_FAILURE, P._], () => ({
      color: 'error',
      label: status,
      'data-testid': testIdPrefix + status
    }))
    .with([PipelineStatus.RUNNING, P._], () => ({
      color: 'success',
      label: status,
      'data-testid': testIdPrefix + status
    }))
    .with([PipelineStatus.PAUSING, P._], () => ({
      color: 'info',
      label: status,
      'data-testid': testIdPrefix + status
    }))
    .with([PipelineStatus.PAUSED, 'NotReady'], () => ({
      color: 'primary',
      label: 'Compiling',
      'data-testid': testIdPrefix + 'Compiling'
    }))
    .with([PipelineStatus.PAUSED, 'Pending'], () => ({
      color: 'info',
      label: 'Queued',
      'data-testid': testIdPrefix + 'Queued'
    }))
    .with([PipelineStatus.PAUSED, 'CompilingRust'], () => ({
      color: 'primary',
      label: 'Compiling bin',
      'data-testid': testIdPrefix + 'Compiling binary'
    }))
    .with([PipelineStatus.PAUSED, 'Error'], () => ({
      color: 'error',
      label: 'Program err',
      'data-testid': testIdPrefix + 'Program error'
    }))
    .with([PipelineStatus.PAUSED, P._], () => ({
      color: 'info',
      label: status,
      'data-testid': testIdPrefix + status
    }))
    .with([PipelineStatus.FAILED, P._], () => ({
      tooltip: params.row.state.error?.message || 'Unknown Error',
      color: 'error',
      label: status,
      onDelete: () => shutdownPipelineClick(params.row.descriptor.name),
      'data-testid': testIdPrefix + status
    }))
    .with([PipelineStatus.SHUTTING_DOWN, P._], () => ({
      color: 'secondary',
      label: status,
      'data-testid': testIdPrefix + status
    }))
    .exhaustive()

  return (
    <Badge badgeContent={(params.row as any).warn_cnt} color='warning'>
      <Badge badgeContent={(params.row as any).error_cnt} color='error'>
        {chipProps.tooltip ? (
          <Tooltip title={chipProps.tooltip} disableInteractive>
            <CustomChip rounded size='small' skin='light' sx={{ width: 125 }} {...chipProps} />
          </Tooltip>
        ) : (
          <CustomChip rounded size='small' skin='light' sx={{ width: 125 }} {...chipProps} />
        )}
      </Badge>
    </Badge>
  )
}

const PipelineActionsCell = (params: { row: Pipeline }) => {
  const pipeline = params.row.descriptor

  const [status, programStatus] = usePipelineStatus(params)

  const startPipelineClick = usePipelineMutation(mutationStartPipeline)
  const pausePipelineClick = usePipelineMutation(mutationPausePipeline)
  const shutdownPipelineClick = usePipelineMutation(mutationShutdownPipeline)
  const deletePipelineClick = usePipelineMutation(mutationDeletePipeline)

  const { showDeleteDialog } = useDeleteDialog()

  const actions = {
    pause: () => (
      <Tooltip title='Pause Pipeline' key='pause'>
        <IconButton
          className='pauseButton'
          size='small'
          onClick={() => pausePipelineClick(pipeline.name)}
          data-testid='button-pause'
        >
          <i className={`bx bx-pause-circle`} style={{ fontSize: 24 }} />
        </IconButton>
      </Tooltip>
    ),
    start: () => (
      <Tooltip title='Start Pipeline' key='start'>
        <IconButton size='small' onClick={() => startPipelineClick(pipeline.name)} data-testid='button-start'>
          <i className={`bx bx-play-circle`} style={{ fontSize: 24 }} />
        </IconButton>
      </Tooltip>
    ),
    spinner: () => (
      <Tooltip title={status} key='spinner'>
        <IconButton size='small'>
          <TwoSeventyRingWithBg fontSize={20} color={'currentColor'} />
        </IconButton>
      </Tooltip>
    ),
    shutdown: () => (
      <Tooltip title='Shutdown Pipeline' key='shutdown'>
        <IconButton
          className='shutdownButton'
          size='small'
          onClick={() => shutdownPipelineClick(pipeline.name)}
          data-testid='button-shutdown'
        >
          <i className={`bx bx-stop-circle`} style={{ fontSize: 24 }} />
        </IconButton>
      </Tooltip>
    ),
    inspect: () => (
      <Tooltip title='Inspect' key='inspect'>
        <IconButton size='small' component={Link} href='#' data-testid='button-inspect'>
          <i className={`bx bx-show`} style={{ fontSize: 24 }} />
        </IconButton>
      </Tooltip>
    ),
    edit: () => (
      <Tooltip title='Edit Pipeline' key='edit'>
        <IconButton
          className='editButton'
          size='small'
          href={`/streaming/builder/?pipeline_name=${pipeline.name}`}
          data-testid='button-edit'
        >
          <i className={`bx bx-pencil`} style={{ fontSize: 24 }} />
        </IconButton>
      </Tooltip>
    ),
    delete: () => (
      <Tooltip title='Delete Pipeline' key='delete'>
        <IconButton
          className='deleteButton'
          size='small'
          onClick={showDeleteDialog(
            'Delete',
            `${pipeline.name.replace(/^[Pp]ipeline\s+|\s+[Pp]ipeline$/, '')} pipeline`,
            () => deletePipelineClick(pipeline.name)
          )}
          data-testid='button-delete'
        >
          <i className={`bx bx-trash-alt`} style={{ fontSize: 24 }} />
        </IconButton>
      </Tooltip>
    ),
    spacer: () => (
      <IconButton size='small' sx={{ opacity: 0 }} disabled key='spacer'>
        <i className={`bx bx-stop-circle`} style={{ fontSize: 24 }} />
      </IconButton>
    )
  }

  const enabled = match([status, programStatus])
    .returnType<(keyof typeof actions)[]>()
    .with([PipelineStatus.SHUTDOWN, 'Ready'], () => ['start', 'edit', 'delete'])
    .with([PipelineStatus.SHUTDOWN, P._], () => ['spacer', 'edit', 'delete'])
    .with([PipelineStatus.PROVISIONING, P._], () => ['spinner', 'edit'])
    .with([PipelineStatus.INITIALIZING, P._], () => ['spinner', 'edit'])
    .with([PipelineStatus.STARTING, P._], () => ['spinner', 'edit'])
    .with([PipelineStatus.RUNNING, P._], () => ['pause', 'edit', 'shutdown'])
    .with([PipelineStatus.PAUSING, P._], () => ['spinner', 'edit'])
    .with([PipelineStatus.PAUSED, 'Ready'], () => ['start', 'edit', 'shutdown'])
    .with([PipelineStatus.SHUTTING_DOWN, P._], () => ['spinner', 'edit'])
    .with([PipelineStatus.FAILED, P._], () => ['spacer', 'edit', 'shutdown'])
    .otherwise(() => ['spacer', 'edit'])

  return <Box data-testid={`box-pipeline-actions-${pipeline.name}`}>{enabled.map(e => actions[e]())}</Box>
}
