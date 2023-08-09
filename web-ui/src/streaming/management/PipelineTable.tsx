// Table showing the list of pipelines for a user.
//
// The rows of the table can be expanded for even more details.

import { useCallback, useEffect, useState } from 'react'
import React from 'react'
import Link from 'next/link'
import Box from '@mui/material/Box'
import Card from '@mui/material/Card'
import Paper from '@mui/material/Paper'
import Grid from '@mui/material/Grid'
import {
  DataGridPro,
  DataGridProProps,
  GRID_DETAIL_PANEL_TOGGLE_COL_DEF,
  GridColDef,
  GridRenderCellParams,
  GridValueSetterParams,
  useGridApiRef
} from '@mui/x-data-grid-pro'
import CustomChip from 'src/@core/components/mui/chip'
import Badge from '@mui/material/Badge'
import IconButton from '@mui/material/IconButton'
import { Icon } from '@iconify/react'
import List from '@mui/material/List'
import ListItem from '@mui/material/ListItem'
import ListItemText from '@mui/material/ListItemText'
import ListItemIcon from '@mui/material/ListItemIcon'
import ListSubheader from '@mui/material/ListSubheader'
import Tooltip from '@mui/material/Tooltip'
import ExpandMoreIcon from '@mui/icons-material/ExpandMore'

import AnalyticsPipelineTput from 'src/streaming/management/AnalyticsPipelineTput'
import QuickSearchToolbar from 'src/components/table/QuickSearchToolbar'
import {
  AttachedConnector,
  Pipeline,
  ConnectorDescr,
  PipelineStatus,
  PipelineRevision,
  ErrorResponse,
  Relation,
  PipelinesService,
  UpdatePipelineResponse,
  ApiError,
  PipelineId,
  UpdatePipelineRequest
} from 'src/types/manager'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { escapeRegExp } from 'src/utils'
import { match } from 'ts-pattern'
import router from 'next/router'
import { ConnectorStatus, GlobalMetrics, InputConnectorMetrics, OutputConnectorMetrics } from 'src/types/pipeline'
import { humanSize } from 'src/utils'
import { format } from 'd3-format'
import { PipelineRevisionStatusChip } from 'src/streaming/management/RevisionStatus'
import { ClientPipelineStatus, usePipelineStateStore } from './StatusContext'
import usePausePipeline from './hooks/usePausePipeline'
import useShutdownPipeline from './hooks/useShutdownPipeline'
import useDeletePipeline from './hooks/useDeletePipeline'
import useStartPipeline from './hooks/useStartPipeline'
import useStatusNotification from 'src/components/errors/useStatusNotification'

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
    const connections: [AttachedConnector, ConnectorDescr][] = attachedConnectors
      .filter(ac => ac.relation_name === relation.name)
      .map(ac => {
        const connector = connectors.find(c => c.connector_id === ac?.connector_id)
        if (!connector) {
          // This can't happen in a revision
          throw Error('Attached connector has no connector.')
        }
        return [ac, connector] as [AttachedConnector, ConnectorDescr]
      })

    return { relation, connections }
  })
}

const DetailPanelContent = (props: { row: Pipeline }) => {
  const [inputs, setInputs] = useState<ConnectorData[]>([])
  const [outputs, setOutputs] = useState<ConnectorData[]>([])
  const { descriptor, state } = props.row

  const pipelineRevisionQuery = useQuery<PipelineRevision>([
    'pipelineLastRevision',
    { pipeline_id: descriptor.pipeline_id }
  ])
  useEffect(() => {
    if (!pipelineRevisionQuery.isLoading && !pipelineRevisionQuery.isError && pipelineRevisionQuery.data) {
      setInputs(getConnectorData(pipelineRevisionQuery.data, 'input'))
      setOutputs(getConnectorData(pipelineRevisionQuery.data, 'output'))
    }
  }, [
    pipelineRevisionQuery.isLoading,
    pipelineRevisionQuery.isError,
    pipelineRevisionQuery.data,
    setInputs,
    setOutputs
  ])

  const [globalMetrics, setGlobalMetrics] = useState<GlobalMetrics[]>([])
  const [inputMetrics, setInputMetrics] = useState<Map<string, InputConnectorMetrics>>(new Map())
  const [outputMetrics, setOutputMetrics] = useState<Map<string, OutputConnectorMetrics>>(new Map())
  const pipelineStatsQuery = useQuery<any>(['pipelineStats', { pipeline_id: descriptor.pipeline_id }], {
    enabled: state.current_status == PipelineStatus.RUNNING,
    refetchInterval: 1000
  })

  useEffect(() => {
    if (!pipelineStatsQuery.isLoading && !pipelineStatsQuery.isError) {
      const metrics = pipelineStatsQuery.data['global_metrics']
      setGlobalMetrics(oldMetrics => [...oldMetrics, metrics])

      const newInputMetrics = new Map<string, InputConnectorMetrics>()
      pipelineStatsQuery.data['inputs'].forEach((cs: ConnectorStatus) => {
        // @ts-ignore (config is untyped needs backend fix)
        newInputMetrics.set(cs.config['stream'], cs.metrics as InputConnectorMetrics)
      })
      setInputMetrics(newInputMetrics)

      const newOutputMetrics = new Map<string, OutputConnectorMetrics>()
      pipelineStatsQuery.data['outputs'].forEach((cs: ConnectorStatus) => {
        // @ts-ignore (config is untyped needs backend fix)
        newOutputMetrics.set(cs.config['stream'], cs.metrics as OutputConnectorMetrics)
      })
      setOutputMetrics(newOutputMetrics)
    }
    if (state.current_status == PipelineStatus.SHUTDOWN) {
      setGlobalMetrics([])
      setInputMetrics(new Map())
      setOutputMetrics(new Map())
    }
  }, [pipelineStatsQuery.isLoading, pipelineStatsQuery.isError, pipelineStatsQuery.data, state.current_status])

  function getRelationColumns(direction: InputOrOutput): GridColDef<ConnectorData>[] {
    return [
      {
        field: 'name',
        headerName: direction === 'input' ? 'Input' : 'Output',
        flex: 0.5,
        renderCell: params => {
          if (params.row.connections.length > 0) {
            return params.row.connections.map(c => c[1].name).join(', ')
          } else {
            return <Box sx={{ fontStyle: 'italic' }}>No connection.</Box>
          }
        }
      },
      {
        field: 'config',
        valueGetter: params => params.row.relation.name,
        headerName: direction === 'input' ? 'Table' : 'View',
        flex: 0.8
      },
      {
        field: 'records',
        headerName: 'Records',
        flex: 0.15,
        renderCell: params => {
          const records =
            direction === 'input'
              ? inputMetrics.get(params.row.relation.name)?.total_records
              : outputMetrics.get(params.row.relation.name)?.transmitted_records
          return format('.1s')(records || 0)
        }
      },
      {
        field: 'traffic',
        headerName: 'Traffic',
        flex: 0.15,
        renderCell: params => {
          const bytes =
            direction === 'input'
              ? inputMetrics.get(params.row.relation.name)?.total_bytes
              : outputMetrics.get(params.row.relation.name)?.transmitted_bytes
          return humanSize(bytes || 0)
        }
      },
      {
        field: 'action',
        headerName: 'Action',
        flex: 0.15,
        renderCell: params => (
          <>
            <Tooltip title={direction === 'input' ? 'Inspect Table' : 'Inspect View'}>
              <IconButton
                size='small'
                onClick={e => {
                  e.preventDefault()
                  router.push(
                    `/streaming/inspection?pipeline_id=${descriptor.pipeline_id}&relation=${params.row.relation.name}`
                  )
                }}
              >
                <Icon icon='bx:show' fontSize={20} />
              </IconButton>
            </Tooltip>
            {direction === 'input' && state.current_status == PipelineStatus.RUNNING && (
              <Tooltip title='Import Data'>
                <IconButton
                  size='small'
                  onClick={e => {
                    e.preventDefault()
                    router.push(
                      `/streaming/inspection?pipeline_id=${descriptor.pipeline_id}&relation=${params.row.relation.name}&tab=insert`
                    )
                  }}
                >
                  <Icon icon='bx:upload' fontSize={20} />
                </IconButton>
              </Tooltip>
            )}
          </>
        )
      }
    ]
  }

  return !pipelineRevisionQuery.isLoading && !pipelineRevisionQuery.isError ? (
    <Box display='flex' sx={{ m: 2 }} justifyContent='center'>
      <Grid container spacing={3} sx={{ height: 1, width: '95%' }} alignItems='stretch'>
        <Grid item xs={4}>
          <Card>
            <List subheader={<ListSubheader>Configuration</ListSubheader>}>
              <ListItem>
                <ListItemIcon>
                  <Icon icon='bi:filetype-sql' fontSize={20} />
                </ListItemIcon>
                <ListItemText primary={pipelineRevisionQuery.data?.program.name || 'not set'} />
              </ListItem>
              {state.current_status == PipelineStatus.RUNNING && (
                <>
                  <ListItem>
                    <Tooltip title='Pipeline Running Since'>
                      <ListItemIcon>
                        <Icon icon='clarity:date-line' fontSize={20} />
                      </ListItemIcon>
                    </Tooltip>
                    <ListItemText primary={state.created || 'Not running'} />
                  </ListItem>
                  <ListItem>
                    <Tooltip title='Pipeline Port'>
                      <ListItemIcon>
                        <Icon icon='carbon:port-input' fontSize={20} />
                      </ListItemIcon>
                    </Tooltip>
                    <ListItemText className='pipelinePort' primary={state.location || '0000'} />
                  </ListItem>
                </>
              )}
            </List>
          </Card>
        </Grid>

        <Grid item xs={8}>
          <Paper>
            <AnalyticsPipelineTput metrics={globalMetrics} />
          </Paper>
        </Grid>

        <Grid item xs={12}>
          {/* className referenced by webui-tester */}
          <Paper className='inputStats'>
            <DataGridPro
              autoHeight
              getRowId={(row: ConnectorData) => row.relation.name}
              columns={getRelationColumns('input')}
              rows={inputs}
              sx={{ flex: 1 }}
              hideFooter
            />
          </Paper>
        </Grid>

        <Grid item xs={12}>
          {/* className referenced by webui-tester */}
          <Paper className='outputStats'>
            <DataGridPro
              autoHeight
              getRowId={(row: ConnectorData) => row.relation.name}
              columns={getRelationColumns('output')}
              rows={outputs}
              sx={{ flex: 1 }}
              hideFooter
            />
          </Paper>
        </Grid>
      </Grid>
    </Box>
  ) : (
    <Box>Loading...</Box>
  )
}

const statusToChip = (
  status: ClientPipelineStatus | undefined,
  error: ErrorResponse | null,
  onDelete: ((event: any) => void) | undefined
) => {
  return match(status)
    .with(undefined, () => <CustomChip rounded size='small' skin='light' label='Unknown' />)
    .with(ClientPipelineStatus.UNKNOWN, () => <CustomChip rounded size='small' skin='light' label={status} />)
    .with(ClientPipelineStatus.INACTIVE, () => <CustomChip rounded size='small' skin='light' label={status} />)
    .with(ClientPipelineStatus.INITIALIZING, () => (
      <CustomChip rounded size='small' skin='light' color='secondary' label={status} />
    ))
    .with(ClientPipelineStatus.PROVISIONING, () => (
      <CustomChip rounded size='small' skin='light' color='secondary' label={status} />
    ))
    .with(ClientPipelineStatus.CREATE_FAILURE, () => (
      <CustomChip rounded size='small' skin='light' color='error' label={status} />
    ))
    .with(ClientPipelineStatus.STARTING, () => (
      <CustomChip rounded size='small' skin='light' color='secondary' label={status} />
    ))
    .with(ClientPipelineStatus.STARTUP_FAILURE, () => (
      <CustomChip rounded size='small' skin='light' color='error' label={status} />
    ))
    .with(ClientPipelineStatus.RUNNING, () => (
      <CustomChip rounded size='small' skin='light' color='success' label={status} />
    ))
    .with(ClientPipelineStatus.PAUSING, () => (
      <CustomChip rounded size='small' skin='light' color='info' label={status} />
    ))
    .with(ClientPipelineStatus.PAUSED, () => (
      <CustomChip rounded size='small' skin='light' color='info' label={status} />
    ))
    .with(ClientPipelineStatus.FAILED, () => (
      <Tooltip title={error?.message || 'Unknown Error'} disableInteractive>
        <CustomChip rounded size='small' skin='light' color='error' label={status} onDelete={onDelete} />
      </Tooltip>
    ))
    .with(ClientPipelineStatus.SHUTTING_DOWN, () => (
      <CustomChip rounded size='small' skin='light' color='secondary' label={status} />
    ))
    .exhaustive()
}

const pipelineStatusToClientStatus = (status: PipelineStatus) => {
  return match(status)
    .with(PipelineStatus.SHUTDOWN, () => ClientPipelineStatus.INACTIVE)
    .with(PipelineStatus.PROVISIONING, () => ClientPipelineStatus.PROVISIONING)
    .with(PipelineStatus.INITIALIZING, () => ClientPipelineStatus.INITIALIZING)
    .with(PipelineStatus.PAUSED, () => ClientPipelineStatus.PAUSED)
    .with(PipelineStatus.RUNNING, () => ClientPipelineStatus.RUNNING)
    .with(PipelineStatus.SHUTTING_DOWN, () => ClientPipelineStatus.SHUTTING_DOWN)
    .with(PipelineStatus.FAILED, () => ClientPipelineStatus.FAILED)
    .exhaustive()
}

export default function PipelineTable() {
  const [searchText, setSearchText] = useState<string>('')
  const [rows, setRows] = useState<Pipeline[]>([])
  const [filteredData, setFilteredData] = useState<Pipeline[]>([])
  const pipelineStatus = usePipelineStateStore(state => state.clientStatus)
  const setPipelineStatus = usePipelineStateStore(state => state.setStatus)
  const [paginationModel, setPaginationModel] = useState({
    pageSize: 7,
    page: 0
  })

  const startPipelineClick = useStartPipeline()
  const pausePipelineClick = usePausePipeline()
  const shutdownPipelineClick = useShutdownPipeline()
  const deletePipelineClick = useDeletePipeline()

  const { isLoading, isError, data, error } = useQuery<Pipeline[]>(['pipeline'], {
    refetchInterval: 2000
  })
  useEffect(() => {
    if (!isLoading && !isError) {
      setRows(data)
      for (const { descriptor, state } of data) {
        // If we're not in the desired status, we know better what to display as
        // status in the client (something pending), so we don't reset it until
        // desired status is reached and rely on whatever we set with
        // `setPipelineStatus` in the start/stop/pause hooks.
        if (state.current_status == state.desired_status || state.current_status == PipelineStatus.FAILED) {
          setPipelineStatus(descriptor.pipeline_id, pipelineStatusToClientStatus(state.current_status))
        }
      }
    }
    if (isError) {
      throw error
    }
  }, [isLoading, isError, data, setRows, setPipelineStatus, error])

  const getDetailPanelContent = useCallback<NonNullable<DataGridProProps['getDetailPanelContent']>>(
    ({ row }) => <DetailPanelContent row={row} />,
    []
  )

  const handleSearch = (searchValue: string) => {
    setSearchText(searchValue)
    const searchRegex = new RegExp(escapeRegExp(searchValue), 'i')
    if (!isLoading && !isError) {
      const filteredRows = data.filter((row: any) => {
        return Object.keys(row).some(field => {
          // @ts-ignore
          if (row[field] !== null) {
            return searchRegex.test(row[field].toString())
          }
        })
      })
      if (searchValue.length) {
        setFilteredData(filteredRows)
      } else {
        setFilteredData([])
      }
    }
  }

  // Only show the details tab button if this pipeline has a revision
  function CustomDetailPanelToggle(props: Pick<GridRenderCellParams, 'id' | 'value' | 'row'>) {
    const { value: isExpanded, row: row } = props
    const [hasRevision, setHasRevision] = useState<boolean>(false)

    const pipelineRevisionQuery = useQuery<PipelineRevision | null>([
      'pipelineLastRevision',
      { pipeline_id: props.row.descriptor.pipeline_id }
    ])
    useEffect(() => {
      if (!pipelineRevisionQuery.isLoading && !pipelineRevisionQuery.isError && pipelineRevisionQuery.data != null) {
        setHasRevision(true)
      }
    }, [pipelineRevisionQuery.isLoading, pipelineRevisionQuery.isError, pipelineRevisionQuery.data])

    return (isExpanded ||
      row.state.current_status === PipelineStatus.RUNNING ||
      row.state.current_status === PipelineStatus.PAUSED) &&
      hasRevision ? (
      <IconButton size='small' tabIndex={-1} aria-label={isExpanded ? 'Close' : 'Open'}>
        <ExpandMoreIcon
          sx={{
            transform: `rotateZ(${isExpanded ? 180 : 0}deg)`,
            transition: theme =>
              theme.transitions.create('transform', {
                duration: theme.transitions.duration.shortest
              })
          }}
          fontSize='inherit'
        />
      </IconButton>
    ) : (
      <></>
    )
  }

  const columns: GridColDef[] = [
    {
      ...GRID_DETAIL_PANEL_TOGGLE_COL_DEF,
      renderCell: params => <CustomDetailPanelToggle id={params.id} value={params.value} row={params.row} />
    },
    {
      field: 'name',
      headerName: 'Name',
      editable: true,
      flex: 2,
      valueGetter: params => params.row.descriptor.name,
      valueSetter: (params: GridValueSetterParams) => {
        params.row.descriptor.name = params.value
        return params.row
      }
    },
    {
      field: 'description',
      headerName: 'Description',
      editable: true,
      flex: 3,
      valueGetter: params => params.row.descriptor.description,
      valueSetter: (params: GridValueSetterParams) => {
        params.row.descriptor.description = params.value
        return params.row
      }
    },
    {
      field: 'modification',
      headerName: 'Changes',
      flex: 1,
      renderCell: (params: GridRenderCellParams) => {
        return <PipelineRevisionStatusChip pipeline={params.row} />
      }
    },
    {
      field: 'status',
      headerName: 'Status',
      flex: 1,
      renderCell: (params: GridRenderCellParams) => {
        const status = statusToChip(pipelineStatus.get(params.row.descriptor.pipeline_id), params.row.state.error, () =>
          shutdownPipelineClick(params.row.descriptor.pipeline_id)
        )
        return (
          <Badge badgeContent={params.row.warn_cnt} color='warning'>
            <Badge badgeContent={params.row.error_cnt} color='error'>
              {status}
            </Badge>
          </Badge>
        )
      }
    },
    {
      field: 'actions',
      headerName: 'Actions',
      flex: 1.0,
      renderCell: (params: GridRenderCellParams) => {
        const { descriptor } = params.row
        const currentStatus = pipelineStatus.get(descriptor.pipeline_id)
        const needsPause = currentStatus === ClientPipelineStatus.RUNNING
        const needsStart =
          currentStatus === ClientPipelineStatus.INACTIVE || currentStatus === ClientPipelineStatus.PAUSED
        const needsShutdown =
          currentStatus === ClientPipelineStatus.PAUSED || currentStatus === ClientPipelineStatus.RUNNING
        const needsDelete = currentStatus === ClientPipelineStatus.INACTIVE
        const needsEdit = true
        const needsInspect = false //currentStatus === ClientPipelineStatus.RUNNING
        const needsSpinner =
          currentStatus === ClientPipelineStatus.PROVISIONING ||
          currentStatus === ClientPipelineStatus.INITIALIZING ||
          currentStatus === ClientPipelineStatus.STARTING ||
          currentStatus === ClientPipelineStatus.PAUSING ||
          currentStatus === ClientPipelineStatus.SHUTTING_DOWN

        return (
          <>
            {/* the className attributes are used by webui-tester */}
            {needsPause && (
              <Tooltip title='Pause Pipeline'>
                <IconButton
                  className='pauseButton'
                  size='small'
                  onClick={() => pausePipelineClick(params.row.descriptor.pipeline_id)}
                >
                  <Icon icon='bx:pause-circle' fontSize={20} />
                </IconButton>
              </Tooltip>
            )}
            {needsStart && (
              <Tooltip title='Start Pipeline'>
                <IconButton
                  className='startButton'
                  size='small'
                  onClick={() => startPipelineClick(params.row.descriptor.pipeline_id)}
                >
                  <Icon icon='bx:play-circle' fontSize={20} />
                </IconButton>
              </Tooltip>
            )}
            {needsSpinner && (
              <Tooltip title={pipelineStatus.get(params.row.descriptor.pipeline_id)}>
                <IconButton size='small'>
                  <Icon icon='svg-spinners:270-ring-with-bg' fontSize={20} />
                </IconButton>
              </Tooltip>
            )}
            {needsShutdown && (
              <Tooltip title='Shutdown Pipeline'>
                <IconButton
                  className='shutdownButton'
                  size='small'
                  onClick={() => shutdownPipelineClick(params.row.descriptor.pipeline_id)}
                >
                  <Icon icon='bx:stop-circle' fontSize={20} />
                </IconButton>
              </Tooltip>
            )}
            {needsInspect && (
              <Tooltip title='Inspect'>
                <IconButton size='small' component={Link} href='#'>
                  <Icon icon='bx:show' fontSize={20} />
                </IconButton>
              </Tooltip>
            )}
            {needsEdit && (
              <Tooltip title='Edit Pipeline'>
                <IconButton
                  className='editButton'
                  size='small'
                  href='#'
                  onClick={e => {
                    e.preventDefault()
                    router.push(`/streaming/builder/?pipeline_id=${params.row.descriptor.pipeline_id}`)
                  }}
                >
                  <Icon icon='bx:pencil' fontSize={20} />
                </IconButton>
              </Tooltip>
            )}
            {needsDelete && (
              <Tooltip title='Delete Pipeline'>
                <IconButton
                  className='deleteButton'
                  size='small'
                  onClick={() => deletePipelineClick(params.row.descriptor.pipeline_id)}
                >
                  <Icon icon='bx:trash-alt' fontSize={20} />
                </IconButton>
              </Tooltip>
            )}
          </>
        )
      }
    }
  ]

  // Makes sure we can edit name and description in the table
  const apiRef = useGridApiRef()
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()
  const mutation = useMutation<
    UpdatePipelineResponse,
    ApiError,
    { pipeline_id: PipelineId; request: UpdatePipelineRequest }
  >(args => {
    return PipelinesService.updatePipeline(args.pipeline_id, args.request)
  })
  const onUpdateRow = (newRow: Pipeline, oldRow: Pipeline) => {
    console.log('onUpdateRow;;', {
      name: newRow.descriptor.name,
      description: newRow.descriptor.description,
      program_id: newRow.descriptor.program_id
    })
    mutation.mutate(
      {
        pipeline_id: newRow.descriptor.pipeline_id,
        request: {
          name: newRow.descriptor.name,
          description: newRow.descriptor.description,
          program_id: newRow.descriptor.program_id
        }
      },
      {
        onError: (error: ApiError) => {
          queryClient.invalidateQueries(['pipeline'])
          queryClient.invalidateQueries(['pipelineStatus', { program_id: newRow.descriptor.pipeline_id }])
          pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
          apiRef.current.updateRows([oldRow])
        }
      }
    )

    return newRow
  }

  return (
    <Card>
      <DataGridPro
        autoHeight
        apiRef={apiRef}
        getRowId={(row: Pipeline) => row.descriptor.pipeline_id}
        columns={columns}
        rowThreshold={0}
        getDetailPanelHeight={() => 'auto'}
        getDetailPanelContent={getDetailPanelContent}
        components={{
          Toolbar: QuickSearchToolbar
        }}
        rows={filteredData.length ? filteredData : rows}
        pageSizeOptions={[7, 10, 25, 50]}
        paginationModel={paginationModel}
        onPaginationModelChange={setPaginationModel}
        processRowUpdate={onUpdateRow}
        loading={isLoading}
        componentsProps={{
          baseButton: {
            variant: 'outlined'
          },
          toolbar: {
            hasSearch: true,
            value: searchText,
            clearSearch: () => handleSearch(''),
            onChange: (event: React.ChangeEvent<HTMLInputElement>) => handleSearch(event.target.value)
          }
        }}
      />
    </Card>
  )
}
