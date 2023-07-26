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
  GridRenderCellParams
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
  ErrorResponse
} from 'src/types/manager'
import { useQuery } from '@tanstack/react-query'
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

interface ConnectorData {
  ac: AttachedConnector
  c: ConnectorDescr | undefined
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
      const connectors = pipelineRevisionQuery.data.connectors
      const attachedConnectors = pipelineRevisionQuery.data.pipeline.attached_connectors
      setInputs(
        attachedConnectors
          .filter(ac => ac.is_input)
          .map(ac => {
            return { ac, c: connectors.find(c => c.connector_id === ac.connector_id) }
          })
      )

      setOutputs(
        attachedConnectors
          .filter(ac => !ac.is_input)
          .map(ac => {
            return { ac, c: connectors.find(c => c.connector_id === ac.connector_id) }
          })
      )
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
              getRowId={(row: ConnectorData) => row.ac.name}
              columns={[
                {
                  field: 'name',
                  headerName: 'Input',
                  flex: 0.5,
                  valueGetter: params => params.row.c?.name || 'unknown'
                },
                {
                  field: 'config',
                  valueGetter: params => params.row.ac.config,
                  headerName: 'Into Table',
                  flex: 0.8
                },
                {
                  field: 'records',
                  headerName: 'Records',
                  flex: 0.15,
                  renderCell: params =>
                    format('.1s')(inputMetrics?.get(params.row.ac.config.trim())?.total_records || 0)
                },
                {
                  field: 'traffic',
                  headerName: 'Traffic',
                  flex: 0.15,
                  renderCell: params => humanSize(inputMetrics?.get(params.row.ac.config.trim())?.total_bytes || 0)
                },
                {
                  field: 'action',
                  headerName: 'Action',
                  flex: 0.15,
                  renderCell: params => (
                    <Tooltip title='Inspect Table'>
                      <IconButton
                        size='small'
                        onClick={e => {
                          e.preventDefault()
                          router.push('/streaming/inspection/' + descriptor.pipeline_id + '/' + params.row.ac.config)
                        }}
                      >
                        <Icon icon='bx:show' fontSize={20} />
                      </IconButton>
                    </Tooltip>
                  )
                }
              ]}
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
              getRowId={(row: ConnectorData) => row.ac.name}
              columns={[
                {
                  field: 'name',
                  headerName: 'Output',
                  flex: 0.5,
                  valueGetter: params => params.row.c?.name || 'unknown'
                },
                {
                  field: 'config',
                  valueGetter: params => params.row.ac.config,
                  headerName: 'From View',
                  flex: 0.8
                },
                {
                  field: 'records',
                  headerName: 'Records',
                  flex: 0.15,
                  renderCell: params =>
                    format('.1s')(outputMetrics?.get(params.row.ac.config.trim())?.transmitted_records || 0)
                },
                {
                  field: 'traffic',
                  headerName: 'Traffic',
                  flex: 0.15,
                  renderCell: params =>
                    humanSize(outputMetrics?.get(params.row.ac.config.trim())?.transmitted_bytes || 0)
                },
                {
                  field: 'action',
                  headerName: 'Action',
                  flex: 0.15,
                  renderCell: params => (
                    <Tooltip title='Inspect View'>
                      <IconButton
                        size='small'
                        onClick={e => {
                          e.preventDefault()
                          router.push('/streaming/inspection/' + descriptor.pipeline_id + '/' + params.row.ac.config)
                        }}
                      >
                        <Icon icon='bx:show' fontSize={20} />
                      </IconButton>
                    </Tooltip>
                  )
                }
              ]}
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
      valueGetter: params => params.row.descriptor.name
    },
    {
      field: 'description',
      headerName: 'Description',
      editable: true,
      flex: 3,
      valueGetter: params => params.row.descriptor.description
    },
    {
      field: 'modification',
      headerName: 'Changes',
      flex: 1,
      renderCell: (params: GridRenderCellParams) => {
        return <PipelineRevisionStatusChip pipeline={params.row.descriptor} />
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
        const needsShutdown = currentStatus === ClientPipelineStatus.PAUSED
        const needsDelete = currentStatus === ClientPipelineStatus.INACTIVE
        const needsEdit = currentStatus === ClientPipelineStatus.INACTIVE
        const needsInspect = currentStatus === ClientPipelineStatus.RUNNING
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
            {needsSpinner &&
              (pipelineStatus.get(params.row.descriptor.pipeline_id) === ClientPipelineStatus.STARTING ||
                pipelineStatus.get(params.row.descriptor.pipeline_id) === ClientPipelineStatus.PROVISIONING) && (
                <Tooltip title={pipelineStatus.get(params.row.descriptor.pipeline_id)}>
                  <IconButton size='small'>
                    <Icon icon='svg-spinners:270-ring-with-bg' fontSize={20} />
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
                    router.push('/streaming/builder/' + params.row.descriptor.pipeline_id)
                  }}
                >
                  <Icon icon='bx:pencil' fontSize={20} />
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
            {needsInspect && (
              <Tooltip title='Inspect'>
                <IconButton size='small' component={Link} href='#'>
                  <Icon icon='bx:show' fontSize={20} />
                </IconButton>
              </Tooltip>
            )}
          </>
        )
      }
    }
  ]

  return (
    <Card>
      <DataGridPro
        autoHeight
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
