// Table showing the list of pipelines for a user.
//
// The rows of the table can be expanded for even more details.

import { useCallback, useEffect, useState } from 'react'
import Link from 'next/link'
import Box from '@mui/material/Box'
import Card from '@mui/material/Card'
import Paper from '@mui/material/Paper'
import Grid from '@mui/material/Grid'
import { DataGridPro, DataGridProProps, GridColumns, GridRenderCellParams } from '@mui/x-data-grid-pro'
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

import AnalyticsPipelineTput from 'src/streaming/AnalyticsPipelineTput'
import QuickSearchToolbar from 'src/components/table/QuickSearchToolbar'
import {
  AttachedConnector,
  CancelError,
  PipelineDescr,
  ConnectorDescr,
  PipelineService,
  ProgramDescr,
  PipelineStatus
} from 'src/types/manager'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ErrorOverlay } from 'src/components/table/ErrorOverlay'
import { escapeRegExp } from 'src/utils'
import { match } from 'ts-pattern'
import router from 'next/router'
import useStatusNotification from 'src/components/errors/useStatusNotification'
import { ConnectorStatus, GlobalMetrics, InputConnectorMetrics, OutputConnectorMetrics } from 'src/types/pipeline'
import { humanSize } from 'src/utils'
import { format } from 'd3-format'

interface ConnectorData {
  ac: AttachedConnector
  c: ConnectorDescr | undefined
}

const DetailPanelContent = (props: { row: PipelineDescr }) => {
  const [inputs, setInputs] = useState<ConnectorData[]>([])
  const [outputs, setOutputs] = useState<ConnectorData[]>([])
  const projectQuery = useQuery<ProgramDescr>(['programStatus', { program_id: props.row.program_id }], {
    enabled: props.row.program_id !== undefined
  })

  const connectorQuery = useQuery<ConnectorDescr[]>(['connector'])
  useEffect(() => {
    if (!connectorQuery.isLoading && !connectorQuery.isError) {
      setInputs(
        props.row.attached_connectors
          .filter(ac => ac.is_input)
          .map(ac => {
            return { ac, c: connectorQuery.data.find(c => c.connector_id === ac.connector_id) }
          })
      )

      setOutputs(
        props.row.attached_connectors
          .filter(ac => !ac.is_input)
          .map(ac => {
            return { ac, c: connectorQuery.data.find(c => c.connector_id === ac.connector_id) }
          })
      )
    }
  }, [
    connectorQuery.isLoading,
    connectorQuery.isError,
    connectorQuery.data,
    setInputs,
    setOutputs,
    props.row.attached_connectors
  ])

  const [globalMetrics, setGlobalMetrics] = useState<GlobalMetrics[]>([])
  const [inputMetrics, setInputMetrics] = useState<Map<string, InputConnectorMetrics>>(new Map())
  const [outputMetrics, setOutputMetrics] = useState<Map<string, OutputConnectorMetrics>>(new Map())
  const pipelineStatsQuery = useQuery<any>(['pipelineStats', { pipeline_id: props.row.pipeline_id }], {
    enabled: props.row.status == PipelineStatus.RUNNING,
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
    if (props.row.status == PipelineStatus.RUNNING) {
      setGlobalMetrics([])
      setInputMetrics(new Map())
      setOutputMetrics(new Map())
    }
  }, [pipelineStatsQuery.isLoading, pipelineStatsQuery.isError, pipelineStatsQuery.data, props.row.status])

  return !connectorQuery.isLoading && !connectorQuery.isError && !projectQuery.isLoading && !projectQuery.isError ? (
    <Box display='flex' sx={{ m: 2 }} justifyContent='center'>
      <Grid container spacing={3} sx={{ height: 1, width: '95%' }} alignItems='stretch'>
        <Grid item xs={4}>
          <Card>
            <List subheader={<ListSubheader>Configuration</ListSubheader>}>
              <ListItem>
                <ListItemIcon>
                  <Icon icon='bi:filetype-sql' fontSize={20} />
                </ListItemIcon>
                <ListItemText primary={projectQuery.data?.name || 'not set'} />
              </ListItem>
              {props.row.status == PipelineStatus.RUNNING && (
                <>
                  <ListItem>
                    <Tooltip title='Pipeline Running Since'>
                      <ListItemIcon>
                        <Icon icon='clarity:date-line' fontSize={20} />
                      </ListItemIcon>
                    </Tooltip>
                    <ListItemText primary={props.row.created || 'Not running'} />
                  </ListItem>
                  <ListItem>
                    <Tooltip title='Pipeline Port'>
                      <ListItemIcon>
                        <Icon icon='carbon:port-input' fontSize={20} />
                      </ListItemIcon>
                    </Tooltip>
                    <ListItemText className='pipelinePort' primary={props.row.port || '0000'} />
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
                  headerName: 'Feeds Table',
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
                    <>
                      <Tooltip title='Inspect Connector'>
                        <IconButton
                          size='small'
                          onClick={e => {
                            e.preventDefault()
                            router.push(
                              '/streaming/introspection/' + props.row.pipeline_id + '/' + params.row.ac.config
                            )
                          }}
                        >
                          <Icon icon='bx:show' fontSize={20} />
                        </IconButton>
                      </Tooltip>
                      <Tooltip title='Remove Connector from the Pipeline'>
                        <IconButton size='small' href='#'>
                          <Icon icon='bx:trash-alt' fontSize={20} />
                        </IconButton>
                      </Tooltip>
                    </>
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
                //{
                //  field: 'enabled',
                //  headerName: 'Active',
                //  flex: 0.15,
                //  renderCell: () => <Switch defaultChecked disabled />
                //},
                {
                  field: 'action',
                  headerName: 'Action',
                  flex: 0.15,
                  renderCell: params => (
                    <>
                      <Tooltip title='Inspect Connector'>
                        <IconButton
                          size='small'
                          onClick={e => {
                            e.preventDefault()
                            router.push(
                              '/streaming/introspection/' + props.row.pipeline_id + '/' + params.row.ac.config
                            )
                          }}
                        >
                          <Icon icon='bx:show' fontSize={20} />
                        </IconButton>
                      </Tooltip>
                      <Tooltip title='Remove Connector from the Pipeline'>
                        <IconButton size='small' href='#'>
                          <Icon icon='bx:trash-alt' fontSize={20} />
                        </IconButton>
                      </Tooltip>
                    </>
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

enum ClientPipelineStatus {
  UNUSED = 'Unused',
  CREATING = 'Creating ...',
  CREATE_FAILURE = 'Failed to create',
  STARTING = 'Starting ...',
  STARTUP_FAILURE = 'Failed to start',
  RUNNING = 'Running',
  PAUSING = 'Pausing ...',
  PAUSED = 'Paused',
  ERROR = 'Error',
  SHUTTING_DOWN = 'Shutting down ...'
}

const statusToChip = (status: ClientPipelineStatus | undefined) => {
  return match(status)
    .with(undefined, () => <CustomChip rounded size='small' skin='light' label='Unused' />)
    .with(ClientPipelineStatus.UNUSED, () => <CustomChip rounded size='small' skin='light' label={status} />)
    .with(ClientPipelineStatus.CREATING, () => (
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
    .with(ClientPipelineStatus.ERROR, () => (
      <CustomChip rounded size='small' skin='light' color='error' label={status} />
    ))
    .with(ClientPipelineStatus.SHUTTING_DOWN, () => (
      <CustomChip rounded size='small' skin='light' color='secondary' label={status} />
    ))
    .exhaustive()
}

type PipelineAction = {
  command: 'deploy' | 'pause' | 'start' | 'shutdown'
  pipeline_id: string
}

export default function PipelineTable() {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()

  const [pageSize, setPageSize] = useState<number>(7)
  const [searchText, setSearchText] = useState<string>('')
  const [rows, setRows] = useState<PipelineDescr[]>([])
  const [filteredData, setFilteredData] = useState<PipelineDescr[]>([])
  const [isLaunching, setIsLaunching] = useState(new Map<string, ClientPipelineStatus>())

  const { isLoading, isError, data, error } = useQuery<PipelineDescr[]>(['pipeline'])
  useEffect(() => {
    if (!isLoading && !isError) {
      setRows(data)
    }
  }, [isLoading, isError, data, setRows])

  const { mutate: piplineAction, isLoading: pipelineActionLoading } = useMutation<string, CancelError, PipelineAction>({
    mutationFn: (action: PipelineAction) => {
      return PipelineService.pipelineAction(action.pipeline_id, action.command)
    }
  })

  const startPipelineClick = useCallback(
    (curRow: PipelineDescr) => {
      if (!pipelineActionLoading && curRow.status != PipelineStatus.RUNNING) {
        setIsLaunching(map => new Map(map.set(curRow.pipeline_id, ClientPipelineStatus.CREATING)))
        piplineAction(
          {
            pipeline_id: curRow.pipeline_id,
            command: 'deploy' as const
          },
          {
            onSuccess: () => {
              setIsLaunching(map => new Map(map.set(curRow.pipeline_id, ClientPipelineStatus.STARTING)))
              piplineAction(
                {
                  pipeline_id: curRow.pipeline_id,
                  command: 'start' as const
                },
                {
                  onSettled: () => {
                    queryClient.invalidateQueries(['configs'])
                    queryClient.invalidateQueries(['pipelineStatus', { pipeline_id: curRow.pipeline_id }])
                  },
                  onSuccess: () => {
                    setIsLaunching(map => new Map(map.set(curRow.pipeline_id, ClientPipelineStatus.RUNNING)))
                  },
                  onError: error => {
                    pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
                    setIsLaunching(map => new Map(map.set(curRow.pipeline_id, ClientPipelineStatus.STARTUP_FAILURE)))
                  }
                }
              )
            },
            onSettled: () => {
              queryClient.invalidateQueries(['pipeline'])
              queryClient.invalidateQueries(['pipelineStatus', { pipeline_id: curRow.pipeline_id }])
            },
            onError: error => {
              pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
              setIsLaunching(map => new Map(map.set(curRow.pipeline_id, ClientPipelineStatus.CREATE_FAILURE)))
            }
          }
        )
      } else if (!pipelineActionLoading && curRow.status != PipelineStatus.SHUTDOWN) {
        // Pipeline already exists, we just need to start it
        setIsLaunching(map => new Map(map.set(curRow.pipeline_id, ClientPipelineStatus.STARTING)))
        piplineAction(
          {
            pipeline_id: curRow.pipeline_id,
            command: 'start' as const
          },
          {
            onSettled: () => {
              queryClient.invalidateQueries(['pipeline'])
              queryClient.invalidateQueries(['pipelineStatus', { pipeline_id: curRow.pipeline_id }])
            },
            onSuccess: () => {
              setIsLaunching(map => new Map(map.set(curRow.pipeline_id, ClientPipelineStatus.RUNNING)))
            },
            onError: error => {
              pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
              setIsLaunching(map => new Map(map.set(curRow.pipeline_id, ClientPipelineStatus.STARTUP_FAILURE)))
            }
          }
        )
      }
    },
    [piplineAction, pipelineActionLoading, queryClient, pushMessage, setIsLaunching]
  )

  const pausePipelineClick = useCallback(
    (curRow: PipelineDescr) => {
      if (!pipelineActionLoading && curRow.status == PipelineStatus.RUNNING) {
        setIsLaunching(map => new Map(map.set(curRow.pipeline_id, ClientPipelineStatus.PAUSING)))
        piplineAction(
          { pipeline_id: curRow.pipeline_id, command: 'pause' },
          {
            onSettled: () => {
              queryClient.invalidateQueries(['pipeline'])
              queryClient.invalidateQueries(['pipelineStatus', { pipeline_id: curRow.pipeline_id }])
            },
            onSuccess: () => {
              setIsLaunching(map => new Map(map.set(curRow.pipeline_id, ClientPipelineStatus.PAUSED)))
            },
            onError: error => {
              pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
              setIsLaunching(map => new Map(map.set(curRow.pipeline_id, ClientPipelineStatus.RUNNING)))
            }
          }
        )
      }
    },
    [pipelineActionLoading, pushMessage, queryClient, piplineAction]
  )

  const shutdownPipelineClick = useCallback(
    (curRow: PipelineDescr) => {
      if (!pipelineActionLoading && curRow.status !== PipelineStatus.RUNNING) {
        setIsLaunching(map => new Map(map.set(curRow.pipeline_id, ClientPipelineStatus.SHUTTING_DOWN)))
        piplineAction(
          { pipeline_id: curRow.pipeline_id, command: 'shutdown' as const },
          {
            onSettled: () => {
              queryClient.invalidateQueries(['pipeline'])
              queryClient.invalidateQueries(['pipelineStatus', { pipeline_id: curRow.pipeline_id }])
            },
            onSuccess: () => {
              setIsLaunching(map => new Map(map.set(curRow.pipeline_id, ClientPipelineStatus.UNUSED)))
              queryClient.setQueryData(['pipeline'], (oldData: PipelineDescr[] | undefined) =>
                oldData?.map((config: PipelineDescr) => {
                  if (config.pipeline_id === curRow.pipeline_id) {
                    return { ...config, status: PipelineStatus.SHUTDOWN }
                  } else {
                    return config
                  }
                })
              )
            },
            onError: error => {
              pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
              setIsLaunching(map => new Map(map.set(curRow.pipeline_id, ClientPipelineStatus.RUNNING)))
            }
          }
        )
      }
    },
    [pipelineActionLoading, pushMessage, queryClient, piplineAction]
  )

  const { mutate: deletePipeline, isLoading: deletePipelineLoading } = useMutation<string, CancelError, string>(
    PipelineService.pipelineDelete
  )
  const deletePipelineClick = useCallback(
    (curRow: PipelineDescr) => {
      if (!deletePipelineLoading) {
        setIsLaunching(map => new Map(map.set(curRow.pipeline_id, ClientPipelineStatus.SHUTTING_DOWN)))
        deletePipeline(curRow.pipeline_id, {
          onSettled: () => {
            queryClient.invalidateQueries(['pipeline'])
            queryClient.invalidateQueries(['pipelineStatus', { pipeline_id: curRow.pipeline_id }])
          },
          onSuccess: () => {
            setIsLaunching(map => new Map(map.set(curRow.pipeline_id, ClientPipelineStatus.UNUSED)))
            queryClient.setQueryData(['pipeline'], (oldData: PipelineDescr[] | undefined) =>
              oldData?.map((config: PipelineDescr) => {
                if (config.pipeline_id === curRow.pipeline_id) {
                  return { ...config, status: PipelineStatus.SHUTDOWN }
                } else {
                  return config
                }
              })
            )
          },
          onError: error => {
            pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
            setIsLaunching(map => new Map(map.set(curRow.pipeline_id, ClientPipelineStatus.RUNNING)))
          }
        })
      }
    },
    [deletePipelineLoading, pushMessage, queryClient, deletePipeline]
  )

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

  const columns: GridColumns = [
    { field: 'name', headerName: 'Name', editable: true, flex: 2 },
    { field: 'description', headerName: 'Description', editable: true, flex: 3 },
    {
      field: 'status',
      headerName: 'Status',
      flex: 1,
      renderCell: (params: GridRenderCellParams) => {
        const status = statusToChip(isLaunching.get(params.row.pipeline_id))

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
        const needsPause = isLaunching.get(params.row.pipeline_id) === ClientPipelineStatus.RUNNING
        const needsStart =
          !isLaunching.has(params.row.pipeline_id) ||
          (isLaunching.get(params.row.pipeline_id) !== ClientPipelineStatus.RUNNING &&
            isLaunching.get(params.row.pipeline_id) !== ClientPipelineStatus.STARTING &&
            isLaunching.get(params.row.pipeline_id) !== ClientPipelineStatus.CREATING &&
            isLaunching.get(params.row.pipeline_id) !== ClientPipelineStatus.PAUSING)
        const needsEdit =
          !isLaunching.has(params.row.pipeline_id) ||
          (isLaunching.get(params.row.pipeline_id) !== ClientPipelineStatus.RUNNING &&
            isLaunching.get(params.row.pipeline_id) !== ClientPipelineStatus.STARTING &&
            isLaunching.get(params.row.pipeline_id) !== ClientPipelineStatus.CREATING &&
            isLaunching.get(params.row.pipeline_id) !== ClientPipelineStatus.PAUSING)
        const needsShutdown =
          isLaunching.get(params.row.pipeline_id) !== ClientPipelineStatus.RUNNING &&
          isLaunching.get(params.row.pipeline_id) !== ClientPipelineStatus.STARTING &&
          isLaunching.get(params.row.pipeline_id) !== ClientPipelineStatus.CREATING &&
          isLaunching.get(params.row.pipeline_id) !== ClientPipelineStatus.PAUSING
        const needsDelete =
          isLaunching.get(params.row.pipeline_id) !== ClientPipelineStatus.RUNNING &&
          isLaunching.get(params.row.pipeline_id) !== ClientPipelineStatus.STARTING &&
          isLaunching.get(params.row.pipeline_id) !== ClientPipelineStatus.CREATING &&
          isLaunching.get(params.row.pipeline_id) !== ClientPipelineStatus.PAUSING
        const needsInspect = isLaunching.get(params.row.pipeline_id) === ClientPipelineStatus.RUNNING
        const needsSpinner =
          isLaunching.get(params.row.pipeline_id) === ClientPipelineStatus.STARTING ||
          isLaunching.get(params.row.pipeline_id) === ClientPipelineStatus.PAUSING ||
          isLaunching.get(params.row.pipeline_id) === ClientPipelineStatus.CREATING

        return (
          <>
            {/* the className attributes are used by webui-tester */}
            {needsPause && (
              <Tooltip title='Pause Pipeline'>
                <IconButton className='pauseButton' size='small' onClick={() => pausePipelineClick(params.row)}>
                  <Icon icon='bx:pause-circle' fontSize={20} />
                </IconButton>
              </Tooltip>
            )}
            {needsStart && (
              <Tooltip title='Start Pipeline'>
                <IconButton className='startButton' size='small' onClick={() => startPipelineClick(params.row)}>
                  <Icon icon='bx:play-circle' fontSize={20} />
                </IconButton>
              </Tooltip>
            )}
            {needsSpinner &&
              (isLaunching.get(params.row.pipeline_id) === ClientPipelineStatus.STARTING ||
                isLaunching.get(params.row.pipeline_id) === ClientPipelineStatus.CREATING) && (
                <Tooltip title={isLaunching.get(params.row.pipeline_id)}>
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
                    router.push('/streaming/builder/' + params.row.pipeline_id)
                  }}
                >
                  <Icon icon='bx:pencil' fontSize={20} />
                </IconButton>
              </Tooltip>
            )}
            {needsShutdown && (
              <Tooltip title='Shutdown Pipeline'>
                <IconButton className='shutdownButton' size='small' onClick={() => shutdownPipelineClick(params.row)}>
                  <Icon icon='bx:stop-circle' fontSize={20} />
                </IconButton>
              </Tooltip>
            )}
            {needsDelete && (
              <Tooltip title='Delete Pipeline'>
                <IconButton className='deleteButton' size='small' onClick={() => deletePipelineClick(params.row)}>
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
        getRowId={(row: PipelineDescr) => row.pipeline_id}
        columns={columns}
        rowThreshold={0}
        getDetailPanelHeight={() => 'auto'}
        getDetailPanelContent={getDetailPanelContent}
        components={{
          Toolbar: QuickSearchToolbar,
          ErrorOverlay: ErrorOverlay
        }}
        rows={filteredData.length ? filteredData : rows}
        pageSize={pageSize}
        rowsPerPageOptions={[7, 10, 25, 50]}
        onPageSizeChange={newPageSize => setPageSize(newPageSize)}
        error={error}
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
          },
          errorOverlay: {
            isError: isError,
            error: error
          }
        }}
      />
    </Card>
  )
}
