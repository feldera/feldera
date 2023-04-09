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
import QuickSearchToolbar from 'src/components/table/data-grid/QuickSearchToolbar'
import {
  AttachedConnector,
  CancelError,
  ConfigDescr,
  ConfigService,
  ConnectorDescr,
  ConnectorService,
  Direction,
  NewPipelineRequest,
  NewPipelineResponse,
  PipelineService,
  ProjectService,
  ShutdownPipelineRequest
} from 'src/types/manager'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ErrorOverlay } from 'src/components/table/data-grid/ErrorOverlay'
import { escapeRegExp } from 'src/utils/escapeRegExp'
import { match } from 'ts-pattern'
import router from 'next/router'
import useStatusNotification from 'src/components/errors/useStatusNotification'
import { ConnectorStatus, GlobalMetrics, InputConnectorMetrics, OutputConnectorMetrics } from 'src/types/pipeline'
import { humanSize } from 'src/utils/humanSize'
import { format } from 'd3-format'

interface ConnectorData {
  ac: AttachedConnector
  c: ConnectorDescr | undefined
}

function DetailPanelContent(props: { row: ConfigDescr }) {
  const [inputs, setInputs] = useState<ConnectorData[]>([])
  const [outputs, setOutputs] = useState<ConnectorData[]>([])

  //const [project, setProject] = useState<ProjectDescr | undefined>(undefined)

  const projectQuery = useQuery({
    queryKey: ['projectStatus'],
    queryFn: () => {
      if (props.row.project_id) {
        return ProjectService.projectStatus(props.row.project_id)
      }
    },
    enabled: props.row.project_id !== undefined
  })

  const connectorQuery = useQuery({
    queryKey: ['connector'],
    queryFn: ConnectorService.listConnectors
  })
  useEffect(() => {
    if (!connectorQuery.isLoading && !connectorQuery.isError) {
      setInputs(
        props.row.attached_connectors
          .filter(ac => ac.direction === Direction.INPUT)
          .map(ac => {
            return { ac, c: connectorQuery.data.find(c => c.connector_id === ac.connector_id) }
          })
      )

      setOutputs(
        props.row.attached_connectors
          .filter(ac => ac.direction === Direction.OUTPUT)
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
  const [inputMetrics, setInputMetrics] = useState<Map<string, ConnectorMetrics>>(new Map())
  const [outputMetrics, setOutputMetrics] = useState<Map<string, ConnectorMetrics>>(new Map())
  const pipelineStatusQuery = useQuery({
    queryFn: () => {
      if (props.row.pipeline) {
        return PipelineService.pipelineStatus(props.row.pipeline.pipeline_id)
      }
    },
    queryKey: ['pipelineStatus'],
    enabled: props.row.pipeline !== undefined && props.row.pipeline !== null,
    refetchInterval: 1000
  })
  useEffect(() => {
    if (!pipelineStatusQuery.isLoading && !pipelineStatusQuery.isError) {
      console.log(pipelineStatusQuery.data)
      const metrics = pipelineStatusQuery.data['global_metrics']
      setGlobalMetrics(oldMetrics => [...oldMetrics, metrics])

      const newInputMetrics = new Map<string, InputConnectorMetrics>()
      pipelineStatusQuery.data['inputs'].forEach((cs: ConnectorStatus) => {
        newInputMetrics.set(cs.endpoint_name, cs.metrics as InputConnectorMetrics)
      })
      setInputMetrics(newInputMetrics)

      const newOutputMetrics = new Map<string, OutputConnectorMetrics>()
      pipelineStatusQuery.data['outputs'].forEach((cs: ConnectorStatus) => {
        newOutputMetrics.set(cs.endpoint_name, cs.metrics as OutputConnectorMetrics)
      })
      setOutputMetrics(newOutputMetrics)
    }
    if (props.row.pipeline == undefined) {
      setGlobalMetrics([])
      setInputMetrics(new Map())
      setOutputMetrics(new Map())
    }
  }, [pipelineStatusQuery.isLoading, pipelineStatusQuery.isError, pipelineStatusQuery.data, props.row.pipeline])

  return (
    !connectorQuery.isLoading &&
    !connectorQuery.isError &&
    !projectQuery.isLoading &&
    !projectQuery.isError && (
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
                {props.row.pipeline && (
                  <>
                    <ListItem>
                      <Tooltip title='Pipeline Running Since'>
                        <ListItemIcon>
                          <Icon icon='clarity:date-line' fontSize={20} />
                        </ListItemIcon>
                      </Tooltip>
                      <ListItemText primary={props.row.pipeline.created || 'Not running'} />
                    </ListItem>
                    <ListItem>
                      <Tooltip title='Pipeline Port'>
                        <ListItemIcon>
                          <Icon icon='carbon:port-input' fontSize={20} />
                        </ListItemIcon>
                      </Tooltip>
                      <ListItemText primary={props.row.pipeline.port || '0000'} />
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
            <Paper>
              <DataGridPro
                autoHeight
                getRowId={(row: ConnectorData) => row.ac.uuid}
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

                  //{
                  //field: 'enabled',
                  //headerName: 'Active',
                  //flex: 0.15,
                  //renderCell: () => <Switch defaultChecked disabled />
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
                            onClick={() =>
                              router.push(
                                '/streaming/introspection/' + props.row.config_id + '/' + params.row.ac.config
                              )
                            }
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
            <Paper>
              <DataGridPro
                autoHeight
                getRowId={(row: ConnectorData) => row.ac.uuid}
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
                            onClick={() =>
                              router.push(
                                '/streaming/introspection/' + props.row.config_id + '/' + params.row.ac.config
                              )
                            }
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
    )
  )
}

enum PipelineStatus {
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

const statusToChip = (status: PipelineStatus | undefined) => {
  return match(status)
    .with(undefined, () => <CustomChip rounded size='small' skin='light' label='Unused' />)
    .with(PipelineStatus.UNUSED, () => <CustomChip rounded size='small' skin='light' label={status} />)
    .with(PipelineStatus.CREATING, () => (
      <CustomChip rounded size='small' skin='light' color='secondary' label={status} />
    ))
    .with(PipelineStatus.CREATE_FAILURE, () => (
      <CustomChip rounded size='small' skin='light' color='error' label={status} />
    ))
    .with(PipelineStatus.STARTING, () => (
      <CustomChip rounded size='small' skin='light' color='secondary' label={status} />
    ))
    .with(PipelineStatus.STARTUP_FAILURE, () => (
      <CustomChip rounded size='small' skin='light' color='error' label={status} />
    ))
    .with(PipelineStatus.RUNNING, () => <CustomChip rounded size='small' skin='light' color='success' label={status} />)
    .with(PipelineStatus.PAUSING, () => <CustomChip rounded size='small' skin='light' color='info' label={status} />)
    .with(PipelineStatus.PAUSED, () => <CustomChip rounded size='small' skin='light' color='info' label={status} />)
    .with(PipelineStatus.ERROR, () => <CustomChip rounded size='small' skin='light' color='error' label={status} />)
    .with(PipelineStatus.SHUTTING_DOWN, () => (
      <CustomChip rounded size='small' skin='light' color='secondary' label={status} />
    ))
    .exhaustive()
}

export default function PipelineTable() {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()

  const [pageSize, setPageSize] = useState<number>(7)
  const [searchText, setSearchText] = useState<string>('')
  const [rows, setRows] = useState<ConfigDescr[]>([])
  const [filteredData, setFilteredData] = useState<ConfigDescr[]>([])
  const [isLaunching, setIsLaunching] = useState(new Map<number, PipelineStatus>())

  const { isLoading, isError, data, error } = useQuery(['configs'], ConfigService.listConfigs)
  useEffect(() => {
    if (!isLoading && !isError) {
      setRows(data)
      console.log(data)
    }
  }, [isLoading, isError, data, setRows])

  const { mutate: startPipelineMutate, isLoading: startPipelineLoading } = useMutation<string, CancelError, number>(
    PipelineService.pipelineStart
  )
  const { mutate: newPipelineMutate, isLoading: newPipelineLoading } = useMutation<
    NewPipelineResponse,
    CancelError,
    NewPipelineRequest
  >(PipelineService.newPipeline)

  const startPipelineClick = useCallback(
    (curRow: ConfigDescr) => {
      if (!newPipelineLoading && !startPipelineLoading && !curRow.pipeline) {
        setIsLaunching(map => new Map(map.set(curRow.config_id, PipelineStatus.CREATING)))
        newPipelineMutate(
          { config_id: curRow.config_id, config_version: 0 },
          {
            onSuccess: resp => {
              setIsLaunching(map => new Map(map.set(curRow.config_id, PipelineStatus.STARTING)))
              startPipelineMutate(resp.pipeline_id, {
                onSuccess: () => {
                  queryClient.invalidateQueries({ queryKey: ['configs'] })
                  setIsLaunching(map => new Map(map.set(curRow.config_id, PipelineStatus.RUNNING)))
                },
                onError: error => {
                  pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
                  queryClient.invalidateQueries({ queryKey: ['configs'] })
                  setIsLaunching(map => new Map(map.set(curRow.config_id, PipelineStatus.STARTUP_FAILURE)))
                }
              })
            },
            onError: error => {
              pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
              queryClient.invalidateQueries({ queryKey: ['configs'] })
              setIsLaunching(map => new Map(map.set(curRow.config_id, PipelineStatus.CREATE_FAILURE)))
            }
          }
        )
      } else if (!newPipelineLoading && !startPipelineLoading && curRow.pipeline) {
        // Pipeline already exists, we just need to start it
        setIsLaunching(map => new Map(map.set(curRow.config_id, PipelineStatus.STARTING)))
        startPipelineMutate(curRow.pipeline.pipeline_id, {
          onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['configs'] })
            setIsLaunching(map => new Map(map.set(curRow.config_id, PipelineStatus.RUNNING)))
          },
          onError: error => {
            pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
            queryClient.invalidateQueries({ queryKey: ['configs'] })
            setIsLaunching(map => new Map(map.set(curRow.config_id, PipelineStatus.STARTUP_FAILURE)))
          }
        })
      }
    },
    [
      newPipelineMutate,
      newPipelineLoading,
      queryClient,
      pushMessage,
      setIsLaunching,
      startPipelineLoading,
      startPipelineMutate
    ]
  )

  const { mutate: pausePipelineMutate, isLoading: pausePipelineLoading } = useMutation<string, CancelError, number>(
    PipelineService.pipelinePause
  )
  const pausePipelineClick = useCallback(
    (curRow: ConfigDescr) => {
      if (!pausePipelineLoading && curRow.pipeline !== undefined) {
        setIsLaunching(map => new Map(map.set(curRow.config_id, PipelineStatus.PAUSING)))
        pausePipelineMutate(curRow.pipeline.pipeline_id, {
          onSuccess: () => {
            setIsLaunching(map => new Map(map.set(curRow.config_id, PipelineStatus.PAUSED)))
            queryClient.invalidateQueries({ queryKey: ['configs'] })
          },
          onError: error => {
            pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
            queryClient.invalidateQueries({ queryKey: ['configs'] })
            setIsLaunching(map => new Map(map.set(curRow.config_id, PipelineStatus.RUNNING)))
          }
        })
      }
    },
    [pausePipelineLoading, pushMessage, queryClient, pausePipelineMutate]
  )

  const { mutate: deletePipelineMutate, isLoading: deletePipelineLoading } = useMutation<
    string,
    CancelError,
    ShutdownPipelineRequest
  >(PipelineService.pipelineShutdown)
  const deletePipelineClick = useCallback(
    (curRow: ConfigDescr) => {
      if (!deletePipelineLoading && curRow.pipeline !== undefined) {
        setIsLaunching(map => new Map(map.set(curRow.config_id, PipelineStatus.SHUTTING_DOWN)))
        deletePipelineMutate(
          { pipeline_id: curRow.pipeline.pipeline_id },
          {
            onSuccess: () => {
              setIsLaunching(map => new Map(map.set(curRow.config_id, PipelineStatus.UNUSED)))
              queryClient.invalidateQueries({ queryKey: ['configs'] })
              queryClient.setQueryData(['configs'], (oldData: ConfigDescr[] | undefined) =>
                oldData?.map((config: ConfigDescr) => {
                  if (config.config_id === curRow.config_id) {
                    return { ...config, pipeline: undefined }
                  } else {
                    return config
                  }
                })
              )
            },
            onError: error => {
              pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
              queryClient.invalidateQueries({ queryKey: ['configs'] })
              setIsLaunching(map => new Map(map.set(curRow.config_id, PipelineStatus.RUNNING)))
            }
          }
        )
      }
    },
    [deletePipelineLoading, pushMessage, queryClient, deletePipelineMutate]
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
        const status = statusToChip(isLaunching.get(params.row.config_id))

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
        const needsPause = isLaunching.get(params.row.config_id) === PipelineStatus.RUNNING
        const needsStart =
          (params.row.pipeline == null && !isLaunching.has(params.row.config_id)) ||
          (isLaunching.get(params.row.config_id) !== PipelineStatus.RUNNING &&
            isLaunching.get(params.row.config_id) !== PipelineStatus.STARTING &&
            isLaunching.get(params.row.config_id) !== PipelineStatus.CREATING &&
            isLaunching.get(params.row.config_id) !== PipelineStatus.PAUSING)
        const needsEdit =
          (params.row.pipeline == null && !isLaunching.has(params.row.config_id)) ||
          (isLaunching.get(params.row.config_id) !== PipelineStatus.RUNNING &&
            isLaunching.get(params.row.config_id) !== PipelineStatus.STARTING &&
            isLaunching.get(params.row.config_id) !== PipelineStatus.CREATING &&
            isLaunching.get(params.row.config_id) !== PipelineStatus.PAUSING)
        const needsDelete =
          params.row.pipeline !== null &&
          isLaunching.get(params.row.config_id) !== PipelineStatus.RUNNING &&
          isLaunching.get(params.row.config_id) !== PipelineStatus.STARTING &&
          isLaunching.get(params.row.config_id) !== PipelineStatus.CREATING &&
          isLaunching.get(params.row.config_id) !== PipelineStatus.PAUSING
        const needsInspect = isLaunching.get(params.row.config_id) === PipelineStatus.RUNNING
        const needsSpinner =
          isLaunching.get(params.row.config_id) === PipelineStatus.STARTING ||
          isLaunching.get(params.row.config_id) === PipelineStatus.PAUSING ||
          isLaunching.get(params.row.config_id) === PipelineStatus.CREATING

        return (
          <>
            {needsPause && (
              <Tooltip title='Pause Pipeline'>
                <IconButton size='small' onClick={() => pausePipelineClick(params.row)}>
                  <Icon icon='bx:pause-circle' fontSize={20} />
                </IconButton>
              </Tooltip>
            )}
            {needsStart && (
              <Tooltip title='Start Pipeline'>
                <IconButton size='small' onClick={() => startPipelineClick(params.row)}>
                  <Icon icon='bx:play-circle' fontSize={20} />
                </IconButton>
              </Tooltip>
            )}
            {needsSpinner &&
              (isLaunching.get(params.row.config_id) === PipelineStatus.STARTING ||
                isLaunching.get(params.row.config_id) === PipelineStatus.CREATING) && (
                <Tooltip title={isLaunching.get(params.row.config_id)}>
                  <IconButton size='small'>
                    <Icon icon='svg-spinners:270-ring-with-bg' fontSize={20} />
                  </IconButton>
                </Tooltip>
              )}
            {needsEdit && (
              <Tooltip title='Edit Pipeline'>
                <IconButton
                  size='small'
                  href='#'
                  onClick={() => router.push('/streaming/builder/' + params.row.config_id)}
                >
                  <Icon icon='bx:pencil' fontSize={20} />
                </IconButton>
              </Tooltip>
            )}
            {needsDelete && (
              <Tooltip title='Shutdown Pipeline'>
                <IconButton size='small' onClick={() => deletePipelineClick(params.row)}>
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
        getRowId={(row: ConfigDescr) => row.config_id}
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
