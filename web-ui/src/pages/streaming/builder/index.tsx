import assert from 'assert'
import { Dispatch, SetStateAction, useEffect, useState } from 'react'
import Grid from '@mui/material/Grid'
import Typography from '@mui/material/Typography'
import PageHeader from 'src/layouts/components/page-header'
import { Card, CardContent } from '@mui/material'
import PipelineGraph from 'src/streaming/builder/PipelineBuilder'
import SaveIndicator, { SaveIndicatorState } from 'src/components/SaveIndicator'
import { match } from 'ts-pattern'
import Metadata from 'src/streaming/builder/Metadata'
import { useBuilderState } from 'src/streaming/builder/useBuilderState'
import {
  AttachedConnector,
  CancelError,
  PipelineDescr,
  PipelineId,
  PipelineService,
  ConnectorDescr,
  NewPipelineRequest,
  NewPipelineResponse,
  ProgramDescr,
  UpdatePipelineRequest,
  UpdatePipelineResponse
} from 'src/types/manager'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ReactFlowProvider, useReactFlow } from 'reactflow'
import { useDebouncedCallback } from 'use-debounce'
import { removePrefix } from 'src/utils'
import { useReplacePlaceholder } from 'src/streaming/builder/hooks/useSqlPlaceholderClick'
import { connectorConnects, useAddConnector } from 'src/streaming/builder/hooks/useAddIoNode'
import MissingSchemaDialog from 'src/streaming/builder/NoSchemaDialog'
import useStatusNotification from 'src/components/errors/useStatusNotification'
import { invalidatePipeline } from 'src/types/defaultQueryFn'

const stateToSaveLabel = (state: SaveIndicatorState): string =>
  match(state)
    .with('isModified' as const, () => {
      return 'Saving ...'
    })
    .with('isDebouncing' as const, () => {
      return 'Saving ...'
    })
    .with('isSaving' as const, () => {
      return 'Saving ...'
    })
    .with('isUpToDate' as const, () => {
      return 'Saved'
    })
    .with('isNew' as const, () => {
      return 'New Pipeline'
    })
    .exhaustive()

export const PipelineWithProvider = (props: {
  pipelineId: PipelineId | undefined
  setPipelineId: Dispatch<SetStateAction<PipelineId | undefined>>
}) => {
  const queryClient = useQueryClient()
  const [missingSchemaDialog, setMissingSchemaDialog] = useState(false)

  const { pipelineId, setPipelineId } = props
  const setSaveState = useBuilderState(state => state.setSaveState)
  const saveState = useBuilderState(state => state.saveState)

  const name = useBuilderState(state => state.name)
  const setName = useBuilderState(state => state.setName)

  const description = useBuilderState(state => state.description)
  const setDescription = useBuilderState(state => state.setDescription)

  const config = useBuilderState(state => state.config)
  const setConfig = useBuilderState(state => state.setConfig)

  const project = useBuilderState(state => state.project)
  const setProject = useBuilderState(state => state.setProject)

  const { getNode, getEdges } = useReactFlow()

  const { mutate: newPipelineMutate } = useMutation<NewPipelineResponse, CancelError, NewPipelineRequest>(
    PipelineService.newPipeline
  )
  const { mutate: updatePipelineMutate } = useMutation<UpdatePipelineResponse, CancelError, UpdatePipelineRequest>(
    PipelineService.updatePipeline
  )
  const replacePlaceholder = useReplacePlaceholder()
  const addConnector = useAddConnector()

  const { pushMessage } = useStatusNotification()
  const projects = useQuery<ProgramDescr[]>(['program'])
  const connectorQuery = useQuery<ConnectorDescr[]>(['connector'])
  const pipelineQuery = useQuery<PipelineDescr>(['pipelineStatus', { pipeline_id: pipelineId }], {
    enabled:
      pipelineId !== undefined && saveState !== 'isSaving' && saveState !== 'isModified' && saveState !== 'isDebouncing'
  })
  useEffect(() => {
    if (saveState !== 'isSaving' && saveState !== 'isModified' && saveState !== 'isDebouncing') {
      if (
        !pipelineQuery.isLoading &&
        !pipelineQuery.isError &&
        !projects.isLoading &&
        !projects.isError &&
        !connectorQuery.isLoading &&
        !connectorQuery.isError
      ) {
        setPipelineId(() => pipelineQuery.data.pipeline_id)
        setName(pipelineQuery.data.name)
        setDescription(pipelineQuery.data.description)
        setConfig(pipelineQuery.data.config)
        setSaveState('isUpToDate')

        const attachedConnectors = pipelineQuery.data.attached_connectors
        let invalidConnections: AttachedConnector[] = []
        let validConnections: AttachedConnector[] = attachedConnectors
        console.log(attachedConnectors)

        // We don't set so `setSaveState` here because we don't want to override
        // the saveState every time the backend returns some result. Because it
        // could cancel potentially in-progress saves (started by client action).

        if (pipelineQuery.data.program_id) {
          const foundProject = projects.data.find(p => p.program_id === pipelineQuery.data.program_id)
          if (foundProject) {
            if (!foundProject.schema) {
              setMissingSchemaDialog(true)
            } else {
              setMissingSchemaDialog(false)
            }

            if (attachedConnectors) {
              invalidConnections = attachedConnectors.filter(attached_connector => {
                return !connectorConnects(attached_connector, foundProject.schema)
              })
              validConnections = attachedConnectors.filter(attached_connector => {
                return connectorConnects(attached_connector, foundProject.schema)
              })
            }

            setProject(foundProject)
            replacePlaceholder(foundProject)
          }
        }

        if (invalidConnections.length > 0) {
          pushMessage({
            key: new Date().getTime(),
            color: 'warning',
            message: `Could not attach ${
              invalidConnections.length
            } connector(s): No tables/views named ${invalidConnections.map(c => c.config).join(', ')} found.`
          })
        }

        if (validConnections) {
          validConnections.forEach(attached_connector => {
            const connector = connectorQuery.data.find(
              connector => connector.connector_id === attached_connector.connector_id
            )
            if (connector) {
              addConnector(connector, attached_connector)
            }
          })
        }
      } else if (pipelineId === undefined) {
        setProject(undefined)
        setSaveState('isNew')
        setName('')
        setDescription('')
        // TODO: Set to 8 for now, needs to be configurable eventually
        setConfig('workers: 8\n')
      }
    }
  }, [
    connectorQuery.isLoading,
    connectorQuery.isError,
    connectorQuery.data,
    pipelineQuery.isLoading,
    pipelineQuery.isError,
    pipelineQuery.data,
    projects.isLoading,
    projects.isError,
    projects.data,
    setPipelineId,
    setName,
    setDescription,
    setConfig,
    setSaveState,
    setProject,
    replacePlaceholder,
    addConnector,
    pipelineId,
    pushMessage,
    saveState
  ])

  const debouncedSave = useDebouncedCallback(() => {
    if (saveState === 'isDebouncing') {
      setSaveState('isModified')
    }
  }, 2000)

  useEffect(() => {
    if (saveState === 'isDebouncing') {
      debouncedSave()
    }

    if (saveState === 'isModified') {
      setSaveState('isSaving')
      console.log('update existing config')

      // Create a new pipeline
      if (pipelineId === undefined) {
        newPipelineMutate(
          {
            name,
            program_id: project?.program_id,
            description,
            config
          },
          {
            onError: (error: CancelError) => {
              pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
              setSaveState('isUpToDate')
              console.log('error', error)
            },
            onSuccess: (data: NewPipelineResponse) => {
              setPipelineId(data.pipeline_id)
              setSaveState('isUpToDate')
            }
          }
        )
      } else {
        // Update an existing pipeline
        const connectors: Array<AttachedConnector> = getEdges().map(edge => {
          const source = getNode(edge.source)
          const target = getNode(edge.target)
          const connector = source?.id === 'sql' ? target : source

          const ac = connector?.data.ac
          //console.log('edge.sourceHandle', edge.sourceHandle, 'edge', edge)
          if (ac == undefined) {
            throw new Error('data.ac in an edge was undefined')
          }
          const tableOrView = ac.is_input
            ? removePrefix(edge.targetHandle || '', 'table-')
            : removePrefix(edge.sourceHandle || '', 'view-')
          ac.config = tableOrView

          return ac
        })

        const updateRequest = {
          pipeline_id: pipelineId,
          name,
          description,
          program_id: project?.program_id,
          config,
          connectors
        }

        updatePipelineMutate(updateRequest, {
          onSettled: () => {
            assert(pipelineId !== undefined)
            invalidatePipeline(queryClient, pipelineId)
          },
          onError: (error: CancelError) => {
            pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
            setSaveState('isUpToDate')
          },
          onSuccess: () => {
            // It's important to update the query cache here because otherwise
            // sometimes the query cache will be out of date and the UI will
            // show the old connectors again after deletion.
            queryClient.setQueryData(
              ['pipelineStatus', { pipeline_id: pipelineId }],
              (oldData: PipelineDescr | undefined) => {
                return oldData
                  ? {
                      ...oldData,
                      name,
                      description,
                      program_id: project?.program_id,
                      config,
                      attached_connectors: connectors
                    }
                  : oldData
              }
            )
            setSaveState('isUpToDate')
          }
        })
      }
    }
  }, [
    saveState,
    debouncedSave,
    setSaveState,
    setPipelineId,
    updatePipelineMutate,
    newPipelineMutate,
    pipelineId,
    project,
    name,
    description,
    config,
    getNode,
    getEdges,
    pushMessage,
    queryClient
  ])

  return (
    <>
      <Grid container spacing={6} className='match-height' id='pipeline-builder-content'>
        <PageHeader
          title={<Typography variant='h5'>Pipeline Creator</Typography>}
          subtitle={<Typography variant='body2'>Define an end-to-end pipeline with analytics.</Typography>}
        />
        <Grid item xs={12}>
          {/* id referenced by webui-tester */}
          <Card>
            <CardContent>
              <Metadata errors={{}} />
            </CardContent>
            <CardContent>
              <Grid item xs={12}>
                {/* id referenced by webui-tester */}
                <SaveIndicator id='save-indicator' stateToLabel={stateToSaveLabel} state={saveState} />
              </Grid>
            </CardContent>
          </Card>
        </Grid>

        <div style={{ width: '100vw', height: '60vh' }}>
          <PipelineGraph />
        </div>
      </Grid>
      <MissingSchemaDialog
        open={missingSchemaDialog}
        setOpen={setMissingSchemaDialog}
        program_id={project?.program_id}
      />
    </>
  )
}

const Pipeline = () => {
  const [pipelineId, setPipelineId] = useState<PipelineId | undefined>(undefined)

  return (
    <ReactFlowProvider>
      <PipelineWithProvider pipelineId={pipelineId} setPipelineId={setPipelineId} />
    </ReactFlowProvider>
  )
}

export default Pipeline
