import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import SaveIndicator, { SaveIndicatorState } from '$lib/components/common/SaveIndicator'
import Metadata from '$lib/components/streaming/builder/Metadata'
import MissingSchemaDialog from '$lib/components/streaming/builder/NoSchemaDialog'
import PipelineGraph from '$lib/components/streaming/builder/PipelineBuilder'
import { connectorConnects, useAddConnector } from '$lib/compositions/streaming/builder/useAddIoNode'
import { useBuilderState } from '$lib/compositions/streaming/builder/useBuilderState'
import { useReplacePlaceholder } from '$lib/compositions/streaming/builder/useSqlPlaceholderClick'
import { partition } from '$lib/functions/common/array'
import { removePrefix } from '$lib/functions/common/string'
import { invalidatePipeline } from '$lib/services/defaultQueryFn'
import {
  ApiError,
  AttachedConnector,
  ConnectorDescr,
  NewPipelineRequest,
  NewPipelineResponse,
  Pipeline,
  PipelineId,
  PipelinesService,
  ProgramDescr,
  UpdatePipelineRequest,
  UpdatePipelineResponse
} from '$lib/services/manager'
import assert from 'assert'
import { useRouter } from 'next/router'
import { Dispatch, SetStateAction, useEffect, useState } from 'react'
import { ReactFlowProvider, useReactFlow } from 'reactflow'
import { match } from 'ts-pattern'
import { useDebouncedCallback } from 'use-debounce'

import { Card, CardContent } from '@mui/material'
import Grid from '@mui/material/Grid'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

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

const detachConnector = (c: AttachedConnector) => ({ ...c, relation_name: '' }) as AttachedConnector

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

  const { mutate: newPipelineMutate } = useMutation<NewPipelineResponse, ApiError, NewPipelineRequest>(
    PipelinesService.newPipeline
  )
  const { mutate: updatePipelineMutate } = useMutation<
    UpdatePipelineResponse,
    ApiError,
    { pipeline_id: PipelineId; request: UpdatePipelineRequest }
  >({
    mutationFn: args => PipelinesService.updatePipeline(args.pipeline_id, args.request)
  })
  const replacePlaceholder = useReplacePlaceholder()
  const addConnector = useAddConnector()

  const { pushMessage } = useStatusNotification()
  const projects = useQuery<ProgramDescr[]>(['program'])
  const connectorQuery = useQuery<ConnectorDescr[]>(['connector'])
  const pipelineQuery = useQuery<Pipeline>(['pipelineStatus', { pipeline_id: pipelineId }], {
    enabled:
      pipelineId !== undefined && saveState !== 'isSaving' && saveState !== 'isModified' && saveState !== 'isDebouncing'
  })
  useEffect(() => {
    if (saveState === 'isSaving' || saveState === 'isModified' || saveState === 'isDebouncing') {
      return
    }
    const isReady = !(
      pipelineQuery.isLoading ||
      pipelineQuery.isError ||
      projects.isLoading ||
      projects.isError ||
      connectorQuery.isLoading ||
      connectorQuery.isError
    )
    if (!isReady && pipelineId === undefined) {
      setProject(undefined)
      setSaveState('isNew')
      setName('')
      setDescription('')
      // TODO: Set to 8 for now, needs to be configurable eventually
      setConfig({ workers: 8 })
      return
    }
    if (!isReady) {
      return
    }
    const descriptor = pipelineQuery.data.descriptor
    setPipelineId(() => descriptor.pipeline_id)
    setName(descriptor.name)
    setDescription(descriptor.description)
    setConfig(descriptor.config)
    setSaveState('isUpToDate')

    const attachedConnectors = descriptor.attached_connectors
    console.log(attachedConnectors)

    // We don't set so `setSaveState` here because we don't want to override
    // the saveState every time the backend returns some result. Because it
    // could cancel potentially in-progress saves (started by client action).

    const project = (id => (id ? projects.data.find(p => p.program_id === id) : undefined))(descriptor.program_id)
    const validConnections = !project
      ? attachedConnectors
      : (() => {
          setMissingSchemaDialog(!project.schema)

          console.log(project.schema)
          console.log(attachedConnectors)
          const [validConnections, invalidConnections] = partition(attachedConnectors, connector =>
            connectorConnects(project.schema, connector)
          )

          setProject(project)
          replacePlaceholder(project)

          if (invalidConnections.length > 0) {
            pushMessage({
              key: new Date().getTime(),
              color: 'warning',
              message: `Could not attach ${
                invalidConnections.length
              } connector(s): No tables/views named ${invalidConnections.map(c => c.relation_name).join(', ')} found.`
            })
          }

          const connectors = invalidConnections.map(detachConnector)
          validConnections.push(...connectors)

          return validConnections
        })()

    validConnections.forEach(attached_connector => {
      const connector = connectorQuery.data.find(
        connector => connector.connector_id === attached_connector.connector_id
      )
      if (connector) {
        addConnector(connector, attached_connector)
      }
    })
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

    if (saveState !== 'isModified') {
      return
    }

    setSaveState('isSaving')

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
          onError: (error: ApiError) => {
            pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
            setSaveState('isUpToDate')
            console.log('error', error)
          },
          onSuccess: (data: NewPipelineResponse) => {
            setPipelineId(data.pipeline_id)
            setSaveState('isUpToDate')
          }
        }
      )
      return
    }

    // Update an existing pipeline
    const connectors: Array<AttachedConnector> = getEdges().map(edge => {
      const source = getNode(edge.source)
      const target = getNode(edge.target)
      const connector = source?.id === 'sql' ? target : source

      const ac: AttachedConnector | undefined = connector?.data.ac
      //console.log('edge.sourceHandle', edge.sourceHandle, 'edge', edge)
      if (ac == undefined) {
        throw new Error('data.ac in an edge was undefined')
      }
      const tableOrView = ac.is_input
        ? removePrefix(edge.targetHandle || '', 'table-')
        : removePrefix(edge.sourceHandle || '', 'view-')
      ac.relation_name = tableOrView

      return ac
    })

    const updateRequest = {
      name,
      description,
      program_id: project?.program_id,
      config,
      connectors
    }

    updatePipelineMutate(
      { pipeline_id: pipelineId, request: updateRequest },
      {
        onSettled: () => {
          assert(pipelineId !== undefined)
          invalidatePipeline(queryClient, pipelineId)
        },
        onError: (error: ApiError) => {
          pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
          setSaveState('isUpToDate')
        },
        onSuccess: () => {
          // It's important to update the query cache here because otherwise
          // sometimes the query cache will be out of date and the UI will
          // show the old connectors again after deletion.
          queryClient.setQueryData(['pipelineStatus', { pipeline_id: pipelineId }], (oldData: Pipeline | undefined) => {
            return oldData
              ? {
                  ...oldData,
                  descriptor: {
                    ...oldData.descriptor,
                    name,
                    description,
                    program_id: project?.program_id,
                    config,
                    attached_connectors: connectors
                  }
                }
              : oldData
          })
          setSaveState('isUpToDate')
        }
      }
    )
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
  const router = useRouter()
  const [pipelineId, setPipelineId] = useState<PipelineId | undefined>(undefined)

  useEffect(() => {
    const { pipeline_id } = router.query
    console.log(router.query)
    if (router.isReady && typeof pipeline_id === 'string') {
      setPipelineId(pipeline_id)
    }
  }, [router.isReady, router.query, pipelineId, setPipelineId])

  return (
    <ReactFlowProvider>
      <PipelineWithProvider pipelineId={pipelineId} setPipelineId={setPipelineId} />
    </ReactFlowProvider>
  )
}

export default Pipeline
