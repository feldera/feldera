'use client'

import { BreadcrumbsHeader } from '$lib/components/common/BreadcrumbsHeader'
import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import SaveIndicator, { SaveIndicatorState } from '$lib/components/common/SaveIndicator'
import { UnknownConnectorDialog } from '$lib/components/connectors/dialogs/UnknownConnector'
import Metadata from '$lib/components/streaming/builder/Metadata'
import MissingSchemaDialog from '$lib/components/streaming/builder/NoSchemaDialog'
import PipelineGraph from '$lib/components/streaming/builder/PipelineBuilder'
import { connectorConnects, useAddConnector } from '$lib/compositions/streaming/builder/useAddIoNode'
import { useBuilderState } from '$lib/compositions/streaming/builder/useBuilderState'
import { useReplacePlaceholder } from '$lib/compositions/streaming/builder/useSqlPlaceholderClick'
import { useHashPart } from '$lib/compositions/useHashPart'
import { partition, replaceElement } from '$lib/functions/common/array'
import { setQueryData } from '$lib/functions/common/tanstack'
import { showOnHashPart } from '$lib/functions/urlHash'
import {
  ApiError,
  AttachedConnector,
  NewPipelineRequest,
  NewPipelineResponse,
  PipelineId,
  PipelinesService,
  UpdatePipelineRequest,
  UpdatePipelineResponse
} from '$lib/services/manager'
import { invalidatePipeline, PipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import { IONodeData, ProgramNodeData } from '$lib/types/connectors'
import assert from 'assert'
import { useSearchParams } from 'next/navigation'
import { Dispatch, SetStateAction, useEffect, useState } from 'react'
import { ReactFlowProvider, useReactFlow } from 'reactflow'
import invariant from 'tiny-invariant'
import { match } from 'ts-pattern'
import { useDebouncedCallback } from 'use-debounce'

import { Card, CardContent, Link } from '@mui/material'
import Grid from '@mui/material/Grid'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

const stateToSaveLabel = (state: SaveIndicatorState): string =>
  match(state)
    .with('isModified', () => {
      return 'Saving ...'
    })
    .with('isDebouncing', () => {
      return 'Saving ...'
    })
    .with('isSaving', () => {
      return 'Saving ...'
    })
    .with('isUpToDate', () => {
      return 'Saved'
    })
    .with('isNew', () => {
      return 'New Pipeline'
    })
    .exhaustive()

const detachConnector = (c: AttachedConnector) => ({ ...c, relation_name: '' }) as AttachedConnector

const PipelineBuilderPage = (props: {
  pipelineId: PipelineId | undefined
  setPipelineId: Dispatch<SetStateAction<PipelineId | undefined>>
}) => {
  const [hash, setHash] = useHashPart()
  const showOnHash = showOnHashPart([hash, setHash])
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

  const { getNode, getEdges, setNodes } = useReactFlow<IONodeData | ProgramNodeData>()

  const { mutate: newPipelineMutate } = useMutation<NewPipelineResponse, ApiError, NewPipelineRequest>({
    mutationFn: PipelinesService.newPipeline
  })
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
  const projectsQuery = useQuery({
    ...PipelineManagerQuery.programs(),
    refetchInterval: 2000
  })
  const connectorQuery = useQuery(PipelineManagerQuery.connector())
  const pipelineQuery = useQuery({
    ...PipelineManagerQuery.pipelineStatus(pipelineId!),
    enabled:
      pipelineId !== undefined && saveState !== 'isSaving' && saveState !== 'isModified' && saveState !== 'isDebouncing'
  })
  useEffect(() => {
    if (saveState === 'isSaving' || saveState === 'isModified' || saveState === 'isDebouncing') {
      return
    }
    const isReady = !(
      pipelineQuery.isPending ||
      pipelineQuery.isError ||
      projectsQuery.isPending ||
      projectsQuery.isError ||
      connectorQuery.isPending ||
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

    // We don't set so `setSaveState` here because we don't want to override
    // the saveState every time the backend returns some result. Because it
    // could cancel potentially in-progress saves (started by client action).

    const project = projectsQuery.data.find(p => p.program_id === descriptor.program_id)
    const validConnections = !project
      ? attachedConnectors
      : (() => {
          setMissingSchemaDialog(!project.schema)

          setProject(project)
          replacePlaceholder(project)
          // Update handles of SQL Program node when program is recompiled, hide stale connection edges
          setNodes(nodes =>
            replaceElement(nodes, node => {
              if (node.type !== 'sqlProgram') {
                return null
              }
              return {
                ...node,
                data: { label: project.name, program: project }
              }
            })
          )

          const [validConnections, invalidConnections] = partition(attachedConnectors, connector =>
            connectorConnects(project.schema, connector)
          )

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
    connectorQuery.isPending,
    connectorQuery.isError,
    connectorQuery.data,
    pipelineQuery.isPending,
    pipelineQuery.isError,
    pipelineQuery.data,
    projectsQuery.isPending,
    projectsQuery.isError,
    projectsQuery.data,
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
    saveState,
    setNodes
  ])

  const setModifiedWhenDebouncing = useDebouncedCallback(() => {
    if (saveState === 'isDebouncing') {
      setSaveState('isModified')
    }
  }, 2000)

  // Send requests to update pipeline according to builder UI
  useEffect(() => {
    if (saveState === 'isDebouncing') {
      setModifiedWhenDebouncing()
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
    const connectors = getEdges().map(edge => {
      const source = getNode(edge.source)
      const target = getNode(edge.target)
      const connectsInput = 'sql' !== source?.id
      const connector = connectsInput ? source : target
      invariant(connector, "Couldn't extract attached connector from edge")
      invariant('ac' in connector.data, 'Wrong connector node data')
      const ac = connector.data.ac

      ac.relation_name = (handle => {
        invariant(handle, 'Node handle string should be defined')
        return handle.replace(/^table-|^view-/, '')
      })(connectsInput ? edge.targetHandle : edge.sourceHandle)

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
          setQueryData(queryClient, PipelineManagerQuery.pipelineStatus(pipelineId), oldData => {
            if (!oldData) {
              return oldData
            }
            return {
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
          })
          setSaveState('isUpToDate')
        }
      }
    )
  }, [
    saveState,
    setModifiedWhenDebouncing,
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
      <BreadcrumbsHeader>
        <Link href={`/streaming/management`}>Pipelines</Link>
        <Link href={`/streaming/builder/?pipeline_id=${pipelineId}`}>{name}</Link>
      </BreadcrumbsHeader>
      <Grid container spacing={6} className='match-height' id='pipeline-builder-content' sx={{ pl: 6, pt: 6 }}>
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
      {(id => id && <UnknownConnectorDialog {...showOnHash('edit/connector')} connectorId={id} />)(
        /edit\/connector\/([\w-]+)/.exec(hash)?.[1]
      )}
    </>
  )
}

export default () => {
  const [pipelineId, setPipelineId] = useState<PipelineId | undefined>(undefined)
  const newPipelineId = useSearchParams().get('pipeline_id')

  useEffect(() => {
    if (newPipelineId) {
      setPipelineId(newPipelineId)
    }
  }, [newPipelineId, setPipelineId])

  return (
    <ReactFlowProvider>
      <PipelineBuilderPage pipelineId={pipelineId} setPipelineId={setPipelineId} />
    </ReactFlowProvider>
  )
}
