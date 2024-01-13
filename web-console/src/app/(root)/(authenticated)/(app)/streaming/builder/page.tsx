'use client'

import { BreadcrumbsHeader } from '$lib/components/common/BreadcrumbsHeader'
import { EntitySyncIndicator, EntitySyncIndicatorStatus } from '$lib/components/common/EntitySyncIndicator'
import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { UnknownConnectorDialog } from '$lib/components/connectors/dialogs/UnknownConnector'
import Metadata from '$lib/components/streaming/builder/Metadata'
import MissingSchemaDialog from '$lib/components/streaming/builder/NoSchemaDialog'
import { PipelineGraph } from '$lib/components/streaming/builder/PipelineBuilder'
import { connectorConnects, useAddConnector } from '$lib/compositions/streaming/builder/useAddIoNode'
import { useBuilderState } from '$lib/compositions/streaming/builder/useBuilderState'
import { useDeleteNode } from '$lib/compositions/streaming/builder/useDeleteNode'
import { useReplacePlaceholder } from '$lib/compositions/streaming/builder/useSqlPlaceholderClick'
import { useUpdatePipeline } from '$lib/compositions/streaming/builder/useUpdatePipeline'
import { useHashPart } from '$lib/compositions/useHashPart'
import { partition, replaceElement } from '$lib/functions/common/array'
import { invalidateQuery, setQueryData } from '$lib/functions/common/tanstack'
import { showOnHashPart } from '$lib/functions/urlHash'
import {
  ApiError,
  AttachedConnector,
  ConnectorDescr,
  NewPipelineRequest,
  NewPipelineResponse,
  PipelineDescr,
  PipelineRuntimeState,
  UpdatePipelineRequest
} from '$lib/services/manager'
import {
  mutationCreatePipeline,
  PipelineManagerQuery,
  updatePipelineConnectorName
} from '$lib/services/pipelineManagerQuery'
import { IONodeData, ProgramNodeData } from '$lib/types/connectors'
import { Pipeline, PipelineStatus } from '$lib/types/pipeline'
import { useRouter, useSearchParams } from 'next/navigation'
import { Dispatch, SetStateAction, useEffect, useState } from 'react'
import { ReactFlowProvider, useReactFlow } from 'reactflow'
import invariant from 'tiny-invariant'
import { match } from 'ts-pattern'
import { useDebouncedCallback } from 'use-debounce'
import IconCheck from '~icons/bx/check'

import { Button, Card, CardContent, Link } from '@mui/material'
import Grid from '@mui/material/Grid'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

const SAVE_DELAY = 1000

interface FormError {
  name?: { message?: string }
}

const stateToSaveLabel = (state: EntitySyncIndicatorStatus): string =>
  match(state)
    .with('isModified', () => {
      return 'Saving …'
    })
    .with('isSaving', () => {
      return 'Saving …'
    })
    .with('isUpToDate', () => {
      return 'Saved'
    })
    .with('isNew', () => {
      return 'Name the pipeline to save it'
    })
    .with('isLoading', () => {
      return 'Loading …'
    })
    .exhaustive()

const detachConnector = (c: AttachedConnector) => ({ ...c, relation_name: '' }) as AttachedConnector

/*
const PipelineBuilderPage = ({
  pipeline,
  setPipeline,
  saveState,
  setSaveState
}: {
  pipeline: PipelineDescr,
  setPipeline: (pipeline: PipelineDescr) => void,
  saveState: EntitySyncIndicatorStatus,
  setSaveState: Dispatch<SetStateAction<EntitySyncIndicatorStatus>>
}) => {
  const router = useRouter()
  const gotoPipeline = (pipelineName: string) => router.push('/streaming/builder/?pipeline_name=' + pipelineName)
  const [hash, setHash] = useHashPart()
  const showOnHash = showOnHashPart([hash, setHash])
  const queryClient = useQueryClient()
  const [missingSchemaDialog, setMissingSchemaDialog] = useState(false)

  const { getNode, getEdges, setNodes } = useReactFlow<IONodeData | ProgramNodeData>()

  const { mutate: newPipelineMutate } = useMutation<NewPipelineResponse, ApiError, NewPipelineRequest>({
    mutationFn: PipelinesService.newPipeline
  })
  const { mutate: updatePipelineMutate } = useMutation(mutationUpdatePipeline(queryClient))
  const replacePlaceholder = useReplacePlaceholder()
  const addConnector = useAddConnector()

  const { pushMessage } = useStatusNotification()
  const projectsQuery = useQuery({
    ...PipelineManagerQuery.programs(),
    refetchInterval: 2000
  })
  const connectorQuery = useQuery(PipelineManagerQuery.connectors())

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
    if (!isReady && pipelineName === undefined) {
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

    gotoPipeline(descriptor.name)
    setName(descriptor.name)
    setDescription(descriptor.description)
    setConfig(descriptor.config)
    setSaveState('isUpToDate')

    const attachedConnectors = descriptor.attached_connectors

    // We don't set so `setSaveState` here because we don't want to override
    // the saveState every time the backend returns some result. Because it
    // could cancel potentially in-progress saves (started by client action).

    const project = projectsQuery.data.find(p => p.name === descriptor.program_name)
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
      const connector = connectorQuery.data.find(connector => connector.name === attached_connector.connector_name)
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
    gotoPipeline,
    setName,
    setDescription,
    setConfig,
    setSaveState,
    setProject,
    replacePlaceholder,
    addConnector,
    pipelineName,
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
    if (pipelineName === undefined) {
      invariant(name, 'Client error: cannot create pipeline - empty name!')
      newPipelineMutate(
        {
          name,
          program_name: project?.name,
          description,
          config
        },
        {
          onError: (error: ApiError) => {
            pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
            setSaveState('isUpToDate')
          },
          onSuccess: (_data: NewPipelineResponse) => {
            gotoPipeline(name)
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
      const connectsInput = source?.id !== 'sql'
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

    updatePipelineMutate(
      { pipelineName, request: {
        name,
        description,
        program_name: project?.name,
        config,
        connectors
      } },
      {
        onError: (error: ApiError) => {
          pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
          setSaveState('isUpToDate')
        },
        onSuccess: () => {
          setSaveState('isUpToDate')
        }
      }
    )
  }, [
    saveState,
    setModifiedWhenDebouncing,
    setSaveState,
    gotoPipeline,
    updatePipelineMutate,
    newPipelineMutate,
    pipelineName,
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
        <Link href={`/streaming/management`} data-testid='button-breadcrumb-pipelines'>
          Pipelines
        </Link>
        <Link href={`/streaming/builder/?pipeline_name=${pipelineName}`} data-testid='button-breadcrumb-pipeline-name'>
          {name}
        </Link>
      </BreadcrumbsHeader>
      <Grid container spacing={6} className='match-height' sx={{ pl: 6, pt: 6 }}>
        <Grid item xs={12}>
          <Card>
            <CardContent>
              <Metadata errors={{}} />
            </CardContent>
            <CardContent>
              <Grid item xs={12}>
                <EntitySyncIndicator getLabel={stateToSaveLabel} state={saveState} />
              </Grid>
            </CardContent>
          </Card>
        </Grid>

        <div style={{ width: '100vw', height: '60vh' }}>
          <PipelineGraph />
        </div>
      </Grid>
      <MissingSchemaDialog open={missingSchemaDialog} setOpen={setMissingSchemaDialog} program_name={project?.name} />
      {(connectorName =>
        connectorName && (
          <UnknownConnectorDialog
            {...showOnHash('edit/connector')}
            connectorName={connectorName}
            existingTitle={name => 'Update ' + name}
            submitButton={
              <Button
                variant='contained'
                color='success'
                endIcon={<IconCheck />}
                type='submit'
                data-testid='button-update'
              >
                Update
              </Button>
            }
          />
        ))(/edit\/connector\/([\w-]+)/.exec(hash)?.[1])}
      {(connectorName =>
        connectorName && (
          <UnknownConnectorDialog
            show={hash.startsWith('view/connector')}
            setShow={() => router.back()}
            connectorName={connectorName}
            existingTitle={name => 'Inspect ' + name}
            submitButton={<></>}
            disabled={true}
          />
        ))(/view\/connector\/([\w-]+)/.exec(hash)?.[1])}
    </>
  )
}
*/

/**
 * Displays pipeline in graph view when pipeline descriptor is updated
 * @param pipeline
 * @param setMissingSchemaDialog
 */
const useRenderPipelineEffect = (
  pipeline: PipelineDescr,
  saveState: EntitySyncIndicatorStatus,
  setMissingSchemaDialog: Dispatch<SetStateAction<boolean>>
) => {
  const { pushMessage } = useStatusNotification()
  const replacePlaceholder = useReplacePlaceholder()
  const addConnector = useAddConnector()
  const { setNodes, getNodes } = useReactFlow<IONodeData | ProgramNodeData>()
  const projectsQuery = useQuery({
    ...PipelineManagerQuery.programs(),
    refetchInterval: 4000
  })
  const connectorsQuery = useQuery({
    ...PipelineManagerQuery.connectors(),
    refetchInterval: 4000
  })
  const deleteNode = useDeleteNode(() => {})
  const render = () => {
    if (saveState === 'isSaving' || saveState === 'isModified') {
      return
    }
    if (!projectsQuery.data || !connectorsQuery.data) {
      return
    }
    const attachedConnectors = pipeline.attached_connectors

    // We don't set so `setSaveState` here because we don't want to override
    // the saveState every time the backend returns some result. Because it
    // could cancel potentially in-progress saves (started by client action).

    const project = projectsQuery.data.find(p => p.name === pipeline.program_name)
    const validConnections = !project
      ? attachedConnectors
      : (() => {
          setMissingSchemaDialog(!project.schema)
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



    // { // Handle the case when the connector name was changed in popup dialog
    //   const pipelineConnectors = new Set(attachedConnectors.map(c => c.connector_name))
    //   const deprecatedConnectors = connectorsQuery.data.filter(c => !pipelineConnectors.has(c.name))
    //   // const existingNode = getNodes().find(node => node.id === ac.name)
    //   console.log('deprecatedConnectors', deprecatedConnectors, attachedConnectors)
    //   if (deprecatedConnectors.length) {
    //     // setNodes(nodes =>
    //     //   nodes.filter(node => {
    //     //     if (node.type !== 'inputNode' && node.type !== 'outputNode') {
    //     //       return true
    //     //     }
    //     //     invariant(((data: any): data is {connector: ConnectorDescr} => true)(node.data))
    //     //     invariant(node.data.connector.name, 'Conenctor name should be stored in the connector node')
    //     //     return (name => !deprecatedConnectors.find(c => c.name === name))(node.data.connector.name)
    //     //   })
    //     // )
    //     const deprecatedNodes = getNodes().filter(node => {
    //       console.log('filtering node', node)
    //       if (node.type !== 'inputNode' && node.type !== 'outputNode') {
    //         return false
    //       }
    //       invariant(((data: any): data is {connector: ConnectorDescr} => true)(node.data))
    //       invariant(node.data.connector.name, 'Connector name should be stored in the connector node')
    //       console.log('node result', (name => !deprecatedConnectors.find(c => c.name === name))(node.data.connector.name))
    //       return (name => deprecatedConnectors.find(c => c.name === name))(node.data.connector.name)
    //     })
    //     for (const node of deprecatedNodes) {
    //       console.log('deleting node', node)
    //       // setTimeout(() => deleteNode(node.id), 100)
    //       // deleteNode(node.id)()
    //     }
    //   }
    // }

    // console.log('valid & invalid', validConnections, attachedConnectors, connectorsQuery.data)
    validConnections.forEach(attached_connector => {
      const connector = connectorsQuery.data.find(connector => connector.name === attached_connector.connector_name)
      if (connector) {
        console.log('addConnector', connector, attached_connector)
        addConnector(connector, attached_connector)
      }
    })


    { // Rename detached connectors when the connector name was changed in popup dialog
      setNodes(getNodes().map(node => {
        console.log('mapping node', node)
        if (node.type !== 'inputNode' && node.type !== 'outputNode') {
          return node
        }
        invariant(((data: any): data is {connector: ConnectorDescr} => true)(node.data))
        invariant(node.data.connector.name, 'Connector name should be stored in the connector node')
        const realConnector = (data => connectorsQuery.data.find(c => c.connector_id === data.connector.connector_id))(node.data)
        if (!realConnector) {
          return node
        }
        if (node.data.connector.name === realConnector.name) {
          return node
        }
        console.log('renaming node', node, node.data.connector.name, realConnector.name)
        return ({
          ...node,
          data: {
            ...node.data,
            connector: realConnector,
            ac: {
              ...node.data.ac,
              connector_name: realConnector.name
            }
          }
        })
        // console.log('node result', (name => !deprecatedConnectors.find(c => c.name === name))(node.data.connector.name))
        // return (name => deprecatedConnectors.find(c => c.name === name))(node.data.connector.name)
      }))
    }
  }

  useEffect(render, [
    connectorsQuery.isPending,
    connectorsQuery.isError,
    connectorsQuery.data,
    // pipelineQuery.isPending,
    // pipelineQuery.isError,
    // pipelineQuery.data,
    pipeline.program_name,
    pipeline.attached_connectors,
    projectsQuery.isPending,
    projectsQuery.isError,
    projectsQuery.data,
    // gotoPipeline,
    // setName,
    // setDescription,
    // setConfig,
    // setSaveState,
    // setProject,
    replacePlaceholder,
    addConnector,
    // pipelineName,
    pushMessage,
    saveState,
    setNodes,
    getNodes,
    deleteNode,
    setMissingSchemaDialog
  ])
}

const PipelineBuilderPage = ({
  pipeline,
  updatePipeline,
  saveState,
  setSaveState,
  formError,
  setFormError
}: {
  pipeline: PipelineDescr
  // setPipeline: (pipeline: PipelineDescr) => void
  updatePipeline: Dispatch<SetStateAction<UpdatePipelineRequest>>
  saveState: EntitySyncIndicatorStatus
  setSaveState: Dispatch<EntitySyncIndicatorStatus>
  formError: FormError
  setFormError: Dispatch<FormError>
}) => {
  const router = useRouter()
  const [hash, setHash] = useHashPart()
  const showOnHash = showOnHashPart([hash, setHash])
  const queryClient = useQueryClient()
  const [missingSchemaDialog, setMissingSchemaDialog] = useState(false)

  useCreatePipelineEffect(pipeline, updatePipeline, setSaveState, setFormError)
  useRenderPipelineEffect(pipeline, saveState, setMissingSchemaDialog)

  const onConnectorUpdateSuccess = (connector: ConnectorDescr, oldConnectorName: string) => {
    console.log('onConnectorUpdateSuccess')
    invalidateQuery(queryClient, PipelineManagerQuery.pipelineStatus(pipeline.name))
    setQueryData(queryClient, PipelineManagerQuery.pipelineStatus(pipeline.name), updatePipelineConnectorName(oldConnectorName, connector.name))
  }

  return (
    <>
      <BreadcrumbsHeader>
        <Link href={`/streaming/management`} data-testid='button-breadcrumb-pipelines'>
          Pipelines
        </Link>
        <Link href={`/streaming/builder/?pipeline_name=${pipeline.name}`} data-testid='button-breadcrumb-pipeline-name'>
          {pipeline.name}
        </Link>
      </BreadcrumbsHeader>
      <Grid container spacing={6} className='match-height' sx={{ pl: 6, pt: 6 }}>
        <Grid item xs={12}>
          <Card>
            <CardContent>
              <Metadata errors={formError} {...{ pipeline, updatePipeline }} disabled={saveState === 'isLoading'} />
            </CardContent>
            <CardContent>
              <Grid item xs={12}>
                <EntitySyncIndicator getLabel={stateToSaveLabel} state={saveState} />
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
        program_name={pipeline.program_name ?? undefined}
      />
      {(connectorName =>
        connectorName && (
          <UnknownConnectorDialog
            {...showOnHash('edit/connector')}
            connectorName={connectorName}
            existingTitle={name => 'Update ' + name}
            submitButton={
              <Button
                variant='contained'
                color='success'
                endIcon={<IconCheck />}
                type='submit'
                data-testid='button-update'
              >
                Update
              </Button>
            }
            onSuccess={onConnectorUpdateSuccess}
          />
        ))(/edit\/connector\/([\w-]+)/.exec(hash)?.[1])}
      {(connectorName =>
        connectorName && (
          <UnknownConnectorDialog
            show={hash.startsWith('view/connector')}
            setShow={() => router.back()}
            connectorName={connectorName}
            existingTitle={name => 'Inspect ' + name}
            submitButton={<></>}
            disabled={true}
          />
        ))(/view\/connector\/([\w-]+)/.exec(hash)?.[1])}
    </>
  )
}

const defaultPipelineData: Pipeline = {
  descriptor: {
    attached_connectors: [],
    config: { workers: 8 },
    description: '',
    name: '',
    pipeline_id: '',
    program_name: '',
    version: NaN
  },
  state: {
    current_status: PipelineStatus.FAILED,
    desired_status: PipelineStatus.FAILED
  } as PipelineRuntimeState
}

export default () => {
  const pipelineName = useSearchParams().get('pipeline_name') || ''
  // const { pushMessage } = useStatusNotification()
  const queryClient = useQueryClient()
  const formError = useBuilderState(s => s.formError)
  const setFormError = useBuilderState(s => s.setFormError)
  const saveState = useBuilderState(s => s.saveState)
  const setSaveState = useBuilderState(s => s.setSaveState)
  const setPipelineName = useBuilderState(s => s.setPipelineName)
  // const [formError, setFormError] = useState<FormError>({})
  // const [saveState, setSaveState] = useState<EntitySyncIndicatorStatus>(pipelineName ? 'isLoading' : 'isNew')

  // If opening a page without queried pipelineName - set status to isNew
  useEffect(() => {
    if (pipelineName) {
      return
    }
    setSaveState('isNew')
  }, [pipelineName, setSaveState])

  // Clear the data when creating new pipeline
  useEffect(() => {
    if (pipelineName) {
      return
    }
    console.log('set empty named pipeline')
    setQueryData(queryClient, PipelineManagerQuery.pipelineStatus(pipelineName), defaultPipelineData)
    setSaveState('isNew')
  }, [pipelineName, queryClient, setSaveState])

  const pipelineQuery = useQuery({
    ...PipelineManagerQuery.pipelineStatus(pipelineName),
    enabled: !!pipelineName, //&& saveState !== 'isSaving' && saveState !== 'isModified',
    initialData: defaultPipelineData
  })
  const pipeline = pipelineQuery.data?.descriptor
  invariant(pipeline, 'Pipeline should be initialized with a default value')

  useEffect(() => {
    setPipelineName(pipeline.name)
  }, [pipeline.name, setPipelineName])

  // Clear loading state when pipeline is fetched
  useEffect(() => {
    if (!pipeline.pipeline_id) {
      return
    }
    setSaveState('isUpToDate')
  }, [pipeline.pipeline_id, setSaveState])

  // const { mutate: updatePipeline } = useMutation(mutationUpdatePipeline(queryClient))
  const updatePipeline = useUpdatePipeline(pipelineName, setSaveState, setFormError)

  return (
    <ReactFlowProvider>
      <PipelineBuilderPage
        {...{
          pipeline,
          updatePipeline,
          saveState,
          setSaveState,
          formError,
          setFormError
        }}
      />
    </ReactFlowProvider>
  )
}

const useCreatePipelineEffect = (
  pipeline: PipelineDescr,
  updatePipeline: Dispatch<SetStateAction<UpdatePipelineRequest>>,
  setStatus: Dispatch<EntitySyncIndicatorStatus>,
  setFormError: Dispatch<FormError>
) => {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()
  const router = useRouter()

  const { mutate } = useMutation(mutationCreatePipeline(queryClient))
  const createPipeline = (pipeline: NewPipelineRequest) => {
    invariant(pipeline.name, 'Cannot create a pipeline with an empty name!')
    setStatus('isSaving')
    return mutate(pipeline, {
      onSuccess: (_data: NewPipelineResponse) => {
        // setProgram((prevState: ProgramDescr) => ({
        //   ...prevState,
        //   version: data.version,
        //   program_id: data.program_id
        // }))
        if (!pipeline.name) {
          setFormError({ name: { message: 'Enter a name for the project.' } })
        }
        setStatus('isUpToDate')
        router.push(`/streaming/builder/?pipeline_name=${pipeline.name}`)
        setFormError({})
      },
      onError: (error: ApiError) => {
        // TODO: would be good to have error codes from the API
        if (error.message.includes('name already exists')) {
          setFormError({ name: { message: 'This name is already used. Enter a different name.' } })
          setStatus('isNew')
        } else {
          pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
        }
      }
    })
  }
  const createPipelineDebounced = useDebouncedCallback(() => {
    if (!pipeline.name || pipeline.pipeline_id) {
      return
    }
    return createPipeline({
      name: pipeline.name,
      description: pipeline.description,
      config: pipeline.config,
      program_name: pipeline.program_name || undefined,
      connectors: pipeline.attached_connectors
    })
  }, SAVE_DELAY)
  useEffect(() => createPipelineDebounced(), [pipeline, createPipelineDebounced])
}
