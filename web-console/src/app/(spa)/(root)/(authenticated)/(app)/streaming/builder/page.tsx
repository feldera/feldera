'use client'

import { Breadcrumbs } from '$lib/components/common/BreadcrumbsHeader'
import { EntitySyncIndicator, EntitySyncIndicatorStatus } from '$lib/components/common/EntitySyncIndicator'
import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { UnknownConnectorDialog } from '$lib/components/connectors/dialogs/UnknownConnector'
import Metadata from '$lib/components/streaming/builder/Metadata'
import MissingSchemaDialog from '$lib/components/streaming/builder/NoSchemaDialog'
import { PipelineGraph } from '$lib/components/streaming/builder/PipelineBuilder'
import { connectorConnects, useAddConnector } from '$lib/compositions/streaming/builder/useAddIoNode'
import { useBuilderState } from '$lib/compositions/streaming/builder/useBuilderState'
import { useReplacePlaceholder } from '$lib/compositions/streaming/builder/useSqlPlaceholderClick'
import { useUpdatePipeline } from '$lib/compositions/streaming/builder/useUpdatePipeline'
import { useHashPart } from '$lib/compositions/useHashPart'
import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
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
  PipelineStatus as RawPipelineStatus,
  UpdatePipelineRequest
} from '$lib/services/manager'
import {
  mutationCreatePipeline,
  PipelineManagerQueryKey,
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

import { Button, Card, CardContent } from '@mui/material'
import Grid from '@mui/material/Grid'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

const SAVE_DELAY = 1000

interface FormError {
  name?: { message?: string }
}

const stateToSaveLabel = (state: EntitySyncIndicatorStatus): string =>
  match(state)
    .with('isModified', () => {
      return 'Modified'
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

const useCreatePipelineEffect = (
  pipeline: PipelineDescr,
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
          setFormError({ name: { message: 'This name is already in use. Enter a different name.' } })
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

const detachConnector = (c: AttachedConnector) => ({ ...c, relation_name: '' }) as AttachedConnector

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
  const pipelineManagerQuery = usePipelineManagerQuery()
  const projectsQuery = useQuery({
    ...pipelineManagerQuery.programs(),
    refetchInterval: 2000
  })
  const connectorsQuery = useQuery({
    ...pipelineManagerQuery.connectors(),
    refetchInterval: 2000
  })
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

    validConnections.forEach(attached_connector => {
      const connector = connectorsQuery.data.find(connector => connector.name === attached_connector.connector_name)
      if (connector) {
        addConnector(connector, attached_connector)
      }
    })

    {
      // Rename detached connectors when the connector name was changed in popup dialog
      setNodes(
        getNodes().map(node => {
          if (node.type !== 'inputNode' && node.type !== 'outputNode') {
            return node
          }
          invariant(((data: any): data is { connector: ConnectorDescr } => true)(node.data))
          invariant(node.data.connector.name, 'Connector name should be stored in the connector node')
          const realConnector = (data =>
            connectorsQuery.data.find(c => c.connector_id === data.connector.connector_id))(node.data)
          if (!realConnector) {
            return node
          }
          if (node.data.connector.name === realConnector.name) {
            return node
          }
          return {
            ...node,
            data: {
              ...node.data,
              connector: realConnector,
              ac: {
                ...node.data.ac,
                connector_name: realConnector.name
              }
            }
          }
        })
      )
    }
  }

  useEffect(render, [
    connectorsQuery.data,
    pipeline.program_name,
    pipeline.attached_connectors,
    projectsQuery.data,
    replacePlaceholder,
    addConnector,
    pushMessage,
    saveState,
    setNodes,
    getNodes,
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

  useCreatePipelineEffect(pipeline, setSaveState, setFormError)
  useRenderPipelineEffect(pipeline, saveState, setMissingSchemaDialog)

  const onConnectorUpdateSuccess = (connector: ConnectorDescr, oldConnectorName: string) => {
    invalidateQuery(queryClient, PipelineManagerQueryKey.pipelineStatus(pipeline.name))
    setQueryData(
      queryClient,
      PipelineManagerQueryKey.pipelineStatus(pipeline.name),
      updatePipelineConnectorName(oldConnectorName, connector.name)
    )
  }

  return (
    <>
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
                endIcon={<i className={`bx bx-check`} style={{}} />}
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
    desired_status: RawPipelineStatus.FAILED
  } as Pipeline['state']
}

export default () => {
  const pipelineName = useSearchParams().get('pipeline_name') || ''
  const queryClient = useQueryClient()
  const formError = useBuilderState(s => s.formError)
  const setFormError = useBuilderState(s => s.setFormError)
  const saveState = useBuilderState(s => s.saveState)
  const setSaveState = useBuilderState(s => s.setSaveState)
  const setPipelineName = useBuilderState(s => s.setPipelineName)
  const pipelineManagerQuery = usePipelineManagerQuery()

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
    setQueryData(queryClient, PipelineManagerQueryKey.pipelineStatus(pipelineName), defaultPipelineData)
    setSaveState('isNew')
  }, [pipelineName, queryClient, setSaveState])

  const pipelineQuery = useQuery({
    ...pipelineManagerQuery.pipelineStatus(pipelineName),
    enabled: !!pipelineName,
    initialData: defaultPipelineData,
    refetchOnWindowFocus: false
  })
  const pipeline = pipelineQuery.data?.descriptor
  invariant(pipeline, 'Pipeline should be initialized with a default value')

  useEffect(() => {
    setPipelineName(pipelineName)
  }, [pipelineName, setPipelineName])

  // Clear loading state when pipeline is fetched
  useEffect(() => {
    if (!pipeline.pipeline_id) {
      return
    }
    setSaveState('isUpToDate')
  }, [pipeline.pipeline_id, setSaveState])

  const updatePipeline = useUpdatePipeline(pipelineName, setSaveState, setFormError)

  return (
    <ReactFlowProvider>
      <Breadcrumbs.Header>
        <Breadcrumbs.Link href={`/streaming/management`} data-testid='button-breadcrumb-pipelines'>
          Pipelines
        </Breadcrumbs.Link>
        <Breadcrumbs.Link
          href={`/streaming/builder/?pipeline_name=${pipelineName}`}
          data-testid='button-breadcrumb-pipeline-name'
        >
          {pipelineName}
        </Breadcrumbs.Link>
      </Breadcrumbs.Header>
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
