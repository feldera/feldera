import Grid from '@mui/material/Grid'
import Typography from '@mui/material/Typography'
import PageHeader from 'src/layouts/components/page-header'

import { Card, CardContent } from '@mui/material'
import PipelineGraph from 'src/streaming/builder/PipelineBuilder'
import SaveIndicator, { SaveIndicatorState } from 'src/components/SaveIndicator'
import { match } from 'ts-pattern'
import Metadata from 'src/streaming/builder/Metadata'
import { useBuilderState } from 'src/streaming/builder/useBuilderState'
import { Dispatch, SetStateAction, useEffect, useState } from 'react'
import {
  AttachedConnector,
  ConfigId,
  ConfigService,
  ConnectorService,
  Direction,
  NewConfigResponse,
  ProjectService
} from 'src/types/manager'
import { useMutation, useQuery } from '@tanstack/react-query'
import { ReactFlowProvider, useReactFlow } from 'reactflow'
import { useDebouncedCallback } from 'use-debounce'
import { removePrefix } from 'src/utils'
import { useReplacePlaceholder } from 'src/streaming/builder/hooks/useSqlPlaceholderClick'
import { projectToProjectWithSchema } from 'src/types/program'
import { useAddConnector } from 'src/streaming/builder/hooks/useAddIoNode'
import MissingSchemaDialog from 'src/streaming/builder/NoSchemaDialog'

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
  configId: ConfigId | undefined
  setConfigId: Dispatch<SetStateAction<ConfigId | undefined>>
}) => {
  const [missingSchemaDialog, setMissingSchemaDialog] = useState(false)

  const { configId, setConfigId } = props
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

  const { mutate: newConfigMutate } = useMutation(ConfigService.newConfig)
  const { mutate: updateConfigMutate } = useMutation(ConfigService.updateConfig)
  const replacePlaceholder = useReplacePlaceholder()
  const addConnector = useAddConnector()

  const connectorQuery = useQuery({ queryKey: ['source'], queryFn: ConnectorService.listConnectors })

  const projects = useQuery({ queryKey: ['project'], queryFn: ProjectService.listProjects })
  const configQuery = useQuery({
    queryKey: ['configs', configId],
    queryFn: ({ queryKey }) => ConfigService.configStatus(queryKey[1] as number),
    enabled:
      configId !== undefined && saveState !== 'isSaving' && saveState !== 'isModified' && saveState !== 'isDebouncing'
  })
  useEffect(() => {
    if (
      !configQuery.isLoading &&
      !configQuery.isError &&
      !projects.isLoading &&
      !projects.isError &&
      !connectorQuery.isLoading &&
      !connectorQuery.isError
    ) {
      setConfigId(() => configQuery.data.config_id)
      setName(configQuery.data.name)
      setDescription(configQuery.data.description)
      setConfig(configQuery.data.config)
      setSaveState('isUpToDate')

      console.log(configQuery.data.attached_connectors)

      // We don't set saveState here because we don't want to override the
      // saveState (when we get the query result back). Otherwise it will cancel
      // in-progress saves.

      if (configQuery.data.project_id) {
        const foundProject = projects.data.find(p => p.project_id === configQuery.data.project_id)
        if (foundProject) {
          console.log('we found it', foundProject)
          if (!foundProject.schema) {
            setMissingSchemaDialog(true)
          }
          const programWithSchema = projectToProjectWithSchema(foundProject)
          setProject(programWithSchema)
          replacePlaceholder(programWithSchema)
        }
      }

      if (configQuery.data.attached_connectors) {
        const attachedConnectors = configQuery.data.attached_connectors
        attachedConnectors.forEach(attached_connector => {
          const connector = connectorQuery.data.find(
            connector => connector.connector_id === attached_connector.connector_id
          )
          if (connector) {
            addConnector(connector, attached_connector)
          }
        })
      }
    } else if (configId === undefined) {
      setProject(undefined)
      setSaveState('isNew')
      setName('')
      setDescription('')
      // TODO: Set to 8 for now, needs to be configurable eventually
      setConfig('workers: 8\n')
    }
  }, [
    connectorQuery.isLoading,
    connectorQuery.isError,
    connectorQuery.data,
    configQuery.isLoading,
    configQuery.isError,
    configQuery.data,
    projects.isLoading,
    projects.isError,
    projects.data,
    setConfigId,
    setName,
    setDescription,
    setConfig,
    setSaveState,
    setProject,
    replacePlaceholder,
    addConnector,
    configId
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

      // Create a new config
      if (configId === undefined) {
        newConfigMutate(
          {
            name,
            project_id: project?.project_id,
            description,
            config
          },
          {
            onError: error => {
              setSaveState('isUpToDate')
              console.log('error', error)
            },
            onSuccess: (data: NewConfigResponse) => {
              setConfigId(data.config_id)
              setSaveState('isUpToDate')
            }
          }
        )
      } else {
        console.log('update existing config')

        // Update an existing config
        const connectors: Array<AttachedConnector> = getEdges().map(edge => {
          const source = getNode(edge.source)
          const target = getNode(edge.target)
          const connector = source?.id === 'sql' ? target : source

          const ac = connector?.data.ac
          console.log('edge.sourceHandle', edge.sourceHandle, 'edge', edge)
          const tableOrView =
            ac.direction === Direction.INPUT
              ? removePrefix(edge.targetHandle || '', 'table-')
              : removePrefix(edge.sourceHandle || '', 'view-')
          ac.config = tableOrView

          return ac
        })

        const updateRequest = {
          config_id: configId,
          name,
          description,
          project_id: project?.project_id,
          config,
          connectors
        }

        console.log('updateRequest', updateRequest)

        updateConfigMutate(updateRequest, {
          onError: error => {
            console.log('error', error)
            setSaveState('isUpToDate')
          },
          onSuccess: () => {
            setSaveState('isUpToDate')
          }
        })
      }
    }
  }, [
    saveState,
    debouncedSave,
    setSaveState,
    setConfigId,
    updateConfigMutate,
    newConfigMutate,
    configId,
    project,
    name,
    description,
    config,
    getNode,
    getEdges
  ])

  return (
    <>
      <Grid container spacing={6} className='match-height'>
        <PageHeader
          title={<Typography variant='h5'>Pipeline Creator</Typography>}
          subtitle={<Typography variant='body2'>Define an end-to-end pipeline with analytics.</Typography>}
        />
        <Grid item xs={12}>
          <Card>
            <CardContent>
              <Metadata errors={{}} />
            </CardContent>
            <CardContent>
              <Grid item xs={12}>
                <SaveIndicator stateToLabel={stateToSaveLabel} state={saveState} />
              </Grid>
            </CardContent>
          </Card>
        </Grid>

        <div style={{ width: '100vw', height: '60vh' }}>
          <PipelineGraph />
        </div>
      </Grid>
      <MissingSchemaDialog open={missingSchemaDialog} project_id={project?.project_id} />
    </>
  )
}

const Pipeline = () => {
  const [configId, setConfigId] = useState<ConfigId | undefined>(undefined)

  return (
    <ReactFlowProvider>
      <PipelineWithProvider configId={configId} setConfigId={setConfigId} />
    </ReactFlowProvider>
  )
}

export default Pipeline
