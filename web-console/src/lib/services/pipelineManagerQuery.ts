import { compose } from '$lib/functions/common/function'
import { getQueryData, invalidateQuery, mkQuery, setQueryData } from '$lib/functions/common/tanstack'
import {
  ApiError,
  ApiKeyDescr,
  ApiKeysService,
  AttachedConnector,
  AuthenticationService,
  CancelablePromise,
  CompileProgramRequest,
  ConnectorsService,
  NewPipelineRequest,
  NewPipelineResponse,
  PipelinesService,
  PipelineStatus as RawPipelineStatus,
  ProgramDescr,
  ProgramId,
  ProgramsService,
  ProgramStatus,
  ServicesService,
  UpdateConnectorRequest,
  UpdateConnectorResponse,
  UpdatePipelineRequest,
  UpdatePipelineResponse,
  UpdateProgramRequest,
  UpdateProgramResponse
} from '$lib/services/manager'
import { Pipeline, PipelineStatus } from '$lib/types/pipeline'
import { leftJoin } from 'array-join'
import invariant from 'tiny-invariant'
import { match } from 'ts-pattern'

import { Query, QueryClient, UseMutationOptions } from '@tanstack/react-query'

const toClientPipelineStatus = (status: RawPipelineStatus) => {
  return match(status)
    .with(RawPipelineStatus.SHUTDOWN, () => PipelineStatus.SHUTDOWN)
    .with(RawPipelineStatus.PROVISIONING, () => PipelineStatus.PROVISIONING)
    .with(RawPipelineStatus.INITIALIZING, () => PipelineStatus.INITIALIZING)
    .with(RawPipelineStatus.PAUSED, () => PipelineStatus.PAUSED)
    .with(RawPipelineStatus.RUNNING, () => PipelineStatus.RUNNING)
    .with(RawPipelineStatus.SHUTTING_DOWN, () => PipelineStatus.SHUTTING_DOWN)
    .with(RawPipelineStatus.FAILED, () => PipelineStatus.FAILED)
    .exhaustive()
}

/**
 * Consolidate local STARTING | PAUSING | SHUTTING_DOWN status with remote PROVISIONING status
 * @param newPipeline
 * @param oldPipeline
 * @returns
 */
const consolidatePipelineStatus = (newPipeline: Pipeline, oldPipeline: Pipeline | undefined) => {
  const current_status =
    newPipeline.state.current_status !== toClientPipelineStatus(newPipeline.state.desired_status) &&
    newPipeline.state.current_status !== PipelineStatus.FAILED &&
    oldPipeline?.state.current_status &&
    [PipelineStatus.STARTING, PipelineStatus.PAUSING, PipelineStatus.SHUTTING_DOWN].includes(
      oldPipeline.state.current_status
    )
      ? oldPipeline.state.current_status
      : newPipeline.state.current_status
  return { ...newPipeline, state: { ...newPipeline.state, current_status } }
}

export const PipelineManagerQuery = (({ pipelines, pipelineStatus, ...queries }) => ({
  ...queries,
  pipelines: () => ({
    ...pipelines(),
    // Avoid displaying PROVISIONING status when local status is more detailed
    structuralSharing<T>(oldData: T | undefined, newData: T) {
      invariant(((data: any): data is Pipeline[] | undefined => true)(oldData))
      invariant(((data: any): data is Pipeline[] => true)(newData))
      return leftJoin(
        newData,
        oldData ?? [],
        p => p.descriptor.pipeline_id,
        p => p.descriptor.pipeline_id,
        consolidatePipelineStatus
      ) as T
    }
  }),
  pipelineStatus: (pipelineName: string) => ({
    ...pipelineStatus(pipelineName),
    structuralSharing<T>(oldData: T | undefined, newData: T) {
      invariant(((data: any): data is Pipeline | undefined => true)(oldData))
      invariant(((data: any): data is Pipeline => true)(newData))
      return consolidatePipelineStatus(newData, oldData) as T
    }
  })
}))(
  mkQuery({
    programs: () => ProgramsService.getPrograms(),
    programCode: (programName: string) => ProgramsService.getProgram(programName, true),
    programStatus: (programName: string) => ProgramsService.getProgram(programName, false),
    pipelines: () =>
      PipelinesService.listPipelines().then(ps =>
        ps.map(p => ({ ...p, state: { ...p.state, current_status: toClientPipelineStatus(p.state.current_status) } }))
      ),
    pipelineStatus: compose(PipelinesService.getPipeline, p =>
      p.then(p => ({ ...p, state: { ...p.state, current_status: toClientPipelineStatus(p.state.current_status) } }))
    ),
    pipelineConfig: PipelinesService.getPipelineConfig,
    pipelineStats: PipelinesService.pipelineStats,
    pipelineLastRevision: PipelinesService.pipelineDeployed,
    pipelineValidate: PipelinesService.pipelineValidate,
    connectors: () => ConnectorsService.listConnectors(),
    connectorStatus: ConnectorsService.getConnector,
    getAuthConfig: AuthenticationService.getAuthenticationConfig,
    listApiKeys: ApiKeysService.listApiKeys,
    getApiKey: ApiKeysService.getApiKey,
    listServices: ServicesService.listServices,
    getService: ServicesService.getService,
    newService: ServicesService.newService
  })
)

export const mutationGenerateApiKey = (queryClient: QueryClient) => ({
  mutationFn: ApiKeysService.createApiKey,
  onSettled: () => {
    invalidateQuery(queryClient, PipelineManagerQuery.listApiKeys())
  }
})

/**
 * Delete API key with optimistic cache update
 * @param queryClient
 * @returns
 */
export const mutationDeleteApiKey = (queryClient: QueryClient) =>
  ({
    mutationFn: ApiKeysService.deleteApiKey,
    onMutate: async name => {
      await queryClient.cancelQueries(PipelineManagerQuery.listApiKeys())
      const previous = getQueryData(queryClient, PipelineManagerQuery.listApiKeys())
      setQueryData(queryClient, PipelineManagerQuery.listApiKeys(), old => old?.filter(key => key.name !== name))
      return previous
    },
    onError: (_error, _name, context) => {
      setQueryData(queryClient, PipelineManagerQuery.listApiKeys(), context)
    },
    onSettled: () => {
      invalidateQuery(queryClient, PipelineManagerQuery.listApiKeys())
    }
  }) satisfies UseMutationOptions<{}, ApiError, string, ApiKeyDescr[]>

/**
 * Cache is considered valid if it was not invalidated and was set less than 10 seconds ago
 * @param query
 * @returns
 */
const cacheValid = (query: Query) => !query.isStaleByTime(10000)

const getPipelineCache = (queryClient: QueryClient, pipelineName: string) => {
  const data =
    getQueryData(queryClient, PipelineManagerQuery.pipelines(), { predicate: cacheValid })?.find(
      p => p.descriptor.name === pipelineName
    ) ?? getQueryData(queryClient, PipelineManagerQuery.pipelineStatus(pipelineName), { predicate: cacheValid })
  invariant(data, 'getPipelineCache')
  return data
}

export const mutationStartPipeline = (queryClient: QueryClient) =>
  ({
    mutationFn: pipelineName => {
      const currentStatus = getPipelineCache(queryClient, pipelineName).state.current_status
      if (currentStatus === PipelineStatus.RUNNING) {
        return new CancelablePromise<any>(resolve => resolve(undefined))
      }
      pipelineStatusQueryCacheUpdate(queryClient, pipelineName, 'current_status', PipelineStatus.STARTING)
      return PipelinesService.pipelineAction(pipelineName, 'start')
    },
    onSettled: (_data, _error, pipelineName) => {
      invalidatePipeline(queryClient, pipelineName)
    },
    onError: (_error, pipelineName) => {
      pipelineStatusQueryCacheUpdate(queryClient, pipelineName, 'current_status', PipelineStatus.STARTUP_FAILURE)
    }
  }) satisfies UseMutationOptions<string, ApiError, string>

export const mutationPausePipeline = (queryClient: QueryClient) =>
  ({
    mutationFn: pipelineName => {
      const currentStatus = getPipelineCache(queryClient, pipelineName).state.current_status
      if (currentStatus !== PipelineStatus.RUNNING) {
        return new CancelablePromise<any>(resolve => resolve(undefined))
      }
      pipelineStatusQueryCacheUpdate(queryClient, pipelineName, 'current_status', PipelineStatus.PAUSING)
      return PipelinesService.pipelineAction(pipelineName, 'pause')
    },
    onSettled: (_data, _error, pipelineName) => {
      invalidatePipeline(queryClient, pipelineName)
    },
    onError: (_error, pipelineName) => {
      pipelineStatusQueryCacheUpdate(queryClient, pipelineName, 'current_status', PipelineStatus.RUNNING)
    }
  }) satisfies UseMutationOptions<string, ApiError, string>

export const mutationShutdownPipeline = (queryClient: QueryClient) =>
  ({
    mutationFn: pipelineName => {
      const currentStatus = getPipelineCache(queryClient, pipelineName).state.current_status
      if (
        currentStatus !== PipelineStatus.PAUSED &&
        currentStatus !== PipelineStatus.RUNNING &&
        currentStatus !== PipelineStatus.FAILED
      ) {
        return new CancelablePromise<any>(resolve => resolve(undefined))
      }
      pipelineStatusQueryCacheUpdate(queryClient, pipelineName, 'current_status', PipelineStatus.SHUTTING_DOWN)
      return PipelinesService.pipelineAction(pipelineName, 'shutdown')
    },
    onSettled: (_data, _error, pipelineName) => {
      invalidatePipeline(queryClient, pipelineName)
    },
    onError: (_error, pipelineName) => {
      pipelineStatusQueryCacheUpdate(queryClient, pipelineName, 'current_status', PipelineStatus.PAUSED)
    }
  }) satisfies UseMutationOptions<string, ApiError, string>

export const mutationDeletePipeline = (queryClient: QueryClient) =>
  ({
    mutationFn: pipelineName => {
      const currentStatus = getPipelineCache(queryClient, pipelineName).state.current_status
      if (currentStatus !== PipelineStatus.SHUTDOWN) {
        return new CancelablePromise<any>(resolve => resolve(undefined))
      }
      return PipelinesService.pipelineDelete(pipelineName)
    },
    onSettled: (_data, _error, pipelineName) => {
      invalidateQuery(queryClient, PipelineManagerQuery.pipelines())
      invalidateQuery(queryClient, PipelineManagerQuery.pipelineStatus(pipelineName))
    },
    onError: (_error, _pipelineName) => {}
  }) satisfies UseMutationOptions<string, ApiError, string>

export const mutationUpdateProgram = (
  queryClient: QueryClient
): UseMutationOptions<
  UpdateProgramResponse,
  ApiError,
  { programName: ProgramId; update_request: UpdateProgramRequest }
> => ({
  mutationFn: args => {
    return ProgramsService.updateProgram(args.programName, args.update_request)
  },
  onMutate: async ({ programName, update_request }) => {
    const updateCache = <T extends UpdateProgramRequest>(old: T | undefined) =>
      !old
        ? undefined
        : {
            ...old,
            code: update_request.code ?? old.code,
            name: update_request.name ?? old.name,
            description: update_request.description ?? old.description
          }
    // Optimistic update
    setQueryData(queryClient, PipelineManagerQuery.programCode(update_request.name ?? programName), updateCache)
    setQueryData(queryClient, PipelineManagerQuery.programStatus(update_request.name ?? programName), updateCache)
    // For temporary updates of the cache of entities with an old name after the name is changed
    setQueryData(queryClient, PipelineManagerQuery.programCode(programName), updateCache)
    setQueryData(queryClient, PipelineManagerQuery.programStatus(programName), updateCache)
  },
  onSettled(_data, _error, variables, _context) {
    invalidateQuery(queryClient, PipelineManagerQuery.programStatus(variables.programName))
    invalidateQuery(queryClient, PipelineManagerQuery.programCode(variables.programName))
    invalidateQuery(queryClient, PipelineManagerQuery.programs())
  }
})

export const mutationCompileProgram = (
  queryClient: QueryClient
): UseMutationOptions<CompileProgramRequest, ApiError, { programName: string; request: CompileProgramRequest }> => ({
  mutationFn: args => {
    return ProgramsService.compileProgram(args.programName, args.request)
  },
  async onSettled(_data, _error, variables, _context) {
    invalidateQuery(queryClient, PipelineManagerQuery.programStatus(variables.programName))
    invalidateQuery(queryClient, PipelineManagerQuery.programCode(variables.programName))
    invalidateQuery(queryClient, PipelineManagerQuery.programs())
  }
})

export const mutationCreatePipeline = (
  queryClient: QueryClient
): UseMutationOptions<NewPipelineResponse, ApiError, NewPipelineRequest> => ({
  mutationFn: PipelinesService.newPipeline,
  onSettled: (_data, _error, variables, _context) => {
    invalidateQuery(queryClient, PipelineManagerQuery.pipelines())
    invalidateQuery(queryClient, PipelineManagerQuery.pipelineStatus(variables.name))
  }
})

export const mutationUpdatePipeline = (
  queryClient: QueryClient
): UseMutationOptions<UpdatePipelineResponse, ApiError, { pipelineName: string; request: UpdatePipelineRequest }> => ({
  mutationFn: args => {
    // TODO: leave plain call to 'PipelinesService.updatePipeline' once 'name' and 'description' fields become optional
    // a workaround because API requires 'name' and 'description' fields in an update request body
    // a workaround because when pipeline has a program we have to send program_name with each request
    // This workaround affects mutation logic in PipelineBuilder
    const pipeline = getQueryData(queryClient, PipelineManagerQuery.pipelineStatus(args.pipelineName))
    invariant(args.request.name ?? pipeline?.descriptor.name, 'Cannot update to an invalid name')
    return PipelinesService.updatePipeline(args.pipelineName, {
      ...args.request,
      name: args.request.name ?? pipeline?.descriptor.name,
      description: args.request.description ?? pipeline?.descriptor.description ?? '',
      program_name:
        args.request.program_name === undefined ? pipeline?.descriptor.program_name : args.request.program_name
    })
  },
  onSettled: (_data, _error, variables, _context) => {
    invariant(variables.pipelineName !== undefined, 'mutationUpdatePipeline: pipelineName === undefined')
    invalidatePipeline(queryClient, variables.pipelineName)
  },
  onSuccess: (_data, variables, _context) => {
    // It's important to update the query cache here because otherwise
    // sometimes the query cache will be out of date and the UI will
    // show the old connectors again after deletion.
    setQueryData(queryClient, PipelineManagerQuery.pipelineStatus(variables.pipelineName), oldData => {
      if (!oldData) {
        return oldData
      }
      return {
        state: oldData.state,
        descriptor: {
          name: variables.request.name ?? oldData.descriptor.name,
          description: variables.request.description ?? oldData.descriptor.description,
          program_name: variables.request.program_name ?? oldData.descriptor.program_name,
          config: variables.request.config ?? oldData.descriptor.config,
          attached_connectors: variables.request.connectors ?? oldData.descriptor.attached_connectors,
          pipeline_id: oldData.descriptor.pipeline_id,
          version: oldData.descriptor.version
        }
      }
    })
  }
})

export const updatePipelineConnectorName = (oldName: string, newName: string) => (pipeline?: Pipeline) =>
  pipeline
    ? {
        descriptor: {
          ...pipeline.descriptor,
          attached_connectors: pipeline.descriptor.attached_connectors.map(c =>
            c.connector_name === oldName ? ({ ...c, connector_name: newName } satisfies AttachedConnector) : c
          )
        },
        state: pipeline.state
      }
    : pipeline

export const mutationUpdateConnector = (
  queryClient: QueryClient
): UseMutationOptions<
  UpdateConnectorResponse,
  ApiError,
  { connectorName: string; request: UpdateConnectorRequest }
> => ({
  mutationFn: args => ConnectorsService.updateConnector(args.connectorName, args.request),
  onSettled: (_data, _errors, variables, _context) => {
    invalidateQuery(queryClient, PipelineManagerQuery.connectors())
    invalidateQuery(queryClient, PipelineManagerQuery.connectorStatus(variables.connectorName))
  }
})

export const invalidatePipeline = (queryClient: QueryClient, pipelineName: string) => {
  invalidateQuery(queryClient, PipelineManagerQuery.pipelineLastRevision(pipelineName))
  invalidateQuery(queryClient, PipelineManagerQuery.pipelineStatus(pipelineName))
  invalidateQuery(queryClient, PipelineManagerQuery.pipelineConfig(pipelineName))
  invalidateQuery(queryClient, PipelineManagerQuery.pipelineValidate(pipelineName))
  invalidateQuery(queryClient, PipelineManagerQuery.pipelines())
}

// Updates just the program status in the query cache.
export const updateProgramCacheStatus = (queryClient: QueryClient, programName: string, newStatus: ProgramStatus) => {
  const updateCache = <T extends ProgramDescr | undefined>(oldData: T) => {
    if (!oldData) {
      return oldData
    }
    return {
      ...oldData,
      status: newStatus
    }
  }
  setQueryData(queryClient, PipelineManagerQuery.programStatus(programName), updateCache)
  setQueryData(queryClient, PipelineManagerQuery.programCode(programName), updateCache)
  setQueryData(queryClient, PipelineManagerQuery.programs(), oldData => {
    return oldData?.map(item => {
      if (item.name !== programName) {
        return item
      }
      return updateCache(item)
    })
  })
}

// Updates the query cache for a `UpdateProgramRequest` response.
export const programQueryCacheUpdate = (
  queryClient: QueryClient,
  programName: string,
  newData: UpdateProgramRequest
) => {
  const updateCache = <T extends ProgramDescr | undefined>(oldData: T) => {
    if (!oldData) {
      return oldData
    }
    return {
      ...oldData,
      name: newData.name || oldData.name,
      description: newData.description ?? oldData.description,
      code: newData.code ?? oldData.code
    }
  }
  setQueryData(queryClient, PipelineManagerQuery.programCode(programName), updateCache)

  setQueryData(queryClient, PipelineManagerQuery.programStatus(programName), updateCache)

  setQueryData(
    queryClient,
    PipelineManagerQuery.programs(),
    oldDatas =>
      oldDatas?.map(oldData => {
        if (oldData.name !== programName) {
          return oldData
        }
        return updateCache(oldData)
      })
  )
}

const pipelineStatusQueryCacheUpdate = (
  queryClient: QueryClient,
  pipelineName: string,
  field: 'current_status' | 'desired_status',
  status: PipelineStatus
) => {
  const updateCache = <T extends Pipeline | undefined>(oldData: T) => {
    if (!oldData) {
      return oldData
    }
    return { ...oldData, state: { ...oldData.state, [field]: status } }
  }
  setQueryData(
    queryClient,
    PipelineManagerQuery.pipelines(),
    oldDatas =>
      oldDatas?.map(oldData => {
        if (oldData.descriptor.pipeline_id !== pipelineName) {
          return oldData
        }
        return updateCache(oldData)
      })
  )
  setQueryData(queryClient, PipelineManagerQuery.pipelineStatus(pipelineName), updateCache)
}

export const pipelineQueryCacheUpdate = (
  queryClient: QueryClient,
  pipelineName: string,
  newData: UpdatePipelineRequest
) => {
  const updateCache = <T extends Pipeline | undefined>(oldData: T) => {
    if (!oldData) {
      return oldData
    }
    return {
      state: oldData.state,
      descriptor: {
        attached_connectors:
          newData.connectors === undefined ? oldData.descriptor.attached_connectors : newData.connectors ?? [],
        config: newData.config === undefined ? oldData.descriptor.config : newData.config ?? {},
        description: newData.description ?? oldData.descriptor.description,
        name: newData.name ?? oldData.descriptor.name,
        pipeline_id: oldData.descriptor.pipeline_id,
        program_name: newData.program_name === undefined ? oldData.descriptor.program_name : newData.program_name,
        version: oldData.descriptor.version
      }
    } satisfies Pipeline
  }
  setQueryData(queryClient, PipelineManagerQuery.pipelineStatus(pipelineName), updateCache)

  setQueryData(
    queryClient,
    PipelineManagerQuery.pipelines(),
    oldDatas =>
      oldDatas?.map(oldData => {
        if (oldData.descriptor.name !== pipelineName) {
          return oldData
        }
        return updateCache(oldData)
      })
  )
}
