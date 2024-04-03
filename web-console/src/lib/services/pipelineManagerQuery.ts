import { assertUnion } from '$lib/functions/common/array'
import { compose } from '$lib/functions/common/function'
import { getQueryData, invalidateQuery, mkQuery, mkQueryKey, setQueryData } from '$lib/functions/common/tanstack'
import { getCaseIndependentName } from '$lib/functions/felderaRelation'
import { JSONXgressValue } from '$lib/functions/sqlValue'
import {
  ApiError,
  ApiKeyDescr,
  ApiKeysService,
  AttachedConnector,
  AuthenticationService,
  AuthProvider,
  CancelablePromise,
  ConfigurationService,
  ConnectorsService,
  HttpInputOutputService,
  JsonUpdateFormat,
  NewPipelineRequest,
  NewPipelineResponse,
  NewServiceRequest,
  NewServiceResponse,
  PipelinesService,
  PipelineStatus as RawPipelineStatus,
  ProgramDescr,
  ProgramId,
  ProgramsService,
  ProgramStatus,
  Relation,
  ServicesService,
  UpdateConnectorRequest,
  UpdateConnectorResponse,
  UpdatePipelineRequest,
  UpdatePipelineResponse,
  UpdateProgramRequest,
  UpdateProgramResponse,
  UpdateServiceRequest,
  UpdateServiceResponse
} from '$lib/services/manager'
import { Arguments } from '$lib/types/common/function'
import { ControllerStatus, Pipeline, PipelineStatus } from '$lib/types/pipeline'
import { leftJoin } from 'array-join'
import invariant from 'tiny-invariant'
import JSONbig from 'true-json-bigint'
import { match, P } from 'ts-pattern'

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

export type PipelineManagerQueryOptions = {
  /**
   * A callback that receives an error returned by the Pipeline Manager.
   * It is asynchronous, so enables to run an asynchronous action to determine
   * whether a query should be retried
   * @param error Error received from the API
   * @returns A Promise of whether a single retry of the query function should be performed
   */
  onError?: (error: ApiError) => Promise<boolean>
}

const defaultPipelineManagerQueryOptions = {
  onError: () => Promise.resolve(false)
}

export const PublicPipelineManagerQuery = mkQuery({
  getAuthConfig: () =>
    AuthenticationService.getAuthenticationConfig().then((c: AuthProvider) =>
      match(c)
        .with({ AwsCognito: P.select() }, config => ({
          AwsCognito: {
            ...config,
            // AWS Cognito's default grant type is `code`
            grantType: assertUnion(
              ['token', 'code'] as const,
              /response_type=(token|code)/.exec(config.login_url)?.[1] ?? 'code'
            )
          }
        }))
        .with({ GoogleIdentity: P.select() }, (_, value) => value)
        .with({} as any, () => 'NoAuth' as const) // Config may be an empty record
        .exhaustive()
    )
})

const PipelineManagerApi = {
  programs: () => ProgramsService.getPrograms(),
  programCode: (programName: string) => ProgramsService.getProgram(programName, true),
  programStatus: (programName: string) => ProgramsService.getProgram(programName, false),
  pipelines: () =>
    PipelinesService.listPipelines().then(ps =>
      ps.map(p => ({
        ...p,
        state: { ...p.state, current_status: toClientPipelineStatus(p.state.current_status) }
      }))
    ),
  pipelineStatus: compose(PipelinesService.getPipeline, p =>
    p.then(p => ({ ...p, state: { ...p.state, current_status: toClientPipelineStatus(p.state.current_status) } }))
  ),
  pipelineConfig: PipelinesService.getPipelineConfig,
  pipelineStats: (pipelineName: string) =>
    PipelinesService.pipelineStats(pipelineName) as unknown as CancelablePromise<ControllerStatus>,
  pipelineLastRevision: PipelinesService.pipelineDeployed,
  pipelineValidate: PipelinesService.pipelineValidate,
  connectors: () => ConnectorsService.listConnectors(),
  connectorStatus: ConnectorsService.getConnector,
  listApiKeys: ApiKeysService.listApiKeys,
  getApiKey: ApiKeysService.getApiKey,
  listServices: ServicesService.listServices,
  getService: ServicesService.getService,
  getDemos: () =>
    ConfigurationService.getDemos().then(demos =>
      Promise.all(
        demos.map(async url => {
          const demoBody = await fetch(url).then(r => r.json())
          return {
            url,
            title: demoBody.title,
            description: demoBody.description
          }
        })
      )
    )
}

export const makePipelineManagerQuery = ({
  onError = defaultPipelineManagerQueryOptions.onError
}: PipelineManagerQueryOptions) =>
  (({ pipelines, pipelineStatus, ...queries }) => ({
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
    mkQuery(PipelineManagerApi, {
      wrapQueryFn: f =>
        ((...args: Arguments<typeof f>) => {
          // A single retry is performed
          // Refactor to support arbitrary number of retries
          return f(...args).catch(async e => {
            if (!(e instanceof ApiError)) {
              throw e
            }
            const shouldRetry = await onError(e)
            if (!shouldRetry) {
              throw e
            }
            return f(...args)
          })
        }) as any
    })
  )

export const PipelineManagerQueryKey = mkQueryKey(PipelineManagerApi)

export const mutationGenerateApiKey = (queryClient: QueryClient) => ({
  mutationFn: ApiKeysService.createApiKey,
  onSettled: () => {
    invalidateQuery(queryClient, PipelineManagerQueryKey.listApiKeys())
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
      await queryClient.cancelQueries(PipelineManagerQueryKey.listApiKeys())
      const previous = getQueryData(queryClient, PipelineManagerQueryKey.listApiKeys())
      setQueryData(queryClient, PipelineManagerQueryKey.listApiKeys(), old => old?.filter(key => key.name !== name))
      return previous
    },
    onError: (_error, _name, context) => {
      setQueryData(queryClient, PipelineManagerQueryKey.listApiKeys(), context)
    },
    onSettled: () => {
      invalidateQuery(queryClient, PipelineManagerQueryKey.listApiKeys())
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
    getQueryData(queryClient, PipelineManagerQueryKey.pipelines(), { predicate: cacheValid })?.find(
      p => p.descriptor.name === pipelineName
    ) ?? getQueryData(queryClient, PipelineManagerQueryKey.pipelineStatus(pipelineName), { predicate: cacheValid })
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
    onSuccess: (_data, variables, _context) => {
      setQueryData(queryClient, PipelineManagerQueryKey.pipelineStats(variables), null)
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
      invalidateQuery(queryClient, PipelineManagerQueryKey.pipelines())
      invalidateQuery(queryClient, PipelineManagerQueryKey.pipelineStatus(pipelineName))
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
    invariant(args.update_request.name !== '', 'Cannot update a program to an invalid name')
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
    setQueryData(queryClient, PipelineManagerQueryKey.programCode(update_request.name ?? programName), updateCache)
    setQueryData(queryClient, PipelineManagerQueryKey.programStatus(update_request.name ?? programName), updateCache)
    // For temporary updates of the cache of entities with an old name after the name is changed
    setQueryData(queryClient, PipelineManagerQueryKey.programCode(programName), updateCache)
    setQueryData(queryClient, PipelineManagerQueryKey.programStatus(programName), updateCache)
  },
  onSuccess(_data, variables, _context) {
    invalidateQuery(queryClient, PipelineManagerQueryKey.programStatus(variables.programName))
    invalidateQuery(queryClient, PipelineManagerQueryKey.programCode(variables.programName))
    invalidateQuery(queryClient, PipelineManagerQueryKey.programs())
  }
})

export const mutationCreatePipeline = (
  queryClient: QueryClient
): UseMutationOptions<NewPipelineResponse, ApiError, NewPipelineRequest> => ({
  mutationFn: PipelinesService.newPipeline,
  onSettled: (_data, _error, variables, _context) => {
    invalidateQuery(queryClient, PipelineManagerQueryKey.pipelines())
    invalidateQuery(queryClient, PipelineManagerQueryKey.pipelineStatus(variables.name))
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
    const pipeline = getQueryData(queryClient, PipelineManagerQueryKey.pipelineStatus(args.pipelineName))
    invariant(args.request.name !== '', 'Cannot update a pipeline to an invalid name')
    return PipelinesService.updatePipeline(args.pipelineName, {
      ...args.request,
      name: args.request.name ?? args.pipelineName,
      description: args.request.description ?? pipeline?.descriptor.description ?? '',
      program_name:
        args.request.program_name === undefined ? pipeline?.descriptor.program_name : args.request.program_name
    })
  },
  onSuccess: (_data, variables, _context) => {
    invariant(variables.pipelineName !== undefined, 'mutationUpdatePipeline: pipelineName === undefined')
    invalidatePipeline(queryClient, variables.pipelineName)
    // It's important to update the query cache here because otherwise
    // sometimes the query cache will be out of date and the UI will
    // show the old connectors again after deletion.
    setQueryData(queryClient, PipelineManagerQueryKey.pipelineStatus(variables.pipelineName), oldData => {
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
    invalidateQuery(queryClient, PipelineManagerQueryKey.connectors())
    invalidateQuery(queryClient, PipelineManagerQueryKey.connectorStatus(variables.connectorName))
  }
})

export const invalidatePipeline = (queryClient: QueryClient, pipelineName: string) => {
  invalidateQuery(queryClient, PipelineManagerQueryKey.pipelineLastRevision(pipelineName))
  invalidateQuery(queryClient, PipelineManagerQueryKey.pipelineStatus(pipelineName))
  invalidateQuery(queryClient, PipelineManagerQueryKey.pipelineConfig(pipelineName))
  invalidateQuery(queryClient, PipelineManagerQueryKey.pipelineValidate(pipelineName))
  invalidateQuery(queryClient, PipelineManagerQueryKey.pipelines())
}

export const mutationCreateService = (
  queryClient: QueryClient
): UseMutationOptions<NewServiceResponse, ApiError, NewServiceRequest> => ({
  mutationFn: ServicesService.newService,
  onSuccess(_data, _variables, _context) {
    invalidateQuery(queryClient, PipelineManagerQueryKey.listServices())
  }
})

export const mutationUpdateService = (
  queryClient: QueryClient
): UseMutationOptions<UpdateServiceResponse, ApiError, { serviceName: string; request: UpdateServiceRequest }> => ({
  mutationFn: args => ServicesService.updateService(args.serviceName, args.request),
  onSuccess(_data, variables, _context) {
    invalidateQuery(queryClient, PipelineManagerQueryKey.listServices())
    invalidateQuery(queryClient, PipelineManagerQueryKey.getService(variables.serviceName))
  }
})

export const mutationDeleteService = (
  queryClient: QueryClient
): UseMutationOptions<void, ApiError, { serviceName: string }> => ({
  mutationFn: args => ServicesService.deleteService(args.serviceName),
  onSuccess(_data, variables, _context) {
    invalidateQuery(queryClient, PipelineManagerQueryKey.listServices())
    invalidateQuery(queryClient, PipelineManagerQueryKey.getService(variables.serviceName))
  }
})

/**
 * A React Query mutation that inserts data into the pipeline in an insert_delete JSON format
 */
export const mutationHttpIngressJson = (
  _queryClient: QueryClient
): UseMutationOptions<
  string,
  ApiError,
  {
    pipelineName: string
    relation: Relation
    force: boolean
    data: Partial<Record<'insert' | 'delete', Record<string, JSONXgressValue>>>[]
  }
> => ({
  mutationFn: ({ pipelineName, relation, force, data }) => {
    return HttpInputOutputService.httpInput(
      pipelineName,
      getCaseIndependentName(relation),
      force,
      'json',
      JSONbig.stringify(data),
      true,
      JsonUpdateFormat.INSERT_DELETE
    )
  }
})

// Updates just the program status in the query cache.
export const programStatusCacheUpdate = (queryClient: QueryClient, programName: string, newStatus: ProgramStatus) => {
  const updateCache = <T extends ProgramDescr | undefined>(oldData: T) => {
    if (!oldData) {
      return oldData
    }
    return {
      ...oldData,
      status: newStatus
    }
  }
  setQueryData(queryClient, PipelineManagerQueryKey.programStatus(programName), updateCache)
  setQueryData(queryClient, PipelineManagerQueryKey.programCode(programName), updateCache)
  setQueryData(queryClient, PipelineManagerQueryKey.programs(), oldData => {
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
      name: newData.name ?? oldData.name,
      description: newData.description ?? oldData.description,
      code: newData.code ?? oldData.code
    }
  }
  setQueryData(queryClient, PipelineManagerQueryKey.programCode(programName), updateCache)

  setQueryData(queryClient, PipelineManagerQueryKey.programStatus(programName), updateCache)

  setQueryData(queryClient, PipelineManagerQueryKey.programs(), oldDatas =>
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
  setQueryData(queryClient, PipelineManagerQueryKey.pipelines(), oldDatas =>
    oldDatas?.map(oldData => {
      if (oldData.descriptor.name !== pipelineName) {
        return oldData
      }
      return updateCache(oldData)
    })
  )
  setQueryData(queryClient, PipelineManagerQueryKey.pipelineStatus(pipelineName), updateCache)
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
  setQueryData(queryClient, PipelineManagerQueryKey.pipelineStatus(pipelineName), updateCache)

  setQueryData(queryClient, PipelineManagerQueryKey.pipelines(), oldDatas =>
    oldDatas?.map(oldData => {
      if (oldData.descriptor.name !== pipelineName) {
        return oldData
      }
      return updateCache(oldData)
    })
  )
}
