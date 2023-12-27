import { replaceElement } from '$lib/functions/common/array'
import { compose } from '$lib/functions/common/function'
import { getQueryData, invalidateQuery, mkQuery, setQueryData } from '$lib/functions/common/tanstack'
import {
  ApiError,
  ApiKeyDescr,
  ApiKeysService,
  AuthenticationService,
  CancelablePromise,
  ConnectorsService,
  PipelineId,
  PipelinesService,
  PipelineStatus as RawPipelineStatus,
  ProgramsService,
  ProgramStatus,
  ServicesService,
  UpdateProgramRequest
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
  pipelineStatus: (pipelineId: PipelineId) => ({
    ...pipelineStatus(pipelineId),
    structuralSharing<T>(oldData: T | undefined, newData: T) {
      invariant(((data: any): data is Pipeline | undefined => true)(oldData))
      invariant(((data: any): data is Pipeline => true)(newData))
      return consolidatePipelineStatus(newData, oldData) as T
    }
  })
}))(
  mkQuery({
    programs: () => ProgramsService.getPrograms(),
    // programCode: (programName: string) => ProgramsService.getProgram(programName, true), // TODO: Uncomment with an API update
    programCode: (programName: string) =>
      ProgramsService.getPrograms(undefined, programName, true).then(p =>
        p.length ? p[0] : Promise.reject(new Error('/v0/programs: ' + programName + ' not found'))
      ), // TODO: Temporary workaround
    // programStatus: (programId: string) => ProgramsService.getProgram(programId, false).then(v => v), // TODO: Uncomment with an API update
    programStatus: (programName: string) =>
      ProgramsService.getPrograms(undefined, programName, true)
        .then(p => p[0])
        .then(p => ProgramsService.getProgram(p.program_id, false)),
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
    connector: () => ConnectorsService.listConnectors(),
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

const getPipelineCache = (queryClient: QueryClient, pipelineId: PipelineId) => {
  const data =
    getQueryData(queryClient, PipelineManagerQuery.pipelines(), { predicate: cacheValid })?.find(
      p => p.descriptor.pipeline_id === pipelineId
    ) ?? getQueryData(queryClient, PipelineManagerQuery.pipelineStatus(pipelineId), { predicate: cacheValid })
  invariant(data, 'getPipelineCache')
  return data
}

export const mutationStartPipeline = (queryClient: QueryClient) =>
  ({
    mutationFn: pipelineId => {
      const currentStatus = getPipelineCache(queryClient, pipelineId).state.current_status
      if (currentStatus === PipelineStatus.RUNNING) {
        return new CancelablePromise<any>(resolve => resolve(undefined))
      }
      pipelineStatusQueryCacheUpdate(queryClient, pipelineId, 'current_status', PipelineStatus.STARTING)
      return PipelinesService.pipelineAction(pipelineId, 'start')
    },
    onSettled: (_data, _error, pipelineId) => {
      invalidatePipeline(queryClient, pipelineId)
    },
    onError: (_error, pipelineId) => {
      pipelineStatusQueryCacheUpdate(queryClient, pipelineId, 'current_status', PipelineStatus.STARTUP_FAILURE)
    }
  }) satisfies UseMutationOptions<string, ApiError, PipelineId>

export const mutationPausePipeline = (queryClient: QueryClient) =>
  ({
    mutationFn: pipelineId => {
      const currentStatus = getPipelineCache(queryClient, pipelineId).state.current_status
      if (currentStatus !== PipelineStatus.RUNNING) {
        return new CancelablePromise<any>(resolve => resolve(undefined))
      }
      pipelineStatusQueryCacheUpdate(queryClient, pipelineId, 'current_status', PipelineStatus.PAUSING)
      return PipelinesService.pipelineAction(pipelineId, 'pause')
    },
    onSettled: (_data, _error, pipelineId) => {
      invalidatePipeline(queryClient, pipelineId)
    },
    onError: (_error, pipelineId) => {
      pipelineStatusQueryCacheUpdate(queryClient, pipelineId, 'current_status', PipelineStatus.RUNNING)
    }
  }) satisfies UseMutationOptions<string, ApiError, PipelineId>

export const mutationShutdownPipeline = (queryClient: QueryClient) =>
  ({
    mutationFn: pipelineId => {
      const currentStatus = getPipelineCache(queryClient, pipelineId).state.current_status
      if (
        currentStatus !== PipelineStatus.PAUSED &&
        currentStatus !== PipelineStatus.RUNNING &&
        currentStatus !== PipelineStatus.FAILED
      ) {
        return new CancelablePromise<any>(resolve => resolve(undefined))
      }
      pipelineStatusQueryCacheUpdate(queryClient, pipelineId, 'current_status', PipelineStatus.SHUTTING_DOWN)
      return PipelinesService.pipelineAction(pipelineId, 'shutdown')
    },
    onSettled: (_data, _error, pipelineId) => {
      invalidatePipeline(queryClient, pipelineId)
    },
    onError: (_error, pipelineId) => {
      pipelineStatusQueryCacheUpdate(queryClient, pipelineId, 'current_status', PipelineStatus.PAUSED)
    }
  }) satisfies UseMutationOptions<string, ApiError, PipelineId>

export const mutationDeletePipeline = (queryClient: QueryClient) =>
  ({
    mutationFn: pipelineId => {
      const currentStatus = getPipelineCache(queryClient, pipelineId).state.current_status
      if (currentStatus !== PipelineStatus.SHUTDOWN) {
        return new CancelablePromise<any>(resolve => resolve(undefined))
      }
      return PipelinesService.pipelineDelete(pipelineId)
    },
    onSettled: (_data, _error, pipelineId) => {
      invalidateQuery(queryClient, PipelineManagerQuery.pipelines())
      invalidateQuery(queryClient, PipelineManagerQuery.pipelineStatus(pipelineId))
    },
    onError: (_error, _pipelineId) => {}
  }) satisfies UseMutationOptions<string, ApiError, PipelineId>

export const invalidatePipeline = (queryClient: QueryClient, pipelineId: PipelineId) => {
  invalidateQuery(queryClient, PipelineManagerQuery.pipelineLastRevision(pipelineId))
  invalidateQuery(queryClient, PipelineManagerQuery.pipelineStatus(pipelineId))
  invalidateQuery(queryClient, PipelineManagerQuery.pipelineConfig(pipelineId))
  invalidateQuery(queryClient, PipelineManagerQuery.pipelineValidate(pipelineId))
  invalidateQuery(queryClient, PipelineManagerQuery.pipelines())
}

// Updates just the program status in the query cache.
export const programStatusUpdate = (queryClient: QueryClient, programName: string, newStatus: ProgramStatus) => {
  setQueryData(queryClient, PipelineManagerQuery.programStatus(programName), oldData => {
    if (!oldData) {
      return oldData
    }
    return {
      ...oldData,
      status: newStatus
    }
  })
  setQueryData(queryClient, PipelineManagerQuery.programs(), oldData => {
    return oldData?.map(item => {
      if (item.name !== programName) {
        return item
      }
      return {
        ...item,
        status: newStatus
      }
    })
  })
}

// Updates the query cache for a `UpdateProgramRequest` response.
export const programQueryCacheUpdate = (
  queryClient: QueryClient,
  programName: string,
  newData: UpdateProgramRequest
) => {
  setQueryData(queryClient, PipelineManagerQuery.programCode(programName), oldData => {
    if (!oldData) {
      return oldData
    }
    return {
      ...oldData,
      name: newData.name,
      description: newData.description ? newData.description : oldData.description,
      code: newData.code ? newData.code : oldData.code
    }
  })

  setQueryData(queryClient, PipelineManagerQuery.programStatus(programName), oldData => {
    if (!oldData) {
      return oldData
    }
    return {
      ...oldData,
      ...{ name: newData.name, description: newData.description ? newData.description : oldData.description }
    }
  })

  setQueryData(
    queryClient,
    PipelineManagerQuery.programs(),
    oldData =>
      oldData?.map(project => {
        if (project.name !== programName) {
          return project
        }
        return {
          ...project,
          name: newData.name,
          description: newData.description ? newData.description : project.description
        }
      })
  )
}

const pipelineStatusQueryCacheUpdate = (
  queryClient: QueryClient,
  pipelineId: string,
  field: 'current_status' | 'desired_status',
  status: PipelineStatus
) => {
  setQueryData(queryClient, PipelineManagerQuery.pipelines(), oldData => {
    if (!oldData) {
      return oldData
    }
    return replaceElement(oldData, p =>
      p.descriptor.pipeline_id !== pipelineId ? null : { ...p, state: { ...p.state, [field]: status } }
    )
  })
  setQueryData(queryClient, PipelineManagerQuery.pipelineStatus(pipelineId), oldData => {
    if (!oldData) {
      return oldData
    }
    return { ...oldData, state: { ...oldData.state, [field]: status } }
  })
}
