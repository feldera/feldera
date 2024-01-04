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

export const invalidatePipeline = (queryClient: QueryClient, pipelineName: string) => {
  invalidateQuery(queryClient, PipelineManagerQuery.pipelineLastRevision(pipelineName))
  invalidateQuery(queryClient, PipelineManagerQuery.pipelineStatus(pipelineName))
  invalidateQuery(queryClient, PipelineManagerQuery.pipelineConfig(pipelineName))
  invalidateQuery(queryClient, PipelineManagerQuery.pipelineValidate(pipelineName))
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
      name: newData.name || oldData.name,
      description: newData.description ?? oldData.description,
      code: newData.code ?? oldData.code
    }
  })

  setQueryData(queryClient, PipelineManagerQuery.programStatus(programName), oldData => {
    if (!oldData) {
      return oldData
    }
    return {
      ...oldData,
      ...{
        name: newData.name || oldData.name,
        description: newData.description ?? oldData.description,
        code: newData.code ?? oldData.code
      }
    }
  })

  setQueryData(
    queryClient,
    PipelineManagerQuery.programs(),
    oldDatas =>
      oldDatas?.map(oldData => {
        if (oldData.name !== programName) {
          return oldData
        }
        return {
          ...oldData,
          name: newData.name || oldData.name,
          description: newData.description ?? oldData.description,
          code: newData.code ?? oldData.code
        }
      })
  )
}

const pipelineStatusQueryCacheUpdate = (
  queryClient: QueryClient,
  pipelineName: string,
  field: 'current_status' | 'desired_status',
  status: PipelineStatus
) => {
  setQueryData(queryClient, PipelineManagerQuery.pipelines(), oldData => {
    if (!oldData) {
      return oldData
    }
    return replaceElement(oldData, p =>
      p.descriptor.pipeline_id !== pipelineName ? null : { ...p, state: { ...p.state, [field]: status } }
    )
  })
  setQueryData(queryClient, PipelineManagerQuery.pipelineStatus(pipelineName), oldData => {
    if (!oldData) {
      return oldData
    }
    return { ...oldData, state: { ...oldData.state, [field]: status } }
  })
}
