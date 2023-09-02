// Define a default query function that will receive the query key and decide
// what API to call based on the query key.
//
//
// This interplays with react-query in the following way:
// -  The defaultQueryFn is installed in the QueryClientProvider in _app.tsx
// -  The defaultQueryFn is used in the useQuery hook in the components whenever
//    no queryFn is provided (ideally we never provide a queryFn to reduce
//    duplication and bugs but always rely on defaultQueryFn to route to the correct API call)

import { invalidateQuery, mkQuery, setQueryData } from '$lib/functions/common/tanstack'
import {
  ConnectorsService,
  Pipeline,
  PipelineId,
  PipelinesService,
  PipelineStatus,
  ProgramDescr,
  ProgramId,
  ProgramsService,
  ProgramStatus,
  UpdateProgramRequest
} from '$lib/services/manager'

import { QueryClient } from '@tanstack/react-query'

export const PipelineManagerQuery = mkQuery({
  program: () => ProgramsService.getPrograms(),
  programCode: (programId: string) => ProgramsService.getProgram(programId, true),
  programStatus: (programId: string) => ProgramsService.getProgram(programId, false),
  pipeline: () => PipelinesService.listPipelines(),
  pipelineStatus: PipelinesService.getPipeline,
  pipelineConfig: PipelinesService.getPipelineConfig,
  pipelineStats: PipelinesService.pipelineStats,
  pipelineLastRevision: PipelinesService.pipelineDeployed,
  pipelineValidate: PipelinesService.pipelineValidate,
  connector: () => ConnectorsService.listConnectors(),
  connectorStatus: ConnectorsService.getConnector
})

export const invalidatePipeline = (queryClient: QueryClient, pipelineId: PipelineId) => {
  invalidateQuery(queryClient, PipelineManagerQuery.pipelineLastRevision(pipelineId))
  invalidateQuery(queryClient, PipelineManagerQuery.pipelineStatus(pipelineId))
  invalidateQuery(queryClient, PipelineManagerQuery.pipelineConfig(pipelineId))
  invalidateQuery(queryClient, PipelineManagerQuery.pipelineValidate(pipelineId))
  invalidateQuery(queryClient, PipelineManagerQuery.pipeline())
}

// Updates just the program status in the query cache.
export const programStatusUpdate = (queryClient: QueryClient, programId: ProgramId, newStatus: ProgramStatus) => {
  setQueryData(queryClient, PipelineManagerQuery.programStatus(programId), (oldData: ProgramDescr | undefined) => {
    if (!oldData) {
      return oldData
    }
    return {
      ...oldData,
      status: newStatus
    }
  })
  setQueryData(queryClient, PipelineManagerQuery.program(), (oldData: ProgramDescr[] | undefined) => {
    return oldData?.map((item: ProgramDescr) => {
      if (item.program_id !== programId) {
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
  programId: ProgramId,
  newData: UpdateProgramRequest
) => {
  setQueryData(queryClient, PipelineManagerQuery.programCode(programId), (oldData: ProgramDescr | undefined) => {
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

  setQueryData(queryClient, PipelineManagerQuery.programStatus(programId), (oldData: ProgramDescr | undefined) => {
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
    PipelineManagerQuery.program(),
    (oldData: ProgramDescr[] | undefined) =>
      oldData?.map((project: ProgramDescr) => {
        if (project.program_id !== programId) {
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

// Updates the query cache for a pipeline status change.
export const pipelineStatusQueryCacheUpdate = (
  queryClient: QueryClient,
  pipelineId: PipelineId,
  newStatus: PipelineStatus
) => {
  setQueryData(
    queryClient,
    PipelineManagerQuery.pipeline(),
    (oldData: Pipeline[] | undefined) =>
      oldData?.map((p: Pipeline) => {
        if (p.descriptor.pipeline_id !== pipelineId) {
          return p
        }
        return { ...p, state: { ...p.state, desired_status: newStatus } }
      })
  )
  setQueryData(queryClient, PipelineManagerQuery.pipelineStatus(pipelineId), (oldData: Pipeline | undefined) => {
    if (!oldData) {
      return oldData
    }
    return {
      ...oldData,
      state: { ...oldData.state, desired_status: newStatus }
    }
  })
}
