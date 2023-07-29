// Define a default query function that will receive the query key and decide
// what API to call based on the query key.
//
//
// This interplays with react-query in the following way:
// -  The defaultQueryFn is installed in the QueryClientProvider in _app.tsx
// -  The defaultQueryFn is used in the useQuery hook in the components whenever
//    no queryFn is provided (ideally we never provide a queryFn to reduce
//    duplication and bugs but always rely on defaultQueryFn to route to the correct API call)

import { QueryClient, QueryFunctionContext } from '@tanstack/react-query'
import { match, P } from 'ts-pattern'
import {
  ConnectorsService,
  Pipeline,
  PipelineId,
  PipelinesService,
  PipelineStatus,
  ProgramDescr,
  ProgramId,
  ProgramsService,
  UpdateProgramRequest
} from './manager'

// Updates the query cache for a `UpdateProgramRequest` response.
export const projectQueryCacheUpdate = (
  queryClient: QueryClient,
  programId: ProgramId,
  newData: UpdateProgramRequest
) => {
  queryClient.setQueryData(['programCode', { program_id: programId }], (oldData: ProgramDescr | undefined) => {
    return oldData
      ? {
          ...oldData,
          name: newData.name,
          description: newData.description ? newData.description : oldData.description,
          code: newData.code ? newData.code : oldData.code
        }
      : oldData
  })

  queryClient.setQueryData(['programStatus', { program_id: programId }], (oldData: ProgramDescr | undefined) => {
    return oldData
      ? {
          ...oldData,
          ...{ name: newData.name, description: newData.description ? newData.description : oldData.description }
        }
      : oldData
  })

  queryClient.setQueryData(['program'], (oldData: ProgramDescr[] | undefined) =>
    oldData?.map((project: ProgramDescr) => {
      if (project.program_id === programId) {
        const projectDescUpdates = {
          name: newData.name,
          description: newData.description ? newData.description : project.description
        }
        return { ...project, ...projectDescUpdates }
      } else {
        return project
      }
    })
  )
}

// Updates the query cache for a pipeline status change.
export const pipelineStatusQueryCacheUpdate = (
  queryClient: QueryClient,
  pipeline_id: PipelineId,
  newStatus: PipelineStatus
) => {
  queryClient.setQueryData(['pipeline'], (oldData: Pipeline[] | undefined) =>
    oldData?.map((p: Pipeline) => {
      if (p.descriptor.pipeline_id === pipeline_id) {
        return { ...p, state: { ...p.state, desired_status: newStatus } }
      } else {
        return p
      }
    })
  )
  queryClient.setQueryData(['pipelineStatus', { pipeline_id: pipeline_id }], (oldData: Pipeline | undefined) => {
    return oldData
      ? {
          ...oldData,
          state: { ...oldData.state, desired_status: newStatus }
        }
      : oldData
  })
}

export const invalidatePipeline = (queryClient: QueryClient, pipeline_id: PipelineId) => {
  queryClient.invalidateQueries(['pipelineLastRevision', { pipeline_id: pipeline_id }])
  queryClient.invalidateQueries(['pipelineStatus', { pipeline_id: pipeline_id }])
  queryClient.invalidateQueries(['pipelineConfig', { pipeline_id: pipeline_id }])
  queryClient.invalidateQueries(['pipelineValidate', { pipeline_id: pipeline_id }])
  queryClient.invalidateQueries(['pipeline'])
}

export const defaultQueryFn = async (context: QueryFunctionContext) => {
  return match(context.queryKey)
    .with(['programCode', { program_id: P.select() }], program_id => {
      if (typeof program_id == 'string') {
        return ProgramsService.getProgram(program_id, true)
      } else {
        throw new Error('Invalid query key, program_id should be a string')
      }
    })
    .with(['programStatus', { program_id: P.select() }], program_id => {
      if (typeof program_id == 'string') {
        return ProgramsService.getProgram(program_id, false)
      } else {
        throw new Error('Invalid query key, program_id should be a string')
      }
    })
    .with(['pipelineStatus', { pipeline_id: P.select() }], pipeline_id => {
      if (typeof pipeline_id == 'string') {
        return PipelinesService.getPipeline(pipeline_id)
      } else {
        throw new Error('Invalid query key, pipeline_id should be a string')
      }
    })
    .with(['pipelineConfig', { pipeline_id: P.select() }], pipeline_id => {
      if (typeof pipeline_id == 'string') {
        return PipelinesService.getPipelineConfig(pipeline_id)
      } else {
        throw new Error('Invalid query key, pipeline_id should be a string')
      }
    })
    .with(['pipelineStats', { pipeline_id: P.select() }], pipeline_id => {
      if (typeof pipeline_id == 'string') {
        return PipelinesService.pipelineStats(pipeline_id)
      } else {
        throw new Error('Invalid query key, pipeline_id should be a string')
      }
    })
    .with(['pipelineLastRevision', { pipeline_id: P.select() }], pipeline_id => {
      if (typeof pipeline_id == 'string') {
        return PipelinesService.pipelineDeployed(pipeline_id)
      } else {
        throw new Error('Invalid query key, pipeline_id should be a string')
      }
    })
    .with(['pipelineValidate', { pipeline_id: P.select() }], pipeline_id => {
      if (typeof pipeline_id == 'string') {
        return PipelinesService.pipelineValidate(pipeline_id)
      } else {
        throw new Error('Invalid query key, pipeline_id should be a string')
      }
    })
    .with(['connectorStatus', { connector_id: P.select() }], connector_id => {
      if (typeof connector_id == 'string') {
        return ConnectorsService.getConnector(connector_id)
      } else {
        throw new Error('Invalid query key, connector_id should be a string')
      }
    })
    .with(['program'], () => ProgramsService.getPrograms())
    .with(['connector'], () => ConnectorsService.listConnectors())
    .with(['pipeline'], () => PipelinesService.listPipelines())
    .otherwise(() => {
      throw new Error('Invalid query key, maybe you need to update defaultQueryFn.ts')
    })
}
