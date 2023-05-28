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
  ConnectorService,
  PipelineService,
  ProgramCodeResponse,
  ProgramDescr,
  ProgramService,
  UpdateProgramRequest
} from './manager'

// Updates the query cache for a `UpdateProgramRequest` response.
export const projectQueryCacheUpdate = (queryClient: QueryClient, newData: UpdateProgramRequest) => {
  queryClient.setQueryData(
    ['projectCode', { program_id: newData.program_id }],
    (oldData: ProgramCodeResponse | undefined) => {
      if (oldData) {
        const newd = {
          ...oldData,
          ...{
            project: {
              ...oldData.program,
              ...{
                name: newData.name,
                description: newData.description ? newData.description : oldData.program.description
              }
            },
            code: newData.code ? newData.code : oldData.code
          }
        }
        console.log('newdata is')
        console.log(newd)
        return newd
      } else {
        return oldData
      }
    }
  )

  queryClient.setQueryData(
    ['programStatus', { program_id: newData.program_id }],
    (oldData: ProgramDescr | undefined) => {
      return oldData
        ? {
          ...oldData,
          ...{ name: newData.name, description: newData.description ? newData.description : oldData.description }
        }
        : oldData
    }
  )

  queryClient.setQueryData(['program'], (oldData: ProgramDescr[] | undefined) =>
    oldData?.map((project: ProgramDescr) => {
      if (project.program_id === newData.program_id) {
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

export const defaultQueryFn = async (context: QueryFunctionContext) => {
  return match(context.queryKey)
    .with(['projectCode', { program_id: P.select() }], program_id => {
      if (typeof program_id == 'string') {
        return ProgramService.programCode(program_id)
      } else {
        throw new Error('Invalid query key, program_id should be a number')
      }
    })
    .with(['programStatus', { program_id: P.select() }], program_id => {
      if (typeof program_id == 'string') {
        return ProgramService.programStatus(program_id)
      } else {
        throw new Error('Invalid query key, program_id should be a number')
      }
    })
    .with(['pipelineStatus', { pipeline_id: P.select() }], pipeline_id => {
      if (typeof pipeline_id == 'string') {
        return PipelineService.pipelineMetadata(pipeline_id)
      } else {
        throw new Error('Invalid query key, pipeline_id should be a number')
      }
    })
    .with(['connectorStatus', { connector_id: P.select() }], connector_id => {
      if (typeof connector_id == 'string') {
        return ConnectorService.connectorStatus(connector_id)
      } else {
        throw new Error('Invalid query key, connector_id should be a number')
      }
    })
    .with(['program'], () => ProgramService.listPrograms())
    .with(['connector'], () => ConnectorService.listConnectors())
    .otherwise(() => {
      throw new Error('Invalid query key, maybe you need to update defaultQueryFn.ts')
    })
}
