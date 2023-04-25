// Define a default query function that will receive the query key and decide
// what API to call based on the query key.
//
//
// This interplays with react-query in the following way:
// -  The defaultQueryFn is installed in the QueryClientProvider in _app.tsx
// -  The defaultQueryFn is used in the useQuery hook in the components whenever
//    no queryFn is provided (ideally we never provide a queryFn to reduce
//    duplication and bugs but always rely on defaultQueryFn to route to the correct API call)

import { QueryFunctionContext } from '@tanstack/react-query'
import { match, P } from 'ts-pattern'

import { ConfigService, ConnectorService, PipelineService, ProjectService } from './manager'

export const defaultQueryFn = async (context: QueryFunctionContext) => {
  return match(context.queryKey)
    .with(['projectCode', { project_id: P.select() }], project_id => {
      if (typeof project_id == 'number') {
        return ProjectService.projectCode(project_id)
      } else {
        throw new Error('Invalid query key, project_id should be a number')
      }
    })
    .with(['projectStatus', { project_id: P.select() }], project_id => {
      if (typeof project_id == 'number') {
        return ProjectService.projectStatus(project_id)
      } else {
        throw new Error('Invalid query key, project_id should be a number')
      }
    })
    .with(['configStatus', { config_id: P.select() }], config_id => {
      if (typeof config_id == 'number') {
        return ConfigService.configStatus(config_id)
      } else {
        throw new Error('Invalid query key, config_id should be a number')
      }
    })
    .with(['pipelineStatus', { pipeline_id: P.select() }], pipeline_id => {
      if (typeof pipeline_id == 'number') {
        return PipelineService.pipelineStatus(pipeline_id)
      } else {
        throw new Error('Invalid query key, pipeline_id should be a number')
      }
    })
    .with(['connectorStatus', { connector_id: P.select() }], connector_id => {
      if (typeof connector_id == 'number') {
        return ConnectorService.connectorStatus(connector_id)
      } else {
        throw new Error('Invalid query key, connector_id should be a number')
      }
    })
    .with(['project'], () => ProjectService.listProjects())
    .with(['connector'], () => ConnectorService.listConnectors())
    .with(['configs'], () => ConfigService.listConfigs())
    .otherwise(() => {
      throw new Error('Invalid query key, maybe you need to update defaultQueryFn.ts')
    })
}
