import type { DemoSetup } from '$lib/types/demo'
import { ApiError, ConnectorsService, PipelinesService, ProgramsService } from '$lib/services/manager'
import { match } from 'ts-pattern'

export const runDemoSetup = async function* ({ prefix, steps }: DemoSetup) {
  const entityTotal = steps.reduce((acc, step) => acc + step.entities.length, 0)
  let entityNumber = 0
  if (steps.length) {
    yield { description: steps[0].name, ratio: 0 }
  }
  for (const step of steps) {
    for (const entity of step.entities) {
      await match(entity)
        .with({ type: 'program' }, e => ProgramsService.createOrReplaceProgram(prefix + e.name, e.body).then())
        .with({ type: 'connector' }, e => ConnectorsService.createOrReplaceConnector(prefix + e.name, e.body).then())
        .with({ type: 'pipeline' }, e =>
          PipelinesService.createOrReplacePipeline(prefix + e.name, {
            ...e.body,
            program_name: e.body.program_name ? prefix + e.body.program_name : e.body.program_name,
            connectors: e.body.connectors
              ? e.body.connectors.map(connector => ({
                  ...connector,
                  connector_name: prefix + connector.connector_name
                }))
              : e.body.connectors
          }).then()
        )
        .exhaustive()
      ++entityNumber
      yield { description: step.name, ratio: entityNumber / entityTotal }
    }
  }
}

const cleanupDescription = {
  program: 'Cleaning up programs',
  pipelines: 'Cleaning up pipelines',
  connectors: 'Cleaning up connectors'
}

export const runDemoCleanup = async ({ prefix, steps }: DemoSetup) => {
  const [relatedPipelines, relatedPrograms, relatedConnectors] = await Promise.all([
    PipelinesService.listPipelines().then(
      es => es.filter(e => e.descriptor.name.startsWith(prefix)),
      e => {
        if (e instanceof ApiError && (e.body as any).error_code === 'UnknownPipelineName') {
          return []
        }
        throw e
      }
    ),
    ProgramsService.getPrograms().then(
      es => es.filter(e => e.name.startsWith(prefix)),
      e => {
        if (e instanceof ApiError && (e.body as any).error_code === 'UnknownProgramName') {
          return []
        }
        throw e
      }
    ),
    ConnectorsService.listConnectors().then(
      es => es.filter(e => e.name.startsWith(prefix)),
      e => {
        if (e instanceof ApiError && (e.body as any).error_code === 'UnknownConnectorName') {
          return []
        }
        throw e
      }
    )
  ])
  return {
    relatedPipelines,
    relatedPrograms,
    relatedConnectors,
    cleanup: async function* () {
      const entityTotal = steps.reduce((acc, step) => acc + step.entities.length, 0)
      let entityNumber = 0
      yield { description: 'Cleaning up', ratio: 0 }
      for (const pipeline of relatedPipelines) {
        await PipelinesService.pipelineDelete(pipeline.descriptor.name)
        yield { description: cleanupDescription.pipelines, ratio: entityNumber / entityTotal }
        ++entityNumber
      }
      for (const program of relatedPrograms) {
        await ProgramsService.deleteProgram(program.name)
        yield { description: cleanupDescription.program, ratio: entityNumber / entityTotal }
        ++entityNumber
      }
      for (const connector of relatedConnectors) {
        await ConnectorsService.deleteConnector(connector.name)
        yield { description: cleanupDescription.connectors, ratio: entityNumber / entityTotal }
        ++entityNumber
      }
    }
  }
}
