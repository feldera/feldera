import type { DemoSetup } from '$lib/types/demo'
import {
  ConnectorDescr,
  ConnectorsService,
  Pipeline,
  PipelinesService,
  ProgramDescr,
  ProgramsService
} from '$lib/services/manager'
import { match } from 'ts-pattern'

const relatedEntities = async ({ prefix, steps }: DemoSetup) => {
  const partOfTheDemo = (target: Pipeline | ProgramDescr | ConnectorDescr) =>
    (name => !!steps.find(step => step.entities.find(entity => prefix + entity.name === name)))(
      'name' in target ? target.name : target.descriptor.name
    )
  const [relatedPipelines, relatedPrograms, relatedConnectors] = await Promise.all([
    PipelinesService.listPipelines().then(es =>
      es.filter(e => e.descriptor.name.startsWith(prefix)).filter(partOfTheDemo)
    ),
    ProgramsService.getPrograms().then(es => es.filter(e => e.name.startsWith(prefix)).filter(partOfTheDemo)),
    ConnectorsService.listConnectors().then(es => es.filter(e => e.name.startsWith(prefix)).filter(partOfTheDemo))
  ])
  return { relatedPipelines, relatedPrograms, relatedConnectors }
}

export const runDemoSetup = async ({ prefix, steps }: DemoSetup) => {
  const { relatedPipelines, relatedPrograms, relatedConnectors } = await relatedEntities({ prefix, steps })
  return {
    entities: steps.flatMap(step =>
      step.entities.map(e =>
        match(e)
          .with({ type: 'program' }, e => ({ ...e, exists: !!relatedPrograms.find(r => r.name === prefix + e.name) }))
          .with({ type: 'connector' }, e => ({
            ...e,
            exists: !!relatedConnectors.find(r => r.name === prefix + e.name)
          }))
          .with({ type: 'pipeline' }, e => ({
            ...e,
            exists: !!relatedPipelines.find(r => r.descriptor.name === prefix + e.name)
          }))
          .exhaustive()
      )
    ),
    setup: async function* () {
      const entityTotal = steps.reduce((acc, step) => acc + step.entities.length, 0)
      let entityNumber = 0
      if (steps.length) {
        yield { description: steps[0].name, ratio: 0 }
      }
      for (const step of steps) {
        for (const entity of step.entities) {
          await match(entity)
            .with({ type: 'program' }, e => ProgramsService.createOrReplaceProgram(prefix + e.name, e.body).then())
            .with({ type: 'connector' }, e =>
              ConnectorsService.createOrReplaceConnector(prefix + e.name, e.body).then()
            )
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
  }
}

const cleanupDescription = {
  program: 'Cleaning up programs',
  pipelines: 'Cleaning up pipelines',
  connectors: 'Cleaning up connectors'
}

export const runDemoCleanup = async ({ prefix, steps }: DemoSetup) => {
  const { relatedPipelines, relatedPrograms, relatedConnectors } = await relatedEntities({ prefix, steps })
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
