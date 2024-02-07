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
  const [pipelines, programs, connectors] = await Promise.all([
    PipelinesService.listPipelines().then(es =>
      es.filter(e => e.descriptor.name.startsWith(prefix)).filter(partOfTheDemo)
    ),
    ProgramsService.getPrograms().then(es => es.filter(e => e.name.startsWith(prefix)).filter(partOfTheDemo)),
    ConnectorsService.listConnectors().then(es => es.filter(e => e.name.startsWith(prefix)).filter(partOfTheDemo))
  ])
  const stages = (() => {
    const programs = steps[0].entities.filter(e => e.type === 'program').length
    const connectors = programs + steps[0].entities.filter(e => e.type === 'connector').length
    const pipelines = connectors + steps[0].entities.filter(e => e.type === 'pipeline').length
    return {
      programs,
      connectors,
      pipelines
    }
  })()
  return {
    related: { pipelines, programs, connectors },
    getStage: (entityNumber: number) =>
      stages.pipelines < entityNumber
        ? ('pipelines' as const)
        : stages.connectors < entityNumber
          ? ('connectors' as const)
          : ('programs' as const)
  }
}

export const runDemoSetup = async ({ prefix, steps }: DemoSetup) => {
  const { related, getStage } = await relatedEntities({ prefix, steps })
  return {
    entities: steps.flatMap(step =>
      step.entities.map(e =>
        match(e)
          .with({ type: 'program' }, e => ({ ...e, exists: !!related.programs.find(r => r.name === prefix + e.name) }))
          .with({ type: 'connector' }, e => ({
            ...e,
            exists: !!related.connectors.find(r => r.name === prefix + e.name)
          }))
          .with({ type: 'pipeline' }, e => ({
            ...e,
            exists: !!related.pipelines.find(r => r.descriptor.name === prefix + e.name)
          }))
          .exhaustive()
      )
    ),
    setup: async function* () {
      const entityTotal = steps.reduce((acc, step) => acc + step.entities.length, 0)
      let entityNumber = 0
      if (steps.length) {
        yield { description: steps[0].name, ratio: 0, stage: 'programs' as const }
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
          yield {
            description: step.name,
            ratio: entityNumber / entityTotal,
            stage: getStage(entityNumber)
          }
        }
      }
    }
  }
}

type YieldType<T> = T extends AsyncGenerator<infer Yield, void, unknown> ? Yield : never

export type DemoSetupProgress = YieldType<ReturnType<Awaited<ReturnType<typeof runDemoSetup>>['setup']>> | 'done'

const cleanupDescription = {
  program: 'Cleaning up programs',
  pipelines: 'Cleaning up pipelines',
  connectors: 'Cleaning up connectors'
}

export const runDemoCleanup = async ({ prefix, steps }: DemoSetup) => {
  const { related, getStage } = await relatedEntities({ prefix, steps })
  return {
    related,
    cleanup: async function* () {
      const entityTotal = steps.reduce((acc, step) => acc + step.entities.length, 0)
      let entityNumber = 0
      yield { description: 'Cleaning up', ratio: 0, stage: 'pipelines' as const }
      for (const pipeline of related.pipelines) {
        await PipelinesService.pipelineDelete(pipeline.descriptor.name)
        yield {
          description: cleanupDescription.pipelines,
          ratio: entityNumber / entityTotal,
          stage: getStage(entityNumber)
        }
        ++entityNumber
      }
      for (const program of related.programs) {
        await ProgramsService.deleteProgram(program.name)
        yield {
          description: cleanupDescription.program,
          ratio: entityNumber / entityTotal,
          stage: getStage(entityNumber)
        }
        ++entityNumber
      }
      for (const connector of related.connectors) {
        await ConnectorsService.deleteConnector(connector.name)
        yield {
          description: cleanupDescription.connectors,
          ratio: entityNumber / entityTotal,
          stage: getStage(entityNumber)
        }
        ++entityNumber
      }
    }
  }
}
