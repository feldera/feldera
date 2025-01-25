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
    ((name) =>
      !!steps.find((step) => step.entities.find((entity) => prefix + entity.name === name)))(
      'name' in target ? target.name : target.descriptor.name
    )
  const [pipelines, programs, connectors] = await Promise.all([
    PipelinesService.listPipelines().then((es) =>
      es.filter((e) => e.descriptor.name.startsWith(prefix)).filter(partOfTheDemo)
    ),
    ProgramsService.getPrograms().then((es) =>
      es.filter((e) => e.name.startsWith(prefix)).filter(partOfTheDemo)
    ),
    ConnectorsService.listConnectors().then((es) =>
      es.filter((e) => e.name.startsWith(prefix)).filter(partOfTheDemo)
    )
  ])
  return {
    related: { pipelines, programs, connectors }
  }
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

export const runDemoSetup = async ({ prefix, steps }: DemoSetup) => {
  const { related } = await relatedEntities({ prefix, steps })
  return {
    entities: steps.flatMap((step) =>
      step.entities.map((e) =>
        match(e)
          .with({ type: 'program' }, (e) => ({
            ...e,
            exists: !!related.programs.find((r) => r.name === prefix + e.name)
          }))
          .with({ type: 'connector' }, (e) => ({
            ...e,
            exists: !!related.connectors.find((r) => r.name === prefix + e.name)
          }))
          .with({ type: 'pipeline' }, (e) => ({
            ...e,
            exists: !!related.pipelines.find((r) => r.descriptor.name === prefix + e.name)
          }))
          .exhaustive()
      )
    ),
    setup: async function* () {
      const entityTotal = steps.reduce((acc, step) => acc + step.entities.length, 0)
      let entityNumber = 0
      if (steps.length) {
        yield { description: steps[0].name, ratio: 0, stage: 'program' as const }
      }
      for (const step of steps) {
        for (const entity of step.entities) {
          ++entityNumber
          yield {
            description: step.name,
            ratio: entityNumber / entityTotal,
            stage: entity.type
          }
          await match(entity)
            .with({ type: 'program' }, async (e) => {
              await sleep(500)
              ProgramsService.createOrReplaceProgram(prefix + e.name, e.body).then(() => sleep(0))
            })
            .with({ type: 'connector' }, async (e) => {
              await sleep(150)
              return ConnectorsService.createOrReplaceConnector(prefix + e.name, e.body).then(() =>
                sleep(0)
              )
            })
            .with({ type: 'pipeline' }, async (e) => {
              await sleep(500)
              return PipelinesService.createOrReplacePipeline(prefix + e.name, {
                ...e.body,
                program_name: e.body.program_name
                  ? prefix + e.body.program_name
                  : e.body.program_name,
                connectors: e.body.connectors
                  ? e.body.connectors.map((connector) => ({
                      ...connector,
                      connector_name: prefix + connector.connector_name
                    }))
                  : e.body.connectors
              }).then(() => sleep(0))
            })
            .exhaustive()
        }
      }
    }
  }
}

type YieldType<T> = T extends AsyncGenerator<infer Yield, void, unknown> ? Yield : never

export type DemoSetupProgress =
  | YieldType<ReturnType<Awaited<ReturnType<typeof runDemoSetup>>['setup']>>
  | 'done'

const cleanupDescription = {
  program: 'Cleaning up programs',
  pipelines: 'Cleaning up pipelines',
  connectors: 'Cleaning up connectors'
}

export const runDemoCleanup = async ({ prefix, steps }: DemoSetup) => {
  const { related } = await relatedEntities({ prefix, steps })
  return {
    related,
    cleanup: async function* () {
      const entityTotal = steps.reduce((acc, step) => acc + step.entities.length, 0)
      let entityNumber = 0
      yield { description: 'Cleaning up', ratio: 0, stage: 'pipeline' as const }
      for (const pipeline of related.pipelines) {
        yield {
          description: cleanupDescription.pipelines,
          ratio: entityNumber / entityTotal,
          stage: 'pipeline' as const
        }
        ++entityNumber
        await sleep(500)
        await PipelinesService.pipelineDelete(pipeline.descriptor.name)
      }
      for (const program of related.programs) {
        yield {
          description: cleanupDescription.program,
          ratio: entityNumber / entityTotal,
          stage: 'program' as const
        }
        ++entityNumber
        await sleep(500)
        await ProgramsService.deleteProgram(program.name)
      }
      for (const connector of related.connectors) {
        yield {
          description: cleanupDescription.connectors,
          ratio: entityNumber / entityTotal,
          stage: 'connector' as const
        }
        ++entityNumber
        await sleep(150)
        await ConnectorsService.deleteConnector(connector.name)
      }
    }
  }
}
