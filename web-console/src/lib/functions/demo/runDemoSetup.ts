import type { DemoSetup } from '$lib/types/demo'
import { ConnectorsService, PipelinesService, ProgramsService } from '$lib/services/manager'
import { match } from 'ts-pattern'

export const runDemoSetup = async function* ({ prefix, steps }: DemoSetup) {
  const entityTotal = steps.reduce((acc, step) => acc + step.entities.length, 0)
  let entityNumber = 0
  for (const step of steps) {
    for (const entity of step.entities) {
      if (entityNumber === 0) {
        yield { description: step.name, ratio: 0 }
      }
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
