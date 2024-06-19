// export type DemoSetup = {
//   prefix: string
//   steps: {
//     name: string
//     entities: {
//       type: 'program' | 'connector' | 'pipeline'
//       name: string
//       body: any
//     }[]
//   }[]
// }

import type {
  CreateOrReplaceConnectorRequest,
  CreateOrReplacePipelineRequest,
  CreateOrReplaceProgramRequest
} from '$lib/services/manager'

export type DemoSetup = {
  title: string
  description: string
  prefix: string
  steps: {
    name: string
    entities: (
      | {
          type: 'program'
          name: string
          body: CreateOrReplaceProgramRequest
        }
      | {
          type: 'connector'
          name: string
          body: CreateOrReplaceConnectorRequest
        }
      | {
          type: 'pipeline'
          name: string
          body: CreateOrReplacePipelineRequest
        }
    )[]
  }[]
}
