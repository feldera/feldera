import * as va from 'valibot'

import { valibotResolver } from '@hookform/resolvers/valibot'

const schema = va.object({
  prefix: va.string([va.minLength(1, 'Prefix cannot be empty!')])
})

export const demoFormResolver = valibotResolver(schema)
