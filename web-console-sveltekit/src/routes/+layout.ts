import { createClient } from '@hey-api/client-fetch'
import JSONbig from 'true-json-bigint'
import { felderaEndpoint } from '$lib/functions/configs/felderaEndpoint'

export const ssr = false
export const trailingSlash = 'always'

export const load = () => {
  createClient({
    bodySerializer: JSONbig.stringify,
    responseTransformer: JSONbig.parse as any,
    baseUrl: felderaEndpoint
  })
}
