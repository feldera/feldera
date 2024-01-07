import { usePipelineManagerQueryAuth } from '$lib/compositions/auth/usePipelineManagerQueryAuth'
import { makePipelineManagerQuery } from '$lib/services/pipelineManagerQuery'

export const usePipelineManagerQuery = () => makePipelineManagerQuery(usePipelineManagerQueryAuth())
