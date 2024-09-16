import { goto } from '$app/navigation'
import { base } from '$app/paths'
import { useTryPipeline } from '$lib/compositions/pipelines/useTryPipeline'
import type { PipelineDescr } from '$lib/services/manager'
import { getDemos } from '$lib/services/pipelineManager'

export async function load({ url, parent }) {
  await parent()
  const pipeline: PipelineDescr | undefined = await (() => {
    const name = url.searchParams.get('name')
    if (!name) {
      return undefined
    }
    const code = url.searchParams.get('code')
    if (code) {
      return {
        name,
        description: '',
        program_code: code,
        program_config: {},
        runtime_config: {}
      }
    }
    return getDemos().then((demos) => demos.find((demo) => demo.pipeline.name === name)?.pipeline)
  })()
  if (!pipeline) {
    goto(`${base}/`)
    return
  }
  useTryPipeline()(pipeline)
}
