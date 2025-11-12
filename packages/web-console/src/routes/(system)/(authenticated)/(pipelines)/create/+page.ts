import { goto } from '$app/navigation'
import { resolve } from '$lib/functions/svelte'
import { useTryPipeline } from '$lib/compositions/pipelines/useTryPipeline'
import type { Demo } from '$lib/services/manager'
import { getDemos } from '$lib/services/pipelineManager'

export async function load({ url, parent }) {
  await parent()
  const pipeline: Omit<Demo, 'title'> | undefined = await (() => {
    const name = url.searchParams.get('name')
    if (!name) {
      return undefined
    }
    const program_code = url.searchParams.get('program.sql') ?? url.searchParams.get('code')
    const udf_rust = url.searchParams.get('udf.rs') ?? ''
    const udf_toml = url.searchParams.get('udf.toml') ?? ''
    if (program_code) {
      return {
        name,
        description: '',
        program_code,
        udf_rust,
        udf_toml
      }
    }
    return getDemos().then((demos) => demos.find((demo) => demo.name === name))
  })()
  if (!pipeline) {
    goto(resolve(`/`))
    return
  }
  await useTryPipeline()(pipeline)
}
