import { useTryPipeline } from '#lib/compositions/pipelines/useTryPipeline.ts'
import { resolve } from '#lib/functions/svelte.ts'
import type { Demo } from '#lib/services/manager/index.ts'
import { getDemos } from '#lib/services/pipelineManager.ts'
import { goto } from '$app/navigation'

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
  await useTryPipeline()(pipeline, 'url')
}
