import type { LoadEvent } from '@sveltejs/kit'
import { goto } from '$app/navigation'
import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte.js'
import { resolve } from '$lib/functions/svelte'

export const prerender = false

export const load = async ({ parent, params }: LoadEvent<{ pipelineName: string }>) => {
  await parent()
  const api = usePipelineManager()
  const pipelineName = decodeURIComponent(params.pipelineName)
  // An AbortController owned by the page so its consumer (+page.svelte) can
  // cancel the in-flight request when the user navigates to another pipeline
  // before this one finishes loading.
  const controller = new AbortController()
  return {
    pipelinePreload: {
      pending: api.getExtendedPipeline(
        pipelineName,
        {
          onNotFound: () => {
            goto(resolve(`/`))
          }
        },
        { signal: controller.signal }
      ),
      abort: () => controller.abort()
    }
  }
}
