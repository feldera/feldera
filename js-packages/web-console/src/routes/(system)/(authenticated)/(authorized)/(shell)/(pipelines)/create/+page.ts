import {
  readPastedLinkFiles,
  useOpenPastedLink
} from '#lib/compositions/pipelines/usePastedLink.svelte.ts'
import { useTryPipeline } from '#lib/compositions/pipelines/useTryPipeline.ts'
import { resolve } from '#lib/functions/svelte.ts'
import { getDemos } from '#lib/services/pipelineManager.ts'
import { goto } from '$app/navigation'

export async function load({ url, parent }) {
  await parent()
  const name = url.searchParams.get('name')
  if (!name) {
    goto(resolve(`/`))
    return
  }
  const linkFiles = readPastedLinkFiles(url.searchParams)
  if (linkFiles) {
    await useOpenPastedLink()(name, linkFiles)
    return
  }
  const demo = (await getDemos()).find((demo) => demo.name === name)
  if (!demo) {
    goto(resolve(`/`))
    return
  }
  await useTryPipeline()(demo, 'url')
}
