import { resolveDuplicatePipelineName } from '#lib/compositions/duplicatePipeline.ts'
import { useUpdatePipelineList } from '#lib/compositions/pipelines/usePipelineList.svelte.ts'
import { resolve } from '#lib/functions/svelte.ts'
import { captureEvent } from '#lib/services/analytics.ts'
import { goto } from '$app/navigation'
import { usePipelineManager } from '../usePipelineManager.svelte'

/**
 * Code that a `/create/` link carries in its query string. The link's author, not the
 * user, chose this code, so the console never saves it without the user's confirmation.
 */
export type PastedLinkFiles = Record<'program.sql' | 'udf.rs' | 'udf.toml', string>

/**
 * Read the code from a `/create/` link's query string.
 * @returns `undefined` when the link carries no code, as a demo link does.
 */
export const readPastedLinkFiles = (params: URLSearchParams): PastedLinkFiles | undefined => {
  const files = {
    // `code` is the older name of `program.sql`. The Run button on code blocks in
    // docs.feldera.com uses it.
    'program.sql': params.get('program.sql') ?? params.get('code') ?? '',
    'udf.rs': params.get('udf.rs') ?? '',
    'udf.toml': params.get('udf.toml') ?? ''
  }
  return Object.values(files).some((code) => code !== '') ? files : undefined
}

export const pastedLinkFileNames = (files: PastedLinkFiles) =>
  Object.entries(files)
    .filter(([, code]) => code !== '')
    .map(([fileName]) => fileName)

/**
 * Keep the link's name when it is free, and pick a `-copy` name otherwise, so that
 * link code never goes into a pipeline that already exists.
 */
export const resolvePastedLinkPipelineName = (name: string, pipelines: { name: string }[]) =>
  pipelines.some((pipeline) => pipeline.name === name)
    ? resolveDuplicatePipelineName(name, pipelines)
    : name

// Keyed by pipeline id, so a pipeline rename keeps the link's code. The record lives in memory only,
// so a page reload drops the link's code.
let pastedLinks = $state<Record<string, PastedLinkFiles>>({})

/**
 * Pipelines that hold unsaved code from a link. While a pipeline has an entry, its
 * editor shows the entry's code as unsaved edits, turns off autosave, and asks the
 * user to confirm every save until the user confirms one.
 */
export const usePastedLinks = () => ({
  get(pipelineId: string): PastedLinkFiles | undefined {
    return pastedLinks[pipelineId]
  },
  clear(pipelineId: string) {
    delete pastedLinks[pipelineId]
  }
})

/**
 * Open the code from a `/create/` link: create an empty pipeline under a free name
 * and open it with the link's code as unsaved edits.
 */
export const useOpenPastedLink = () => {
  const api = usePipelineManager()
  const { updatePipelines } = useUpdatePipelineList()
  return async (requestedName: string, files: PastedLinkFiles) => {
    captureEvent('demo_opened', { demo: requestedName, already_created: false, placement: 'url' })
    let pipelineName: string
    let pipelineId: string
    try {
      pipelineName = resolvePastedLinkPipelineName(requestedName, await api.getPipelines())
      const newPipeline = await api.postPipeline({
        name: pipelineName,
        description: '',
        program_code: '',
        program_config: {},
        runtime_config: {},
        udf_rust: '',
        udf_toml: ''
      })
      pipelineId = newPipeline.id
      updatePipelines((pipelines) => (pipelines.push(newPipeline), pipelines))
    } catch {
      // The API layer reports the error.
      goto(resolve(`/`), { replaceState: true })
      return
    }
    pastedLinks[pipelineId] = files
    // Replace the `/create/` history entry, so that going back and forward does not
    // create another pipeline.
    goto(resolve(`/pipelines/${encodeURIComponent(pipelineName)}/`), { replaceState: true })
  }
}
