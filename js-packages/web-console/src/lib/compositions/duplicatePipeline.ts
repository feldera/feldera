import type { PipelineManagerApi } from '$lib/compositions/usePipelineManager.svelte'
import type { PipelineThumb } from '$lib/services/pipelineManager'
import type { useUpdatePipelineList } from './pipelines/usePipelineList.svelte'

export const duplicatePipelineTooltip =
  'Pipeline storage and state are not duplicated. The new pipeline starts fresh.'

export const MAX_DUPLICATE_ATTEMPTS = 10_000

export const resolveDuplicatePipelineName = (
  pipelineName: string,
  pipelines: Pick<PipelineThumb, 'name'>[]
) => {
  const pipelineNames = new Set(pipelines.map((pipeline) => pipeline.name))
  const baseName = pipelineName.replace(/-copy(?:\d+)?$/, '')

  for (let index = 1; index <= MAX_DUPLICATE_ATTEMPTS; index += 1) {
    const candidate = `${baseName}-copy${index === 1 ? '' : index}`
    if (!pipelineNames.has(candidate)) {
      return candidate
    }
  }

  throw new Error('Could not allocate a unique duplicate pipeline name.')
}

export const optimisticDuplicatePipelineThumb = (
  pipeline: PipelineThumb,
  pipelineName: string
): PipelineThumb => {
  const now = new Date()

  return {
    ...pipeline,
    name: pipelineName,
    status: { Queued: { cause: 'compile' } },
    storageStatus: 'Cleared',
    deploymentError: null,
    deploymentStatusSince: now.toISOString(),
    programStatusSince: now.toISOString(),
    deploymentResourcesStatus: 'Stopped',
    deploymentResourcesStatusSince: now,
    connectors: undefined
  }
}

export const duplicatePipeline = async (
  api: PipelineManagerApi,
  pipeline: PipelineThumb,
  pipelines: PipelineThumb[],
  updates: ReturnType<typeof useUpdatePipelineList>
) => {
  const duplicateName = resolveDuplicatePipelineName(pipeline.name, pipelines)
  updates.discardPendingListRefresh()
  updates.updatePipelines((pipelines) =>
    pipelines.some((pipeline) => pipeline.name === duplicateName)
      ? pipelines
      : [...pipelines, optimisticDuplicatePipelineThumb(pipeline, duplicateName)]
  )

  try {
    const extendedPipeline = await api.getExtendedPipeline(pipeline.name)
    const newPipeline = await api.postPipeline({
      name: duplicateName,
      description: extendedPipeline.description,
      tags: extendedPipeline.tags,
      runtime_config: extendedPipeline.runtimeConfig,
      program_config: extendedPipeline.programConfig,
      program_code: extendedPipeline.programCode,
      udf_rust: extendedPipeline.programUdfRs,
      udf_toml: extendedPipeline.programUdfToml
    })

    updates.updatePipeline(duplicateName, () => newPipeline)
    return newPipeline
  } catch (error) {
    updates.updatePipelines((pipelines) =>
      pipelines.filter((pipeline) => pipeline.name !== duplicateName)
    )
    throw error
  }
}
