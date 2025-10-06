import type { ExtendedPipeline } from '$lib/services/pipelineManager'
import type { PipelineDiff } from '$lib/types/pipelineManager'
import * as va from 'valibot'

const programDiffSchema = va.object({
  added_tables: va.array(va.string()),
  removed_tables: va.array(va.string()),
  modified_tables: va.array(va.string()),
  added_views: va.array(va.string()),
  removed_views: va.array(va.string()),
  modified_views: va.array(va.string())
})

const pipelineDiffSchema = va.object({
  program_diff: va.optional(va.nullable(programDiffSchema)),
  program_diff_error: va.optional(va.nullable(va.string())),
  added_input_connectors: va.array(va.string()),
  modified_input_connectors: va.array(va.string()),
  removed_input_connectors: va.array(va.string()),
  added_output_connectors: va.array(va.string()),
  modified_output_connectors: va.array(va.string()),
  removed_output_connectors: va.array(va.string())
})

// export type ProgramDiff = va.InferOutput<typeof programDiffSchema>
// export type PipelineDiff = va.InferOutput<typeof pipelineDiffSchema>

export const parsePipelineDiff = (
  pipeline: Pick<ExtendedPipeline, 'deploymentRuntimeStatusDetails' | 'status'>
): PipelineDiff => {
  if (pipeline.status !== 'AwaitingApproval') {
    throw new Error('Pipeline is not awaiting approval')
  }

  if (!pipeline.deploymentRuntimeStatusDetails) {
    throw new Error('Pipeline diff data is not available')
  }

  try {
    const parsedDetails = JSON.parse(pipeline.deploymentRuntimeStatusDetails)
    const rawDiff = va.parse(pipelineDiffSchema, parsedDetails)
    return {
      tables: rawDiff.program_diff
        ? {
            added: rawDiff.program_diff.added_tables,
            removed: rawDiff.program_diff.removed_tables,
            modified: rawDiff.program_diff.modified_tables
          }
        : { added: [], removed: [], modified: [] },
      views: rawDiff.program_diff
        ? {
            added: rawDiff.program_diff.added_views,
            removed: rawDiff.program_diff.removed_views,
            modified: rawDiff.program_diff.modified_views
          }
        : { added: [], removed: [], modified: [] },
      inputConnectors: {
        added: rawDiff.added_input_connectors,
        removed: rawDiff.removed_input_connectors,
        modified: rawDiff.modified_input_connectors
      },
      outputConnectors: {
        added: rawDiff.added_output_connectors,
        removed: rawDiff.removed_output_connectors,
        modified: rawDiff.modified_output_connectors
      }
    }
  } catch (error) {
    throw new Error(
      `Failed to parse pipeline diff: ${error instanceof Error ? error.message : 'Unknown error'}`
    )
  }
}
