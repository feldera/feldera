/**
 * Unit tests for `parsePipelineDiff`, which reads the approval diff from
 * `deployment_runtime_status_details.approval_diff`. Older pipelines stored the
 * diff as the whole runtime status details value; the manager rewrites those
 * into the `approval_diff` shape, so the parser only handles the new location.
 */
import { describe, expect, it } from 'vitest'
import type { ExtendedPipeline } from '$lib/services/pipelineManager'
import { parsePipelineDiff } from './pipelineDiff'

const approvalDiff = {
  program_diff: {
    added_tables: ['t_added'],
    removed_tables: [],
    modified_tables: ['t_modified'],
    added_views: [],
    removed_views: ['v_removed'],
    modified_views: []
  },
  program_diff_error: null,
  added_input_connectors: ['in_added'],
  modified_input_connectors: [],
  removed_input_connectors: [],
  added_output_connectors: [],
  modified_output_connectors: ['out_modified'],
  removed_output_connectors: []
}

const pipeline = (
  details: unknown
): Pick<ExtendedPipeline, 'deploymentRuntimeStatusDetails' | 'status'> =>
  ({
    status: 'AwaitingApproval',
    deploymentRuntimeStatusDetails: details
  }) as Pick<ExtendedPipeline, 'deploymentRuntimeStatusDetails' | 'status'>

describe('parsePipelineDiff', () => {
  it('parses the diff nested under approval_diff', () => {
    const diff = parsePipelineDiff(pipeline({ approval_diff: approvalDiff }))
    expect(diff.tables).toEqual({ added: ['t_added'], removed: [], modified: ['t_modified'] })
    expect(diff.views).toEqual({ added: [], removed: ['v_removed'], modified: [] })
    expect(diff.inputConnectors.added).toEqual(['in_added'])
    expect(diff.outputConnectors.modified).toEqual(['out_modified'])
    expect(diff.error).toBeUndefined()
  })

  it('defaults program table/view diffs when program_diff is null', () => {
    const diff = parsePipelineDiff(
      pipeline({ approval_diff: { ...approvalDiff, program_diff: null } })
    )
    expect(diff.tables).toEqual({ added: [], removed: [], modified: [] })
    expect(diff.views).toEqual({ added: [], removed: [], modified: [] })
  })

  it('surfaces program_diff_error', () => {
    const diff = parsePipelineDiff(
      pipeline({ approval_diff: { ...approvalDiff, program_diff_error: 'boom' } })
    )
    expect(diff.error).toBe('boom')
  })

  it('throws when the expected approval info is not there', () => {
    expect(() =>
      parsePipelineDiff({
        status: 'AwaitingApproval',
        deploymentRuntimeStatusDetails: {}
      } as Pick<ExtendedPipeline, 'deploymentRuntimeStatusDetails' | 'status'>)
    ).toThrow('data is not available')
  })

  it('throws when approval_diff is absent', () => {
    // A running-connector runtime status may set only `connector_stats`.
    expect(() => parsePipelineDiff(pipeline({ connector_stats: { num_errors: 0 } }))).toThrow(
      'not available'
    )
  })

  it('throws when there are no runtime status details at all', () => {
    expect(() => parsePipelineDiff(pipeline(undefined))).toThrow('not available')
  })
})
