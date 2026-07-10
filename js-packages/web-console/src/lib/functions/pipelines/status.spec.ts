/**
 * Unit tests for `deletePipelineDisabledReason`, the helper that names why a
 * pipeline cannot be deleted. It mirrors the pipeline-manager's delete
 * preconditions (fully stopped, storage cleared), so the tests walk the same
 * two blockers and their resolution order.
 */
import { describe, expect, it } from 'vitest'
import type { StorageStatus } from '$lib/services/manager'
import type { PipelineStatus } from '$lib/services/pipelineManager'
import { deletePipelineDisabledReason } from './status'

describe('deletePipelineDisabledReason', () => {
  it('allows deletion once the pipeline is stopped and storage is cleared', () => {
    expect(deletePipelineDisabledReason('Stopped', 'Cleared', false)).toBeUndefined()
  })

  it('reports when the pipeline is already deleted', () => {
    expect(deletePipelineDisabledReason('Stopped', 'Cleared', true)).toBe(
      'This pipeline has already been deleted.'
    )
  })

  it('asks to stop the pipeline while it is still running', () => {
    expect(deletePipelineDisabledReason('Running', 'Cleared', false)).toBe(
      'Stop the pipeline to delete it.'
    )
  })

  // Stop takes priority: a running pipeline always holds its storage, so naming
  // storage first would send the user down a step they cannot take yet.
  it('asks to stop first even when storage is still in use', () => {
    expect(deletePipelineDisabledReason('Running', 'InUse', false)).toBe(
      'Stop the pipeline to delete it.'
    )
  })

  it('asks to clear storage when stopped but storage is in use', () => {
    expect(deletePipelineDisabledReason('Stopped', 'InUse', false)).toBe(
      'Clear the pipeline storage to delete it.'
    )
  })

  it('asks to wait while storage is mid-clear', () => {
    expect(deletePipelineDisabledReason('Stopped', 'Clearing', false)).toBe(
      'Wait for storage to finish clearing to delete the pipeline.'
    )
  })

  // A pipeline in an error state is fully stopped, so cleared storage unblocks
  // deletion just as it does for the normal Stopped state.
  it('treats error states as stopped', () => {
    const errorStates: PipelineStatus[] = ['SqlError', 'RustError', 'SystemError']
    for (const status of errorStates) {
      expect(deletePipelineDisabledReason(status, 'Cleared', false)).toBeUndefined()
      expect(deletePipelineDisabledReason(status, 'InUse', false)).toBe(
        'Clear the pipeline storage to delete it.'
      )
    }
  })

  // Transitional states are not fully stopped, so deletion stays blocked on stop
  // regardless of the storage state.
  it('blocks on stop for transitional states', () => {
    const transitional: PipelineStatus[] = ['Pausing', 'Resuming', 'Stopping', 'Suspending']
    const anyStorage: StorageStatus[] = ['Cleared', 'InUse', 'Clearing']
    for (const status of transitional) {
      for (const storage of anyStorage) {
        expect(deletePipelineDisabledReason(status, storage, false)).toBe(
          'Stop the pipeline to delete it.'
        )
      }
    }
  })
})
