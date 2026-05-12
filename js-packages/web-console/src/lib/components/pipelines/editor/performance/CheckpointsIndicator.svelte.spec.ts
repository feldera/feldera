import { describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'
import type { CheckpointMetadata } from '$lib/services/manager'

vi.mock('$lib/compositions/usePipelineManager.svelte', () => ({
  usePipelineManager: () => ({ checkpointPipeline: vi.fn() })
}))

vi.mock('$lib/compositions/usePremiumFeatures.svelte', () => ({
  usePremiumFeatures: () => ({
    get value() {
      return false
    }
  })
}))

vi.mock('$lib/compositions/layout/useGlobalDialog.svelte', () => ({
  useGlobalDialog: () => ({
    get dialog() {
      return null
    },
    set dialog(_v: unknown) {},
    get onClickAway() {
      return null
    },
    set onClickAway(_v: unknown) {}
  })
}))

import CheckpointsIndicator from './CheckpointsIndicator.svelte'

function makeMetrics(): {
  current: PipelineMetrics
} {
  return {
    current: {
      checkpoint_activity: { status: 'idle' } as PipelineMetrics['checkpoint_activity'],
      permanent_checkpoint_errors: null
    } as PipelineMetrics
  }
}

function makeCheckpoint(): CheckpointMetadata {
  return { uuid: '01900000-0000-7000-8000-000000000000', fingerprint: 0 }
}

describe('CheckpointsIndicator.svelte', () => {
  it('shows elapsed time derived from the checkpoint UUID timestamp', async () => {
    await render(CheckpointsIndicator, {
      pipelineName: 'test',
      checkpoints: [makeCheckpoint()],
      metrics: makeMetrics(),
      checkpointStatus: null,
      onShowCheckpoints: vi.fn()
    })
    await expect.element(page.getByText(/Last checkpoint:.*ago/)).toBeInTheDocument()
  })
})
