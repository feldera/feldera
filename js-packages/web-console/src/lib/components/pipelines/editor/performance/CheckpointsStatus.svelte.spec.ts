import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import type { CheckpointMetadata } from '$lib/services/manager'

const setDialogMock = vi.fn()

vi.mock('$lib/compositions/layout/useGlobalDialog.svelte', () => ({
  useGlobalDialog: () => ({
    get dialog() {
      return null
    },
    set dialog(v: unknown) {
      setDialogMock(v)
    },
    get onClickAway() {
      return null
    },
    set onClickAway(_v: unknown) {}
  })
}))

import CheckpointsStatus from './CheckpointsStatus.svelte'
import SnippetRenderer from './SnippetRenderer.svelte'

function makeCheckpoint(overrides?: Partial<CheckpointMetadata>): CheckpointMetadata {
  return {
    uuid: '01900000-0000-7000-8000-000000000000',
    fingerprint: 0,
    ...overrides
  }
}

describe('CheckpointsStatus.svelte', () => {
  describe('A. Empty state', () => {
    it('shows "No checkpoints" when list is empty', async () => {
      await render(CheckpointsStatus, { checkpoints: [], onClose: vi.fn() })
      await expect.element(page.getByText('No checkpoints')).toBeInTheDocument()
    })

    it('does not show "No checkpoints" when there are checkpoints', async () => {
      await render(CheckpointsStatus, { checkpoints: [makeCheckpoint()], onClose: vi.fn() })
      await expect.element(page.getByText('No checkpoints')).not.toBeInTheDocument()
    })
  })

  describe('B. Checkpoint list', () => {
    it('renders the checkpoint UUID', async () => {
      const uuid = '01900000-0000-7000-8000-aabbccddeeff'
      await render(CheckpointsStatus, {
        checkpoints: [makeCheckpoint({ uuid })],
        onClose: vi.fn()
      })
      await expect.element(page.getByText(uuid)).toBeInTheDocument()
    })

    it('renders all checkpoints when multiple are provided', async () => {
      const checkpoints = [
        makeCheckpoint({ uuid: '01900000-0000-7000-8000-000000000001' }),
        makeCheckpoint({ uuid: '01900000-0000-7000-8000-000000000002' })
      ]
      await render(CheckpointsStatus, { checkpoints, onClose: vi.fn() })
      await expect
        .element(page.getByText('01900000-0000-7000-8000-000000000001'))
        .toBeInTheDocument()
      await expect
        .element(page.getByText('01900000-0000-7000-8000-000000000002'))
        .toBeInTheDocument()
    })
  })

  describe('C. Create checkpoint button', () => {
    it('is shown when onCheckpoint prop is provided', async () => {
      await render(CheckpointsStatus, {
        checkpoints: [],
        onClose: vi.fn(),
        onCheckpoint: vi.fn()
      })
      await expect.element(page.getByTestId('btn-make-checkpoint')).toBeInTheDocument()
    })

    it('is hidden when onCheckpoint prop is absent', async () => {
      await render(CheckpointsStatus, { checkpoints: [], onClose: vi.fn() })
      await expect.element(page.getByTestId('btn-make-checkpoint')).not.toBeInTheDocument()
    })

    it('opens the confirmation dialog on click', async () => {
      setDialogMock.mockClear()
      await render(CheckpointsStatus, {
        checkpoints: [],
        onClose: vi.fn(),
        onCheckpoint: vi.fn()
      })
      await page.getByTestId('btn-make-checkpoint').click()
      expect(setDialogMock).toHaveBeenCalledOnce()
      expect(setDialogMock.mock.calls[0][0]).not.toBeNull()
    })
  })

  describe('D. Creating-checkpoint button state', () => {
    it('shows "Creating checkpoint..." and is disabled when checkpointInProgress is true', async () => {
      await render(CheckpointsStatus, {
        checkpoints: [],
        onClose: vi.fn(),
        onCheckpoint: vi.fn(),
        checkpointInProgress: true
      })
      const btn = page.getByTestId('btn-make-checkpoint')
      await expect.element(btn).toHaveTextContent('Creating checkpoint...')
      await expect.element(btn).toBeDisabled()
    })

    it('shows "Create checkpoint" and is enabled when checkpointInProgress is false', async () => {
      await render(CheckpointsStatus, {
        checkpoints: [],
        onClose: vi.fn(),
        onCheckpoint: vi.fn(),
        checkpointInProgress: false
      })
      const btn = page.getByTestId('btn-make-checkpoint')
      await expect.element(btn).toHaveTextContent('Create checkpoint')
      await expect.element(btn).not.toBeDisabled()
    })

    describe('with fake timers', () => {
      beforeEach(() => {
        vi.useFakeTimers()
        setDialogMock.mockClear()
      })

      afterEach(() => {
        vi.useRealTimers()
      })

      it('shows "Creating checkpoint..." immediately after confirm, stays disabled while in progress, and returns to normal once done', async () => {
        // Regression test: before the fix, the button re-enabled after the 2 s ClickFeedback
        // timeout even though the checkpoint was still running.
        const { rerender, unmount } = await render(CheckpointsStatus, {
          checkpoints: [],
          onClose: vi.fn(),
          onCheckpoint: vi.fn(),
          checkpointInProgress: false
        })

        await page.getByTestId('btn-make-checkpoint').click()
        const dialogSnippet = setDialogMock.mock.calls[0][0]
        const { unmount: unmountDialog } = await render(SnippetRenderer, { content: dialogSnippet })
        await page.getByTestId('btn-confirm-checkpoint').click()

        // Immediately after confirmation the button must already show the in-progress state
        const btn = page.getByTestId('btn-make-checkpoint')
        await expect.element(btn).toHaveTextContent('Creating checkpoint...')
        await expect.element(btn).toBeDisabled()

        // Parent signals that the checkpoint is now in progress
        await rerender({ checkpointInProgress: true })

        // Advance past the ClickFeedback 2 s internal timeout — the external prop must keep the button locked
        await vi.advanceTimersByTimeAsync(2_500)
        await expect.element(btn).toHaveTextContent('Creating checkpoint...')
        await expect.element(btn).toBeDisabled()

        // Checkpoint completes — button must return to normal
        await rerender({ checkpointInProgress: false })
        await expect.element(btn).toHaveTextContent('Create checkpoint')
        await expect.element(btn).not.toBeDisabled()

        unmountDialog()
        unmount()
      })
    })
  })
})
