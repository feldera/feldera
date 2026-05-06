import { describe, expect, it, vi } from 'vitest'
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
})
