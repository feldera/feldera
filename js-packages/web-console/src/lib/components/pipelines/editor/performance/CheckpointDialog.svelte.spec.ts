import { describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import CheckpointDialog from './CheckpointDialog.svelte'

const clickButton = (locator: ReturnType<typeof page.getByTestId>) => {
  ;(locator.element() as HTMLButtonElement).click()
}

describe('CheckpointDialog.svelte', () => {
  describe('A. Content', () => {
    it('renders the title', async () => {
      await render(CheckpointDialog, { onConfirm: vi.fn() })
      await expect
        .element(page.getByTestId('box-dialog-title'))
        .toHaveTextContent('Create a checkpoint?')
    })

    it('renders the warning description', async () => {
      await render(CheckpointDialog, { onConfirm: vi.fn() })
      await expect
        .element(page.getByTestId('box-dialog-description'))
        .toHaveTextContent('may delete the oldest checkpoint')
    })

    it('labels the action button "Checkpoint"', async () => {
      await render(CheckpointDialog, { onConfirm: vi.fn() })
      await expect
        .element(page.getByTestId('btn-confirm-checkpoint'))
        .toHaveTextContent('Checkpoint')
    })
  })

  describe('B. Actions', () => {
    it('calls onConfirm when Checkpoint is clicked', async () => {
      const onConfirm = vi.fn()
      await render(CheckpointDialog, { onConfirm })
      clickButton(page.getByTestId('btn-confirm-checkpoint'))
      expect(onConfirm).toHaveBeenCalledOnce()
    })

    it('does not call onConfirm when Cancel is clicked', async () => {
      const onConfirm = vi.fn()
      await render(CheckpointDialog, { onConfirm })
      clickButton(page.getByTestId('btn-dialog-cancel'))
      expect(onConfirm).not.toHaveBeenCalled()
    })
  })
})
