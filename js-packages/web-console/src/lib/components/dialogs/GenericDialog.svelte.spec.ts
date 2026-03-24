import { describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import type { GlobalDialogContent } from '$lib/compositions/layout/useGlobalDialog.svelte'
import GenericDialog from './GenericDialog.svelte'

function makeContent(overrides?: Partial<GlobalDialogContent>): GlobalDialogContent {
  return {
    title: 'Test Dialog',
    ...overrides
  }
}

async function renderDialog(
  content: GlobalDialogContent,
  opts?: { danger?: boolean; disabled?: boolean; noclose?: boolean }
) {
  return render(GenericDialog, { content, ...opts })
}

describe('GenericDialog.svelte', () => {
  describe('A. Basic rendering', () => {
    it('renders title', async () => {
      await renderDialog(makeContent({ title: 'My Title' }))
      await expect.element(page.getByTestId('box-dialog-title')).toHaveTextContent('My Title')
    })

    it('renders description when provided', async () => {
      await renderDialog(makeContent({ description: 'Some description text' }))
      await expect
        .element(page.getByTestId('box-dialog-description'))
        .toHaveTextContent('Some description text')
    })

    it('does not render description when omitted', async () => {
      await renderDialog(makeContent())
      await expect.element(page.getByTestId('box-dialog-description')).not.toBeInTheDocument()
    })

    it('renders scrollable content when provided', async () => {
      await renderDialog(makeContent({ scrollableContent: 'Scrollable text here' }))
      await expect
        .element(page.getByTestId('box-dialog-scrollable-content'))
        .toHaveTextContent('Scrollable text here')
    })

    it('does not render scrollable content when omitted', async () => {
      await renderDialog(makeContent())
      await expect
        .element(page.getByTestId('box-dialog-scrollable-content'))
        .not.toBeInTheDocument()
    })
  })

  describe('B. Action buttons', () => {
    it('renders success and cancel buttons when onSuccess is provided', async () => {
      await renderDialog(
        makeContent({
          onSuccess: {
            name: 'Confirm',
            callback: vi.fn(),
            'data-testid': 'btn-dialog-success'
          }
        })
      )
      await expect.element(page.getByTestId('btn-dialog-success')).toHaveTextContent('Confirm')
      await expect.element(page.getByTestId('btn-dialog-cancel')).toHaveTextContent('Cancel')
    })

    it('does not render action buttons when onSuccess is omitted', async () => {
      await renderDialog(makeContent())
      await expect.element(page.getByTestId('box-dialog-actions')).not.toBeInTheDocument()
    })

    it('calls onSuccess callback when confirm button is clicked', async () => {
      const onSuccess = vi.fn()
      await renderDialog(
        makeContent({
          onSuccess: { name: 'Confirm', callback: onSuccess, 'data-testid': 'btn-dialog-success' }
        })
      )
      await page.getByTestId('btn-dialog-success').click()
      expect(onSuccess).toHaveBeenCalledOnce()
    })

    it('uses custom cancel button label from onCancel.name', async () => {
      await renderDialog(
        makeContent({
          onSuccess: { name: 'OK', callback: vi.fn() },
          onCancel: { name: 'Go Back' }
        })
      )
      await expect.element(page.getByTestId('btn-dialog-cancel')).toHaveTextContent('Go Back')
    })

    it('renders data-testid on success button when provided', async () => {
      await renderDialog(
        makeContent({
          onSuccess: {
            name: 'Apply',
            callback: vi.fn(),
            'data-testid': 'button-confirm-apply'
          }
        })
      )
      await expect.element(page.getByTestId('button-confirm-apply')).toBeInTheDocument()
    })
  })

  describe('C. Close button and noclose', () => {
    it('renders close X button by default', async () => {
      await renderDialog(makeContent())
      await expect.element(page.getByLabelText('Close dialog')).toBeInTheDocument()
    })

    it('hides close X button when noclose is set', async () => {
      await renderDialog(makeContent(), { noclose: true })
      await expect.element(page.getByLabelText('Close dialog')).not.toBeInTheDocument()
    })
  })

  describe('D. Disabled state', () => {
    it('disables success button when disabled prop is set', async () => {
      await renderDialog(
        makeContent({
          onSuccess: { name: 'Apply', callback: vi.fn(), 'data-testid': 'btn-dialog-success' }
        }),
        { disabled: true }
      )
      await expect.element(page.getByTestId('btn-dialog-success')).toBeDisabled()
    })

    it('success button is enabled by default', async () => {
      await renderDialog(
        makeContent({
          onSuccess: { name: 'Apply', callback: vi.fn(), 'data-testid': 'btn-dialog-success' }
        })
      )
      await expect.element(page.getByTestId('btn-dialog-success')).not.toBeDisabled()
    })
  })

  describe('E. Cancel callback', () => {
    it('calls onCancel callback when cancel button is clicked', async () => {
      const onCancel = vi.fn()
      await renderDialog(
        makeContent({
          onSuccess: { name: 'OK', callback: vi.fn() },
          onCancel: { name: 'Back', callback: onCancel }
        })
      )
      await page.getByTestId('btn-dialog-cancel').click()
      expect(onCancel).toHaveBeenCalledOnce()
    })

    it('calls onCancel callback when close X is clicked', async () => {
      const onCancel = vi.fn()
      await renderDialog(
        makeContent({
          onCancel: { callback: onCancel }
        })
      )
      await page.getByLabelText('Close dialog').click()
      expect(onCancel).toHaveBeenCalledOnce()
    })
  })
})
