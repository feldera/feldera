/**
 * Read-only mode test for JSONDialog. Presence of `onApply` decides the shape:
 * with it, an Apply button; without it, no Apply and a Close button in its place
 * (plus a read-only editor). This lets the pipeline config dialog go read-only
 * for a caller without `write:pipeline_config` while the JSON stays viewable.
 *
 * Also covers closing the dialog while its Monaco editor is still loading.
 */
import loader from '@monaco-editor/loader'
import { describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import JSONDialog from './JSONDialog.svelte'

const baseProps = () => ({
  value: '{\n  "a": 1\n}',
  filePath: 'file://feldera/pipelines/test/runtimeConfig.json',
  title: 'Configure test'
})

describe('JSONDialog.svelte', () => {
  it('renders the Apply button when onApply is provided', async () => {
    render(JSONDialog, { ...baseProps(), onApply: vi.fn(async () => {}) })
    await expect.element(page.getByRole('button', { name: 'Apply' })).toBeInTheDocument()
  })

  it('drops Apply and shows a Close button when onApply is omitted', async () => {
    render(JSONDialog, baseProps())
    // Reverting the onApply-driven onSuccess/onCancel shaping renders Apply and
    // fails this.
    await expect.element(page.getByRole('button', { name: 'Apply' })).not.toBeInTheDocument()
    await expect.element(page.getByTestId('btn-dialog-cancel')).toHaveTextContent('Close')
  })

  it('closing it while Monaco is still loading does not reject', async () => {
    const rejections: string[] = []
    const collectRejection = (event: PromiseRejectionEvent) => rejections.push(String(event.reason))
    window.addEventListener('unhandledrejection', collectRejection)
    try {
      // Unmount inside the window where the editor's loader has not resolved,
      // so the container is gone by the time the editor would be created.
      const dialog = render(JSONDialog, baseProps())
      await dialog.unmount()
      await loader.init()
      // The rejection lands a few turns after the load resolves; stop as soon
      // as one does, and give a clean run the full budget before believing it.
      for (let attempt = 0; attempt < 40 && rejections.length === 0; attempt++) {
        await new Promise((resolve) => setTimeout(resolve, 50))
      }
    } finally {
      window.removeEventListener('unhandledrejection', collectRejection)
    }
    expect(rejections).toEqual([])
  })
})
