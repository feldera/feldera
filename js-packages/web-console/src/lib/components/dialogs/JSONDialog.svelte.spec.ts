/**
 * Read-only mode test for JSONDialog. Presence of `onApply` decides the shape:
 * with it, an Apply button; without it, no Apply and a Close button in its place
 * (plus a read-only editor). This lets the pipeline config dialog go read-only
 * for a caller without `write:pipeline_config` while the JSON stays viewable.
 */
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
})
