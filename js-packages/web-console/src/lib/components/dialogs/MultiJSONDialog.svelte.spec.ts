/**
 * Read-only mode test for MultiJSONDialog. Presence of `onApply` decides the
 * shape: with it, an Apply button; without it, no Apply and a Close button in
 * its place (plus read-only editors). This lets the pipeline configurations
 * popup go read-only for a caller without `write:pipeline_config` while the JSON
 * stays viewable.
 */
import { describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import MultiJSONDialog from './MultiJSONDialog.svelte'

const baseProps = () => ({
  values: { runtimeConfig: '{\n  "a": 1\n}', programConfig: '{}' },
  metadata: {
    runtimeConfig: { title: 'Runtime configuration', filePath: 'file://p/RuntimeConfig.json' },
    programConfig: { title: 'Compilation configuration', filePath: 'file://p/ProgramConfig.json' }
  },
  title: 'Configure test'
})

describe('MultiJSONDialog.svelte', () => {
  it('renders the Apply button when onApply is provided', async () => {
    render(MultiJSONDialog, { ...baseProps(), onApply: vi.fn(async () => {}) })
    await expect.element(page.getByRole('button', { name: 'Apply' })).toBeInTheDocument()
  })

  it('drops Apply and shows a Close button when onApply is omitted', async () => {
    render(MultiJSONDialog, baseProps())
    // Reverting the onApply-driven onSuccess/onCancel shaping renders Apply and
    // fails this.
    await expect.element(page.getByRole('button', { name: 'Apply' })).not.toBeInTheDocument()
    await expect.element(page.getByTestId('btn-dialog-cancel')).toHaveTextContent('Close')
  })
})
