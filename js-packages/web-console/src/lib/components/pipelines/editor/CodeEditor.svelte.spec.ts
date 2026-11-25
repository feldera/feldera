/**
 * Pipeline code can contain connector credentials, so PostHog session replays must
 * mask the code. The recorder shows the text under the `ph-mask` class as asterisks.
 */
import { describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-svelte'
import CodeEditorFixture from './CodeEditorFixture.svelte'

const CODE = "CREATE TABLE t (x INT) WITH ('connectors' = '[]');"

describe('CodeEditor', () => {
  it('masks the code in session replays', async () => {
    const { container } = render(CodeEditorFixture, { code: CODE })
    const codeLines = () => container.querySelector('.monaco-editor .view-lines')
    await expect.poll(() => codeLines()?.textContent).toContain('CREATE')
    expect(codeLines()?.closest('.ph-mask')).toBeInstanceOf(Element)
  })

  it('masks the hidden input of the editor in session replays', async () => {
    const { container } = render(CodeEditorFixture, { code: CODE })
    const hiddenInput = () => container.querySelector('.monaco-editor textarea')
    await expect.poll(hiddenInput).toBeTruthy()
    expect(hiddenInput()?.closest('.ph-mask')).toBeInstanceOf(Element)
  })
})
