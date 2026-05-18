import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import ClipboardCopyButton from './ClipboardCopyButton.svelte'

describe('ClipboardCopyButton', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText: vi.fn().mockResolvedValue(undefined) },
      configurable: true
    })
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('shows copy icon, then confirmation icon after click, then reverts', async () => {
    const { unmount } = render(ClipboardCopyButton, { value: 'test-value' })

    const button = page.getByRole('button', { name: 'Copy to clipboard' })

    await expect.element(button).toHaveClass('fd-copy')
    await expect.element(button).not.toHaveClass('fd-check')

    await button.click()

    expect(navigator.clipboard.writeText).toHaveBeenCalledWith('test-value')
    await expect.element(button).toHaveClass('fd-check')
    await expect.element(button).not.toHaveClass('fd-copy')

    await vi.advanceTimersByTimeAsync(1_000)

    await expect.element(button).toHaveClass('fd-copy')
    await expect.element(button).not.toHaveClass('fd-check')

    unmount()
  })
})
