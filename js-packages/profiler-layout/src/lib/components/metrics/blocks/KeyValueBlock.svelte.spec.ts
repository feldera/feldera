import { CountValue } from 'profiler-lib'
import { describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-svelte'
import KeyValueBlock from './KeyValueBlock.svelte'

describe('KeyValueBlock', () => {
  it('collapses to its title on a click and expands on the next', async () => {
    const { container } = render(KeyValueBlock, {
      props: {
        id: 'global-metrics-collapse',
        title: 'Global stats',
        entries: [{ key: 'records', label: 'Records', value: new CountValue(3) }]
      }
    })
    const button = container.querySelector<HTMLButtonElement>('h3 button')!
    expect(container.querySelectorAll('dd').length).toBe(1)

    button.click()
    await expect.poll(() => container.querySelectorAll('dd').length).toBe(0)
    expect(container.textContent).toContain('Global stats')
    expect(button.getAttribute('aria-expanded')).toBe('false')

    button.click()
    await expect.poll(() => container.querySelectorAll('dd').length).toBe(1)
  })
})
