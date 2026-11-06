import { afterEach, describe, expect, it } from 'vitest'
import { page } from 'vitest/browser'

// The `.checkbox` class from `src/routes/layout.css`, loaded by `vitest-browser-setup.ts`.
describe('.checkbox', () => {
  let row: HTMLElement | undefined
  afterEach(() => row?.remove())

  const renderCheckboxes = () => {
    const div = document.createElement('div')
    // Zoom so that the 16px check mark is visible in the screenshot.
    div.style.cssText = 'display: flex; gap: 8px; padding: 8px; width: fit-content; zoom: 8'
    div.innerHTML =
      '<input type="checkbox" class="checkbox"><input type="checkbox" class="checkbox" checked>'
    document.body.append(div)
    row = div
    const [unchecked, checked] = div.querySelectorAll('input')
    return { row: div, unchecked, checked }
  }

  it('shows the check mark when checked', async () => {
    const { row, unchecked, checked } = renderCheckboxes()
    // Saved to `src/lib/__screenshots__/checkbox.png` to inspect the rendering.
    await page.elementLocator(row).screenshot({ path: '__screenshots__/checkbox.png' })

    expect(getComputedStyle(unchecked).backgroundImage).toBe('none')

    const url = getComputedStyle(checked).backgroundImage.match(/^url\("(.+)"\)$/)?.[1] ?? ''
    expect(url).not.toBe('')
    const response = await fetch(url)
    expect(response.ok).toBe(true)
    expect(await response.text()).toContain('<svg')
  })
})
