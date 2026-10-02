/**
 * Tests for `SplitButton`: the chevron opens the menu without triggering the main action,
 * it carries its accessible name, its square size follows the `size` prop, and the
 * `outlined` variant outlines it.
 */

import { createRawSnippet } from 'svelte'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'
import SplitButton, { type SplitButtonSize, type SplitButtonVariant } from './SplitButton.svelte'

let mounted: { unmount: () => Promise<void> } | undefined

const mainButton = createRawSnippet(() => ({
  render: () => '<button class="btn" data-testid="btn-main">Run</button>'
}))

const mount = (
  props: { size?: SplitButtonSize; variant?: SplitButtonVariant; ontoggle?: () => void } = {}
) => {
  const ontoggle = props.ontoggle ?? vi.fn()
  const result = render(SplitButton, {
    children: mainButton,
    ontoggle,
    toggleLabel: 'More options',
    size: props.size,
    variant: props.variant,
    toggleClass: 'preset-filled-surface-100-900'
  })
  mounted = result
  const root = result.container.querySelector<HTMLElement>('.btn-split')!
  return {
    root,
    main: root.querySelector<HTMLButtonElement>('[data-testid=btn-main]')!,
    toggle: root.querySelector<HTMLButtonElement>('.btn-split-toggle')!,
    chevron: root.querySelector<HTMLElement>('.fd-chevron-down')!
  }
}

afterEach(async () => {
  await mounted?.unmount()
  mounted = undefined
})

describe('SplitButton.svelte', () => {
  it('renders the main action before the chevron', () => {
    const { root, main, toggle } = mount()
    expect(main.textContent).toBe('Run')
    expect(main.compareDocumentPosition(toggle) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    expect(root.querySelectorAll('button')).toHaveLength(2)
  })

  it('opens the menu from the chevron only', () => {
    const ontoggle = vi.fn()
    const { main, toggle } = mount({ ontoggle })

    main.click()
    expect(ontoggle).not.toHaveBeenCalled()

    toggle.click()
    expect(ontoggle).toHaveBeenCalledOnce()
  })

  it('names the chevron and keeps it from submitting a form', () => {
    const { toggle } = mount()
    expect(toggle.getAttribute('aria-label')).toBe('More options')
    expect(toggle.type).toBe('button')
    expect(toggle.classList).toContain('preset-filled-surface-100-900')
  })

  it.each([
    { size: undefined, buttonClass: undefined, iconClass: 'text-[16px]' },
    { size: 'md', buttonClass: undefined, iconClass: 'text-[16px]' },
    { size: 'sm', buttonClass: 'btn-icon-sm', iconClass: 'text-[16px]' },
    { size: 'lg', buttonClass: 'btn-icon-lg', iconClass: 'text-[18px]' }
  ] as const)('sizes the chevron for size $size', ({ size, buttonClass, iconClass }) => {
    const { toggle, chevron } = mount({ size })
    expect(toggle.classList).toContain('btn-icon')
    for (const sizeClass of ['btn-icon-sm', 'btn-icon-lg']) {
      if (sizeClass === buttonClass) {
        expect(toggle.classList).toContain(sizeClass)
      } else {
        expect(toggle.classList).not.toContain(sizeClass)
      }
    }
    expect(chevron.classList).toContain(iconClass)
  })

  it('is filled unless asked to be outlined', () => {
    const { root, toggle } = mount()
    expect(root.classList).not.toContain('btn-split-outlined')
    expect(toggle.classList).not.toContain('preset-outlined-surface-200-800')
  })

  it('outlines the chevron and marks the split as outlined', () => {
    const { root, toggle } = mount({ variant: 'outlined' })
    expect(root.classList).toContain('btn-split-outlined')
    expect(toggle.classList).toContain('preset-outlined-surface-200-800')
    // The caller's classes still apply on top of the variant's.
    expect(toggle.classList).toContain('preset-filled-surface-100-900')
  })
})
