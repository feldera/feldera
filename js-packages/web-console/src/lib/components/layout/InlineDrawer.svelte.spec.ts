/**
 * Component tests for the resizable inline drawer. The drawer lays its
 * always-visible `main` content next to a resizable `children` pane using
 * PaneForge; a draggable separator (`[data-pane-resizer]`) appears only while
 * the drawer is open. These tests run irrespective of viewport, since
 * `InlineDrawer` is the wide-screen variant selected by `Drawer`.
 */

import { createRawSnippet } from 'svelte'
import { describe, expect, it } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import InlineDrawer from './InlineDrawer.svelte'

const textSnippet = (text: string) =>
  createRawSnippet(() => ({ render: () => `<div>${text}</div>` }))

const renderDrawer = (props: { open: boolean; side: 'right' | 'left' }) =>
  render(InlineDrawer, {
    ...props,
    main: textSnippet('MAIN CONTENT'),
    children: textSnippet('DRAWER CONTENT')
  })

describe('InlineDrawer', () => {
  it('shows only the main content and no resize handle when closed', async () => {
    const { container } = renderDrawer({ open: false, side: 'right' })

    await expect.element(page.getByText('MAIN CONTENT')).toBeInTheDocument()
    // Closed: the main pane fills the container, so there is nothing to drag.
    expect(container.querySelector('[data-pane-resizer]')).toBeNull()
    expect(container.textContent).not.toContain('DRAWER CONTENT')
  })

  it('renders the drawer content and a draggable resize handle when open', async () => {
    const { container } = renderDrawer({ open: true, side: 'right' })

    await expect.element(page.getByText('DRAWER CONTENT')).toBeInTheDocument()
    await expect.element(page.getByText('MAIN CONTENT')).toBeInTheDocument()
    // The separator is what makes the drawer resizable — the crux of the feature.
    // Remove the `{#if open}` guard around the resizer to see this assertion fail.
    expect(container.querySelector('[data-pane-resizer]')).not.toBeNull()
  })

  it('places the drawer before the main content when anchored to the left', async () => {
    const { container } = renderDrawer({ open: true, side: 'left' })

    await expect.element(page.getByText('DRAWER CONTENT')).toBeInTheDocument()
    // Declared `order` keeps the drawer pane first regardless of render order.
    const panes = [...container.querySelectorAll('[data-pane]')]
    expect(panes[0]?.textContent).toContain('DRAWER CONTENT')
    expect(panes[1]?.textContent).toContain('MAIN CONTENT')
  })
})
