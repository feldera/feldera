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

  // A long unbreakable line gives the main content a large min-content width,
  // reproducing the wide connector table that triggered these regressions.
  const wideMain = createRawSnippet(() => ({
    render: () => `<div style="white-space: nowrap">${'WIDE '.repeat(400)}</div>`
  }))

  // Regression test for https://github.com/feldera/feldera/issues/6675: content
  // wider than the pane (e.g. the connector table) must not stretch the pane past
  // its container. The panes use `overflow: visible` so popovers are not clipped,
  // which re-enables the flex default `min-width: auto`; `min-w-0` counteracts it.
  it('keeps the main pane within its container when the content is wider', async () => {
    const { container } = render(InlineDrawer, {
      open: false,
      side: 'right',
      main: wideMain,
      children: textSnippet('DRAWER CONTENT')
    })
    container.style.width = '400px'

    const pane = container.querySelector('[data-pane]') as HTMLElement
    await expect.element(page.getByText('WIDE', { exact: false })).toBeInTheDocument()
    expect(pane.getBoundingClientRect().width).toBeLessThanOrEqual(400)
  })

  // Regression test for the drawer being unresizable: with wide main content and
  // no `min-w-0`, the main pane cannot shrink below its intrinsic width, so the
  // drawer pane collapses to nothing and the resizer has no room to move. The
  // drawer pane must keep the width the resizer allotted it (its `defaultSize`).
  it('gives the drawer pane its allotted width even when the main content is wide', async () => {
    const { container } = render(InlineDrawer, {
      open: true,
      side: 'right',
      main: wideMain,
      children: textSnippet('DRAWER CONTENT'),
      defaultSize: '40%'
    })
    container.style.width = '1000px'

    await expect.element(page.getByText('DRAWER CONTENT')).toBeInTheDocument()
    const panes = [...container.querySelectorAll('[data-pane]')] as HTMLElement[]
    const drawerPane = panes.find((p) => p.textContent?.includes('DRAWER CONTENT'))!
    // 40% of 1000px, minus the resizer's margins; assert it kept most of its share.
    expect(drawerPane.getBoundingClientRect().width).toBeGreaterThan(300)
  })
})
