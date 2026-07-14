/**
 * Component tests for the overlay drawer used on tablet/mobile. The drawer has
 * a nominal width (e.g. `w-[700px]`) but must never grow wider than the screen,
 * otherwise its contents scroll the page horizontally. These tests run at the
 * browser's default narrow viewport, where the nominal width exceeds the screen.
 */

import { createRawSnippet } from 'svelte'
import { describe, expect, it } from 'vitest'
import { render } from 'vitest-browser-svelte'
import OverlayDrawer from './OverlayDrawer.svelte'

const textSnippet = (text: string) =>
  createRawSnippet(() => ({ render: () => `<div>${text}</div>` }))

const renderOverlay = () =>
  render(OverlayDrawer, {
    open: true,
    side: 'right',
    modal: true,
    width: 'w-[700px]',
    class: 'p-4',
    children: textSnippet('DRAWER CONTENT')
  })

describe('OverlayDrawer', () => {
  it('caps the drawer at the viewport width on narrow screens', async () => {
    // Precondition: the nominal width is wider than the screen, so an
    // uncapped drawer would overflow. Guard against a wide test viewport.
    expect(window.innerWidth).toBeLessThan(700)

    const { container } = renderOverlay()
    const dialog = container.querySelector('[role="dialog"]') as HTMLElement
    expect(dialog).not.toBeNull()

    // The dialog shrinks to fit instead of forcing 700px.
    // (Drop `max-w-full` from the dialog class to see this fail.)
    expect(dialog.getBoundingClientRect().width).toBeLessThanOrEqual(window.innerWidth)
  })
})
