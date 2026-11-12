// Browser tests for what the diagram leaves behind after it is unmounted or reloaded.

import { describe, expect, it } from 'vitest'
import { mountDiagram, OPERATORS, WITH_SOURCE } from '../test-support/mountDiagram.js'

describe('an unmounted diagram', () => {
  it('does not react to the Escape key', async () => {
    const { reported, unmount } = await mountDiagram('light', OPERATORS)
    unmount()
    reported.attributes.length = 0
    document.dispatchEvent(new KeyboardEvent('keyup', { key: 'Escape' }))
    expect(reported.attributes).toEqual([])
  })
})

describe('a reloaded diagram', () => {
  it('gets mouse clicks only in the new diagram', async () => {
    const { reload, click, reported, unmount } = await mountDiagram(
      'light',
      WITH_SOURCE.profile,
      false,
      WITH_SOURCE.dataflow
    )
    const cy = await reload()
    cy.center(cy.$id('n0'))
    const node = cy.$id('n0')
    const { x, y } = node.renderedPosition()
    const zoom = cy.zoom()
    // A listener that the old diagram left on the container fails, because its cytoscape instance
    // is destroyed. The browser reports the failure as an `error` event on the window.
    const errors: string[] = []
    const onError = (event: ErrorEvent) => {
      errors.push(event.message)
      event.preventDefault()
    }
    window.addEventListener('error', onError)
    reported.doubleClicks.length = 0
    // The code chip of `n0`.
    await click(x + node.renderedOuterWidth() / 2 - 15 * zoom, y - node.renderedOuterHeight() / 2 - 8 * zoom)
    window.removeEventListener('error', onError)
    expect(errors).toEqual([])
    expect(reported.doubleClicks).toEqual([{ nodeId: 'n0', type: 'leaf' }])
    unmount()
  })
})
