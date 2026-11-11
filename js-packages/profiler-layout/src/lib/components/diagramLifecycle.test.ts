// Browser tests for what the diagram leaves behind after it is unmounted.

import { describe, expect, it } from 'vitest'
import { mountDiagram, OPERATORS } from '../test-support/mountDiagram.js'

describe('an unmounted diagram', () => {
  it('does not react to the Escape key', async () => {
    const { reported, unmount } = await mountDiagram('light', OPERATORS)
    unmount()
    reported.attributes.length = 0
    document.dispatchEvent(new KeyboardEvent('keyup', { key: 'Escape' }))
    expect(reported.attributes).toEqual([])
  })
})
