// A region toggle shows the layout message before it changes the graph. The browser does not paint
// while JavaScript runs, so a message set in the same task as the graph change reaches the screen
// only after that change and all the work after it.

import { describe, expect, it } from 'vitest'
import { CIRCUIT_REGION, mountDiagram } from '../test-support/mountDiagram.js'

describe('progress message', () => {
  it('paints the layout message of a region toggle before the graph changes', async () => {
    const { cy, reported, cleanup } = await mountDiagram('light', CIRCUIT_REGION)
    reported.messages.length = 0
    // The browser calls this callback just before it paints the next frame, and before the
    // callbacks that the toggle requests, because this callback was requested first.
    const expandedAtPaint = new Promise<boolean>((resolve) =>
      requestAnimationFrame(() => resolve(cy.$id('region').isParent()))
    )

    cy.$id('region').emit('dblclick')
    expect(reported.messages).toEqual(['Computing layout...'])
    // The frame that paints the message still shows the region expanded.
    expect(await expandedAtPaint).toBe(true)
    await expect.poll(() => cy.$id('region').isParent()).toBe(false)
    await expect.poll(() => reported.messages.at(-1)).toBeNull()
    cleanup()
  })
})
