import { describe, expect, it } from 'vitest'
import type { Rgb } from '../../../../functions/format'
import { cornerColors } from './cornerColors.js'

const RED: Rgb = [255, 0, 0]
const BLUE: Rgb = [0, 0, 255]

describe('cornerColors', () => {
  it('gives each corner its exact color, and a 50/50 blend at the midpoint', () => {
    // Normalized positions (Y up): (0,0)=bottom_left, (1,1)=top_right, (0.5,0.5)=center.
    const points = [
      { x: 0, y: 0 },
      { x: 10, y: 100 },
      { x: 5, y: 50 }
    ]
    const out = cornerColors(points, [
      { corner: 'bottom_left', rgb: RED },
      { corner: 'top_right', rgb: BLUE }
    ])
    expect(out[0]).toBe('rgb(255, 0, 0)')
    expect(out[1]).toBe('rgb(0, 0, 255)')
    expect(out[2]).toBe('rgb(128, 0, 128)')
  })

  it('blends symmetrically for adjacent corners too', () => {
    // Normalized: (0,0), (1,0), (0.5,1). The top-middle point is equidistant from both bottom
    // corners, so it blends 50/50 regardless of the corners being adjacent (not opposite).
    const points = [
      { x: 0, y: 0 },
      { x: 10, y: 0 },
      { x: 5, y: 10 }
    ]
    const out = cornerColors(points, [
      { corner: 'bottom_left', rgb: RED },
      { corner: 'bottom_right', rgb: BLUE }
    ])
    expect(out[2]).toBe('rgb(128, 0, 128)')
  })

  it('supports up to four corners', () => {
    const points = [{ x: 5, y: 5 }] // single point → normalized (0.5, 0.5)
    const out = cornerColors(points, [
      { corner: 'bottom_left', rgb: [255, 0, 0] },
      { corner: 'bottom_right', rgb: [0, 255, 0] },
      { corner: 'top_left', rgb: [0, 0, 255] },
      { corner: 'top_right', rgb: [255, 255, 255] }
    ])
    // Equidistant from all four corners → equal blend → average of the four colors.
    expect(out[0]).toBe('rgb(128, 128, 128)')
  })

  it('returns the single color when one corner is given', () => {
    expect(cornerColors([{ x: 1, y: 2 }], [{ corner: 'top_left', rgb: RED }])).toEqual([
      'rgb(255, 0, 0)'
    ])
  })

  it('returns empty for no points', () => {
    expect(cornerColors([], [{ corner: 'top_left', rgb: RED }])).toEqual([])
  })
})
