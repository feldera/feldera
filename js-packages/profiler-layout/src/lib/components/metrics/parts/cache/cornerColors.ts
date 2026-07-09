// Colors scatter points by their position relative to a set of labeled corners. Points are
// normalized to [0, 1] across the data; each point's color is the inverse-distance-weighted
// blend of the corner colors. This is symmetric, so any 2–4 corners work regardless of whether
// they are adjacent or opposite (2 opposite corners reduce to a diagonal gradient; 2 adjacent
// corners give a gradient along that edge).
import type { Rgb } from '../../../../functions/format'

type Point = { x: number; y: number }

/** A named corner of the plot area. Y is up: `top_*` is high Y, `right_*` is high X. */
export type Corner = 'top_left' | 'top_right' | 'bottom_left' | 'bottom_right'

/** Normalized [x, y] position (Y up) of each named corner. */
const CORNER_XY: Record<Corner, [number, number]> = {
  bottom_left: [0, 0],
  bottom_right: [1, 0],
  top_left: [0, 1],
  top_right: [1, 1]
}

/** A named corner of the plot area and the color at that corner. */
export type CornerColor = { corner: Corner; rgb: Rgb }

function rgbStr(rgb: Rgb): string {
  return `rgb(${Math.round(rgb[0])}, ${Math.round(rgb[1])}, ${Math.round(rgb[2])})`
}

export function cornerColors(points: Point[], corners: CornerColor[]): string[] {
  if (points.length === 0 || corners.length === 0) {
    return points.map(() => rgbStr(corners[0]?.rgb ?? [128, 128, 128]))
  }
  let xmin = Infinity
  let xmax = -Infinity
  let ymin = Infinity
  let ymax = -Infinity
  for (const p of points) {
    xmin = Math.min(xmin, p.x)
    xmax = Math.max(xmax, p.x)
    ymin = Math.min(ymin, p.y)
    ymax = Math.max(ymax, p.y)
  }
  const nx = (v: number) => (xmax > xmin ? (v - xmin) / (xmax - xmin) : 0.5)
  const ny = (v: number) => (ymax > ymin ? (v - ymin) / (ymax - ymin) : 0.5)

  return points.map((p) => {
    const px = nx(p.x)
    const py = ny(p.y)
    // Inverse-distance-squared weights; a point sitting on a corner takes that color exactly.
    let exact = -1
    const weights = corners.map((c, i) => {
      const [cx, cy] = CORNER_XY[c.corner]
      const d2 = (px - cx) ** 2 + (py - cy) ** 2
      if (d2 < 1e-12) {
        exact = i
      }
      return 1 / (d2 + 1e-9)
    })
    if (exact >= 0) {
      return rgbStr(corners[exact]!.rgb)
    }
    const sum = weights.reduce((a, b) => a + b, 0)
    const out: Rgb = [0, 0, 0]
    corners.forEach((c, i) => {
      const w = weights[i]! / sum
      out[0] += c.rgb[0] * w
      out[1] += c.rgb[1] * w
      out[2] += c.rgb[2] * w
    })
    return rgbStr(out)
  })
}
