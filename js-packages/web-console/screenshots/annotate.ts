import type { Locator, Page } from '@playwright/test'

/** A rectangle in CSS pixels, relative to the viewport. */
export type Box = { x: number; y: number; width: number; height: number }

export type Side = 'top' | 'bottom' | 'left' | 'right'

/**
 * A mark drawn over a screenshot:
 * - `frame` outlines `box` and puts `label` (if any) in a tag outside the given side.
 * - `arrow` points from `from` to `to`, and puts `label` (if any) in a tag at `from`.
 * - `callout` outlines `box`, and points to it from a tag in a margin beside it. The tag is level
 *   with `box`, and its edge that faces `box` is at `x`.
 * - `cover` paints `box` white. A clip that reaches over a neighboring part of the UI uses it to
 *   make a margin for callouts.
 */
export type Annotation =
  | { kind: 'frame'; box: Box; label?: string; side?: Side }
  | { kind: 'arrow'; from: { x: number; y: number }; to: { x: number; y: number }; label?: string }
  | { kind: 'callout'; box: Box; label: string; x: number }
  | { kind: 'cover'; box: Box }

/** The bounding box of `locator`, or an error that names it. */
export async function boxOf(locator: Locator): Promise<Box> {
  const box = await locator.boundingBox()
  if (!box) {
    throw new Error(`${locator} is not visible`)
  }
  return box
}

/** `box`, grown by `by` px on each side. */
export const grow = (box: Box, by: number): Box => ({
  x: box.x - by,
  y: box.y - by,
  width: box.width + 2 * by,
  height: box.height + 2 * by
})

/** The smallest box that holds all of `boxes`. */
export const union = (...boxes: Box[]): Box => {
  const left = Math.min(...boxes.map((b) => b.x))
  const top = Math.min(...boxes.map((b) => b.y))
  const right = Math.max(...boxes.map((b) => b.x + b.width))
  const bottom = Math.max(...boxes.map((b) => b.y + b.height))
  return { x: left, y: top, width: right - left, height: bottom - top }
}

/**
 * Draws `annotations` in an SVG layer on top of the page. Because the marks are placed from the
 * boxes of live elements, they follow the UI when its layout changes. Call `clearAnnotations` to
 * remove them.
 */
export async function annotate(page: Page, annotations: Annotation[]) {
  await page.evaluate((annotations) => {
    const COLOR = '#059669'
    const NS = 'http://www.w3.org/2000/svg'
    const svg = document.createElementNS(NS, 'svg')
    svg.id = 'docs-annotations'
    svg.setAttribute(
      'style',
      'position:fixed;inset:0;width:100vw;height:100vh;pointer-events:none;z-index:2147483647'
    )
    svg.innerHTML = `<defs><marker id="docs-arrowhead" viewBox="0 0 10 10" refX="9" refY="5"
      markerWidth="5" markerHeight="5" orient="auto-start-reverse">
      <path d="M0,0 L10,5 L0,10 z" fill="${COLOR}"/></marker></defs>`
    document.body.appendChild(svg)

    const add = (tag: string, attributes: Record<string, string | number>) => {
      const element = document.createElementNS(NS, tag)
      for (const [name, value] of Object.entries(attributes)) {
        element.setAttribute(name, String(value))
      }
      svg.appendChild(element)
      return element as SVGGraphicsElement
    }

    // A white-on-green tag. (`x`, `y`) is the point of the tag that is nearest to its target:
    // `anchor` says which side of the tag that point is on.
    const tag = (
      text: string,
      x: number,
      y: number,
      anchor: 'top' | 'bottom' | 'left' | 'right' | 'center'
    ) => {
      const PAD_X = 6
      const PAD_Y = 3
      const label = add('text', {
        'font-family': 'DM Sans Variable, sans-serif',
        'font-size': 13,
        'font-weight': 600,
        fill: 'white',
        'dominant-baseline': 'central'
      })
      label.textContent = text
      const { width, height } = label.getBBox()
      const w = width + 2 * PAD_X
      const h = height + 2 * PAD_Y
      const left = { top: x - w / 2, bottom: x - w / 2, left: x, right: x - w, center: x - w / 2 }[
        anchor
      ]
      const top = { top: y, bottom: y - h, left: y - h / 2, right: y - h / 2, center: y - h / 2 }[
        anchor
      ]
      const background = add('rect', { x: left, y: top, width: w, height: h, rx: 4, fill: COLOR })
      svg.insertBefore(background, label)
      label.setAttribute('x', String(left + PAD_X))
      label.setAttribute('y', String(top + h / 2))
    }

    const frame = ({
      x,
      y,
      width,
      height
    }: {
      x: number
      y: number
      width: number
      height: number
    }) =>
      add('rect', { x, y, width, height, rx: 6, fill: 'none', stroke: COLOR, 'stroke-width': 2.5 })

    const arrow = (from: { x: number; y: number }, to: { x: number; y: number }) =>
      add('line', {
        x1: from.x,
        y1: from.y,
        x2: to.x,
        y2: to.y,
        stroke: COLOR,
        'stroke-width': 2.5,
        'marker-end': 'url(#docs-arrowhead)'
      })

    // Covers go first, so that no cover hides a mark.
    for (const mark of annotations) {
      if (mark.kind === 'cover') {
        add('rect', { ...mark.box, fill: 'white' })
      }
    }

    for (const mark of annotations) {
      if (mark.kind === 'cover') {
        continue
      }
      if (mark.kind === 'callout') {
        const { x, y, width, height } = mark.box
        frame(mark.box)
        const middle = y + height / 2
        const isLeft = mark.x < x
        const GAP = 8
        tag(mark.label, mark.x, middle, isLeft ? 'right' : 'left')
        arrow(
          { x: mark.x + (isLeft ? GAP / 2 : -GAP / 2), y: middle },
          { x: isLeft ? x - 3 : x + width + 3, y: middle }
        )
        continue
      }
      if (mark.kind === 'frame') {
        const { x, y, width, height } = mark.box
        frame(mark.box)
        if (mark.label === undefined) {
          continue
        }
        const GAP = 4
        const side = mark.side ?? 'top'
        if (side === 'top') {
          tag(mark.label, x + width / 2, y - GAP, 'bottom')
        }
        if (side === 'bottom') {
          tag(mark.label, x + width / 2, y + height + GAP, 'top')
        }
        if (side === 'left') {
          tag(mark.label, x - GAP, y + height / 2, 'right')
        }
        if (side === 'right') {
          tag(mark.label, x + width + GAP, y + height / 2, 'left')
        }
      } else {
        arrow(mark.from, mark.to)
        if (mark.label !== undefined) {
          tag(mark.label, mark.from.x, mark.from.y, 'center')
        }
      }
    }
  }, annotations)
}

export async function clearAnnotations(page: Page) {
  await page.evaluate(() => document.getElementById('docs-annotations')?.remove())
}
