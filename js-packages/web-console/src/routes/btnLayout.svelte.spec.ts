/**
 * Tests for the `.btn` override in `layout.css` that puts a leading icon at the left
 * edge and centres the label in the space after it.
 *
 * These run in the browser project, whose setup loads `layout.css`. The icon is a box
 * of fixed width, so the measurements do not wait for the icon font.
 */

import { afterEach, describe, expect, it } from 'vitest'

/** Skeleton's `.btn` gap, in px. */
const GAP = 8
const ICON_WIDTH = 24

let host: HTMLElement | undefined

afterEach(() => {
  host?.remove()
  host = undefined
})

/** Mounts a button with an optional leading icon, and measures its parts. */
const renderBtn = ({
  width,
  icon = true,
  iconClass = 'fd'
}: {
  width?: number
  icon?: boolean
  /** Any class but `fd` leaves the button out of the override. */
  iconClass?: string
}) => {
  host = document.createElement('div')
  document.body.appendChild(host)
  const button = document.createElement('button')
  button.className = 'btn'
  if (width) {
    button.style.width = `${width}px`
  }
  if (icon) {
    const iconBox = document.createElement('span')
    iconBox.className = iconClass
    iconBox.style.display = 'inline-block'
    iconBox.style.width = `${ICON_WIDTH}px`
    iconBox.style.height = '1em'
    button.appendChild(iconBox)
  }
  button.appendChild(document.createTextNode('Label'))
  host.appendChild(button)

  const range = document.createRange()
  range.selectNodeContents(button.lastChild!)
  const style = getComputedStyle(button)
  const box = button.getBoundingClientRect()
  return {
    button: box,
    icon: button.querySelector('.fd')?.getBoundingClientRect(),
    label: range.getBoundingClientRect(),
    contentLeft: box.left + parseFloat(style.paddingLeft) + parseFloat(style.borderLeftWidth),
    contentRight: box.right - parseFloat(style.paddingRight) - parseFloat(style.borderRightWidth)
  }
}

describe('.btn with a leading icon', () => {
  it('keeps the icon at the left edge and centres the label after it', () => {
    const { icon, label, contentLeft, contentRight } = renderBtn({ width: 320 })

    expect(icon!.left).toBeCloseTo(contentLeft, 0)
    // As much space between the icon's gap and the label as after the label.
    const before = label.left - (icon!.right + GAP)
    const after = contentRight - label.right
    expect(before).toBeGreaterThan(GAP)
    expect(Math.abs(before - after)).toBeLessThanOrEqual(1)
  })

  it('takes no more width than it did without the override', () => {
    const centred = renderBtn({}).button.width
    host!.remove()
    // The same icon box under another class is what the plain `.btn` makes of it.
    const plain = renderBtn({ iconClass: 'icon' }).button.width

    expect(centred).toBeCloseTo(plain, 1)
  })
})

describe('.btn without an icon', () => {
  it('centres the label in the whole button', () => {
    const { label, contentLeft, contentRight } = renderBtn({ width: 320, icon: false })

    expect(Math.abs(label.left - contentLeft - (contentRight - label.right))).toBeLessThanOrEqual(1)
  })
})
