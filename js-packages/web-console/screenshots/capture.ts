import { expect, type Locator, type Page } from '@playwright/test'
import type { Box } from './annotate'

/**
 * The limit for the app to start after a navigation. The app loads its script after the `load` event
 * of the page, and the manager takes about 2 seconds to send the 3.7 MB of it.
 */
export const APP_START_TIMEOUT = 5_000

/**
 * Waits until `locator` has a size, runs no animation, and keeps its box for one frame, as when a
 * menu has grown open. The animations alone are not enough: right after a click, a transition may
 * not have started yet.
 */
export const waitForTransitions = (locator: Locator) =>
  locator.evaluate(
    (element) =>
      new Promise<void>((resolve) => {
        let last = ''
        const check = () => {
          const { x, y, width, height } = element.getBoundingClientRect()
          const box = `${x} ${y} ${width} ${height}`
          const running = element
            .getAnimations({ subtree: true })
            .some((animation) => animation.playState === 'running')
          if (box === last && height > 0 && !running) {
            return resolve()
          }
          last = box
          requestAnimationFrame(check)
        }
        check()
      })
  )

/**
 * Moves to `path` inside the app, as a click on a link does. SvelteKit routes a click on any link of
 * the page, so the app does not load and start again.
 */
export async function navigate(page: Page, path: string) {
  await page.evaluate((path) => {
    const link = document.createElement('a')
    link.href = path
    document.body.append(link)
    link.click()
    link.remove()
  }, path)
  await page.waitForURL(path)
}

/** The `toHaveScreenshot` name of the image `docs.feldera.com/docs/<folder>/<file>`. */
export const docsImage = (folder: string, file: string) => [folder, file]

/** Compares the area `clip` of the page with the docs image `name`. */
export const expectClip = (page: Page, name: string[], clip: Box) =>
  expect(page).toHaveScreenshot(name, { clip: roundBox(clip) })

/** A clip with whole CSS pixels, so the image size does not change by one pixel between runs. */
const roundBox = (box: Box): Box => ({
  x: Math.round(box.x),
  y: Math.round(box.y),
  width: Math.round(box.width),
  height: Math.round(box.height)
})
