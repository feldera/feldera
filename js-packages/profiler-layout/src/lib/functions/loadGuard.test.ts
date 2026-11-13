import { describe, expect, it } from 'vitest'
import { createLoadGuard } from './loadGuard'

describe('createLoadGuard', () => {
  // The work of a snapshot switch can block the main thread for seconds. The browser must render
  // the loading indicator before the work starts, or the user never sees it.
  it('renders a frame between setLoading(true) and the start of the work', async () => {
    let hasRenderedFrame = false
    const withLoadGuard = createLoadGuard({
      setLoading: (loading) => {
        if (loading) {
          requestAnimationFrame(() => {
            hasRenderedFrame = true
          })
        }
      }
    })
    let hasRenderedFrameAtWorkStart: boolean | undefined
    await withLoadGuard(
      async () => {
        hasRenderedFrameAtWorkStart = hasRenderedFrame
      },
      (e) => {
        throw e
      }
    )
    expect(hasRenderedFrameAtWorkStart).toBe(true)
  })
})
