import { afterEach, describe, expect, it, vi } from 'vitest'
import { downloadFile } from './download'

describe('downloadFile', () => {
  afterEach(() => vi.restoreAllMocks())

  it('downloads the content as a named blob of the given type', async () => {
    let capturedBlob: Blob | undefined
    const createObjectURL = vi
      .spyOn(URL, 'createObjectURL')
      .mockImplementation((source: Blob | MediaSource) => {
        capturedBlob = source as Blob
        return 'blob:mock-url'
      })
    const revokeObjectURL = vi.spyOn(URL, 'revokeObjectURL').mockImplementation(() => {})

    // Capture the transient anchor and suppress the real navigation/download.
    let anchor: HTMLAnchorElement | undefined
    vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(function (
      this: HTMLAnchorElement
    ) {
      anchor = this
    })

    downloadFile('<svg></svg>', 'diagram.svg', 'image/svg+xml')

    // Synchronous assertions first: the deferred revoke must not have run yet, and any `await`
    // below would let its macrotask fire.
    expect(createObjectURL).toHaveBeenCalledOnce()
    expect(capturedBlob?.type).toBe('image/svg+xml')
    expect(anchor?.download).toBe('diagram.svg')
    expect(anchor?.getAttribute('href')).toBe('blob:mock-url')
    expect(revokeObjectURL).not.toHaveBeenCalled()

    expect(await capturedBlob?.text()).toBe('<svg></svg>')

    // Revocation is deferred so the browser can read the blob before the URL is released.
    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(revokeObjectURL).toHaveBeenCalledWith('blob:mock-url')
  })
})
