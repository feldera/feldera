import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render } from 'vitest-browser-svelte'

// downloadFile is a side-effecting DOM helper; stub it so the test asserts wiring, not a real
// browser download. vi.mock is hoisted, so the factory creates the spy inline.
vi.mock('../functions/download', () => ({ downloadFile: vi.fn() }))

import { downloadFile } from '../functions/download'
import DiagramFileMenu from './DiagramFileMenu.svelte'

const findButton = (container: HTMLElement, text: string) =>
  Array.from(container.querySelectorAll('button')).find((b) => b.textContent?.trim() === text)

const openExportItem = async (container: HTMLElement) => {
  findButton(container, 'File')?.click()
  await expect.poll(() => findButton(container, 'Export as .svg') !== undefined).toBe(true)
  findButton(container, 'Export as .svg')?.click()
}

describe('DiagramFileMenu', () => {
  beforeEach(() => vi.clearAllMocks())

  it('downloads the exported SVG, naming the file from the pipeline and snapshot date', async () => {
    const exportSvg = vi.fn(async () => '<svg></svg>')
    const onError = vi.fn()
    const { container } = render(DiagramFileMenu, {
      exportSvg,
      onError,
      pipelineName: 'my-pipeline',
      snapshotDate: new Date(2024, 2, 9)
    })

    await openExportItem(container)

    await expect.poll(() => exportSvg.mock.calls.length).toBe(1)
    await expect.poll(() => vi.mocked(downloadFile).mock.calls.length).toBe(1)
    const [content, filename, mimeType] = vi.mocked(downloadFile).mock.calls[0]!
    expect(content).toBe('<svg></svg>')
    expect(filename).toBe('my-pipeline-2024.03.09.svg')
    expect(mimeType).toBe('image/svg+xml')
    expect(onError).not.toHaveBeenCalled()
  })

  it('reports an error and skips the download when nothing is rendered yet', async () => {
    const exportSvg = vi.fn(async () => null)
    const onError = vi.fn()
    const { container } = render(DiagramFileMenu, { exportSvg, onError })

    await openExportItem(container)

    await expect.poll(() => onError.mock.calls.length).toBe(1)
    expect(downloadFile).not.toHaveBeenCalled()
  })
})
