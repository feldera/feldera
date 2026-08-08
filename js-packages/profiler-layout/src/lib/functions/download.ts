/** Trigger a browser download of `content` as a file named `filename`.
 *
 *  Wraps the content in a Blob and clicks a transient anchor. Revocation of the object URL is
 *  deferred so the browser can start reading the blob before the URL is released.
 */
export function downloadFile(content: BlobPart, filename: string, mimeType: string): void {
  const url = URL.createObjectURL(new Blob([content], { type: mimeType }))
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = filename
  anchor.rel = 'noopener'
  anchor.click()
  setTimeout(() => URL.revokeObjectURL(url), 0)
}
