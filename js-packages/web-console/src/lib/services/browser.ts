/**
 * Creates a temporary anchor element and triggers a download.
 * Used to avoid code duplication across different download approaches.
 */
const triggerAnchorDownload = (href: string, fileName: string) => {
  const anchor = document.createElement('a')
  anchor.href = href
  anchor.download = fileName
  anchor.style.display = 'none'
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
}

export const triggerFileDownload = (fileName: string, content: Blob | File) => {
  const url = URL.createObjectURL(content)
  triggerAnchorDownload(url, fileName)
  URL.revokeObjectURL(url)
}
