export const triggerFileDownload = (fileName: string, content: Blob | File) => {
  const url = URL.createObjectURL(content)
  const a = document.createElement('a')
  a.href = url
  a.download = fileName
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}
