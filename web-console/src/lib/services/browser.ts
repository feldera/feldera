import { updateServiceWorkerAuthHeaders } from '$lib/services/serviceWorker/authorizationProxyClient'
import { ServiceWorkerMarkers } from '$lib/types/serviceWorker'

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

/**
 * Downloads a file using either Service Worker (streaming) or Fetch API (fallback).
 *
 * Service Worker approach provides true streaming with no memory buffering.
 * Fetch API fallback loads the entire file into memory as a Blob.
 *
 * @param fileName - The filename for download
 * @param url - The URL to download from
 * @param headers - Custom headers (e.g., Authorization)
 * @param preferServiceWorker - Whether to try Service Worker first (default: true)
 */
export const triggerStreamDownload = async (
  fileName: string,
  url: string,
  headers: Record<string, string> = {},
  preferServiceWorker = true
) => {
  // Try Service Worker approach first (true streaming)
  if (preferServiceWorker) {
    try {
      updateServiceWorkerAuthHeaders(headers)

      const downloadUrl = new URL(url, window.location.origin)
      downloadUrl.searchParams.set(ServiceWorkerMarkers.ADD_AUTH_HEADER, 'true')

      triggerAnchorDownload(downloadUrl.toString(), fileName)
      return
    } catch (error) {
      console.warn('Service Worker download failed, falling back to Fetch API:', error)
    }
  }

  // Fallback to Fetch API approach
  const response = await fetch(url, { headers })

  if (!response.ok) {
    throw new Error(`Download failed: ${response.status} ${response.statusText}`)
  }

  triggerFileDownload(fileName, await response.blob())
}
