import { ServiceWorkerMessageType } from '$lib/types/serviceWorker'
import type { ServiceWorkerMessage, AuthHeadersMessageData } from '$lib/types/serviceWorker'

/**
 * Sends a message to the service worker.
 */
const postServiceWorkerMessage = (type: ServiceWorkerMessageType, data?: any) => {
  const message: ServiceWorkerMessage = { type, data }
  navigator.serviceWorker?.controller?.postMessage(message)
}

/**
 * Updates authentication headers in the service worker.
 */
export const updateServiceWorkerAuthHeaders = (headers: Record<string, string>) => {
  const data: AuthHeadersMessageData = { headers }
  postServiceWorkerMessage(ServiceWorkerMessageType.SET_AUTH_HEADERS, data)
}

/**
 * Clears authentication headers from the service worker.
 */
export const clearServiceWorkerAuthHeaders = () => {
  postServiceWorkerMessage(ServiceWorkerMessageType.CLEAR_AUTH_HEADERS)
}
