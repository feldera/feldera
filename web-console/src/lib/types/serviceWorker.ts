/**
 * Service Worker message types for communication between main thread and service worker
 */
export enum ServiceWorkerMessageType {
  SET_AUTH_HEADERS = 'SET_AUTH_HEADERS',
  CLEAR_AUTH_HEADERS = 'CLEAR_AUTH_HEADERS'
}

/**
 * Query parameters used by service worker to identify special requests
 */
export enum ServiceWorkerMarkers {
  ADD_AUTH_HEADER = 'worker-add-auth-header'
}

/**
 * Interface for service worker message data
 */
export interface ServiceWorkerMessage {
  type: ServiceWorkerMessageType
  data?: any
}

/**
 * Interface for auth headers message data
 */
export interface AuthHeadersMessageData {
  headers: Record<string, string>
}
