import { ServiceWorkerMessageType, ServiceWorkerMarkers } from '$lib/types/serviceWorker'
import type { AuthHeadersMessageData, ServiceWorkerMessage } from '$lib/types/serviceWorker'

/**
 * Service worker for adding authorization header to requests.
 */
export class AuthorizationProxy {
  private authHeaders: Record<string, string> = {}

  /**
   * Sets up relevant listeners on the service worker.
   * @param serviceWorker - The ServiceWorkerGlobalScope instance
   */
  setupListeners(serviceWorker: ServiceWorkerGlobalScope): void {
    // Message handler for auth headers
    serviceWorker.addEventListener('message', (event) => {
      this.handleMessage(event.data)
    })

    // Fetch handler for intercepting HTTP requests
    serviceWorker.addEventListener('fetch', (event) => {
      const url = new URL(event.request.url)

      if (url.searchParams.has(ServiceWorkerMarkers.ADD_AUTH_HEADER)) {
        event.respondWith(this.addAuthorizationHeader(event.request))
      }
    })
  }

  /**
   * Handles messages from the main thread
   */
  private handleMessage(message: ServiceWorkerMessage): void {
    switch (message.type) {
      case ServiceWorkerMessageType.SET_AUTH_HEADERS:
        const data = message.data as AuthHeadersMessageData
        this.authHeaders = data.headers
        break

      case ServiceWorkerMessageType.CLEAR_AUTH_HEADERS:
        this.authHeaders = {}
        break

      default:
        console.log('Authorization proxy: Unknown message type:', message.type)
    }
  }

  /**
   * Add authorization header, remove marker, send request and forward the response
   */
  private async addAuthorizationHeader(request: Request): Promise<Response> {
    try {
      // Remove the service worker marker from URL
      const url = new URL(request.url)
      url.searchParams.delete(ServiceWorkerMarkers.ADD_AUTH_HEADER)

      // Create new request with authentication headers
      const authenticatedRequest = new Request(url.toString(), {
        method: request.method,
        headers: {
          ...Object.fromEntries(request.headers.entries()),
          ...this.authHeaders
        },
        body: request.body,
        mode: request.mode,
        credentials: request.credentials,
        cache: request.cache,
        redirect: request.redirect,
        referrer: request.referrer
      })

      // Forward the request with authentication
      const response = await fetch(authenticatedRequest)

      if (!response.ok) {
        throw new Error(`Request failed: ${response.status} ${response.statusText}`)
      }

      return response
    } catch (error) {
      // Return error response
      return new Response(
        JSON.stringify({
          error: error instanceof Error ? error.message : 'Request failed'
        }),
        {
          status: 500,
          headers: { 'Content-Type': 'application/json' }
        }
      )
    }
  }
}
