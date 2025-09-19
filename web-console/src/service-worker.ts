import {} from '$service-worker'
import { AuthorizationProxy } from '$lib/services/serviceWorker/authorizationProxy'

// Service worker for handling various features
declare const self: ServiceWorkerGlobalScope

// Initialize download forwarder
const authorizationProxy = new AuthorizationProxy()

// Install event
self.addEventListener('install', (event) => {
  // Skip waiting to activate immediately
  self.skipWaiting()
})

// Activate event
self.addEventListener('activate', (event) => {
  // Take control of all clients immediately
  event.waitUntil(self.clients.claim())
})

// Setup feature modules
authorizationProxy.setupListeners(self)

// Future modules can be added here:
// cacheManager.setupListeners(self)
// pushNotifications.setupListeners(self)
// etc.
