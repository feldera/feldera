// localStorage key where the ConceptualHQ loader persists the visitor (device) ID.
const DEVICE_ID_STORAGE_KEY = '_ca_device_id'

// Read Device ID from localStorage while the loader has not run yet
// so a returning visitor is identified on the first render.
const readDeviceId = (): string => {
  if (typeof window === 'undefined') {
    return ''
  }
  const fromLoader = window.ca?.getDeviceId?.()
  if (fromLoader) {
    return fromLoader
  }
  try {
    return window.localStorage.getItem(DEVICE_ID_STORAGE_KEY) ?? ''
  } catch {
    // Reading localStorage throws when the browser blocks site data.
    return ''
  }
}

const visitor = $state({ deviceId: readDeviceId() })

/**
 * Re-read the visitor ID. The ConceptualHQ service calls this when its loader
 * script is run, which is when a first-time visitor gets an ID.
 */
export const refreshConceptualHqDeviceId = () => {
  visitor.deviceId = readDeviceId()
}

/**
 * `deviceId` is '' while no ID is known: analytics disabled, a first visit
 * when the loader hasn't run yet, or localStorage is blocked.
 * Callers treat '' as "cannot attribute", not as an error.
 */
export const useConceptualHq = () => ({
  get deviceId() {
    return visitor.deviceId
  }
})
