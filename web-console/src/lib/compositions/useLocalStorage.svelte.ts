/**
 * Reactive Local/Session Storage for Svelte 5
 *
 * Provides a simple, type-safe way to persist reactive state in browser storage with Svelte 5 runes.
 *
 * ## Features
 *
 * ### 1. Svelte 5 Reactivity
 * - Uses Svelte 5's `createSubscriber` for fine-grained reactivity
 * - Automatically tracks dependencies and updates reactive contexts
 * - Works seamlessly with `$derived` and `$effect`
 *
 * ### 2. Shared State (Singleton Pattern)
 * - Multiple instances with the same key share the same underlying state
 * - Updating the value in one place automatically updates all other instances
 * - Example:
 *   ```ts
 *   // In ComponentA.svelte
 *   const theme = useLocalStorage('theme', 'light')
 *   theme.value = 'dark' // Change value
 *
 *   // In ComponentB.svelte
 *   const theme = useLocalStorage('theme', 'light')
 *   // Automatically reactively updates to 'dark'
 *   ```
 *
 * ### 3. Cross-Document Synchronization
 * - Listens to browser's `storage` event for cross-tab/window sync
 * - Changes in one browser tab automatically reflect in other tabs
 * - Works across multiple windows of the same application
 *
 * ### 4. Type-Safe JSON Serialization
 * - Only accepts JSON-serializable values (primitives, arrays, objects)
 * - Type safety enforced at compile time with TypeScript
 * - Automatic JSON serialization/deserialization
 *
 * ### 5. Simple API
 * - `.value` getter/setter for accessing the reactive value
 * - `.reset()` method to restore the initial value
 * - No complex configuration needed for common use cases
 *
 * ### 6. Storage Options
 * - Supports both `localStorage` (persistent) and `sessionStorage` (session-only)
 * - Gracefully handles environments without storage (SSR, etc.)
 *
 * ## Usage
 *
 * ```ts
 * // Basic usage with localStorage
 * const darkMode = useLocalStorage('darkMode', false)
 * darkMode.value = true // Updates storage and triggers reactivity
 * darkMode.reset() // Resets to false
 *
 * // Custom storage configuration
 * const sessionData = createReactiveStorage({
 *   key: 'user-session',
 *   init: { userId: null },
 *   storage: 'session'
 * })
 * ```
 *
 * @module useLocalStorage
 */

import { createSubscriber } from 'svelte/reactivity'

/**
 * Configuration for creating a reactive storage instance
 */
interface ReactiveStorageConfig<T> {
  /** The storage key to use */
  key: string
  /** The initial/default value */
  init: T
  /** The storage type to use */
  storage: 'local' | 'session'
}

/**
 * A reactive storage instance with shared state
 */
interface ReactiveStorage<T> {
  /**
   * The current value stored in reactive storage.
   * Reading this property makes the current context reactive.
   * Setting this property updates storage and notifies all instances.
   */
  value: T
  /**
   * Reset the value back to the initial value
   */
  reset: () => void
}

// Shared state for all instances with the same key
interface SharedStorageState<T> {
  key: string
  storage: Storage | null
  currentValue: T
  init: T
  updates: Set<() => void>
}

// Global registry to share state between instances
const storageRegistry = new Map<string, SharedStorageState<unknown>>()

// Single global storage event listener (registered once)
let storageListenerRegistered = false

function ensureStorageListener() {
  if (storageListenerRegistered || !('addEventListener' in globalThis)) return

  storageListenerRegistered = true
  globalThis.addEventListener('storage', (event: StorageEvent) => {
    const { key, storageArea, newValue } = event
    if (!key || !storageArea) return

    // Determine storage type and construct registry key
    const storageType =
      storageArea === localStorage ? 'local' : storageArea === sessionStorage ? 'session' : null
    if (!storageType) return

    // Look up the shared state directly using the registry key
    const registryKey = `${storageType}:${key}`
    const shared = storageRegistry.get(registryKey)
    if (!shared) return

    // Update the value and notify all subscribers
    shared.currentValue = JSON.parse(newValue || 'null')
    shared.updates.forEach((update) => update())
  })
}

/**
 * Create a reactive persistent storage instance with a shared state.
 *
 * Multiple calls with the same key will share the same underlying state,
 * meaning updates in one place automatically update all other instances.
 *
 * @param config - Configuration object with key, initial value, and storage type
 * @returns A reactive storage instance with `.value` and `.reset()`
 *
 * @example
 * ```ts
 * const sessionTheme = createReactiveStorage({
 *   key: 'theme',
 *   init: 'light',
 *   storage: 'session'
 * })
 * sessionTheme.value = 'dark'
 * ```
 */
export function createReactiveStorage<T>(config: ReactiveStorageConfig<T>): ReactiveStorage<T> {
  const { key, init, storage: storageType } = config

  // Create a unique registry key that includes storage type
  const registryKey = `${storageType}:${key}`

  // Check if we already have a shared state for this key
  let shared = storageRegistry.get(registryKey) as SharedStorageState<T> | undefined

  if (!shared) {
    // Determine which storage to use
    let storage: Storage | null = null
    if (storageType === 'local' && 'localStorage' in globalThis) {
      storage = localStorage
    } else if (storageType === 'session' && 'sessionStorage' in globalThis) {
      storage = sessionStorage
    }

    // Initialize from storage
    let currentValue: T = init
    const json = storage?.getItem(key)
    if (json) {
      currentValue = JSON.parse(json) as T
    } else if (init !== null && init !== undefined) {
      storage?.setItem(key, JSON.stringify(init))
    }

    // Create new shared state
    shared = {
      key,
      storage,
      currentValue,
      init,
      updates: new Set()
    }
    storageRegistry.set(registryKey, shared)

    // Ensure the global storage event listener is registered (only once)
    ensureStorageListener()
  }

  // Create subscriber for this instance
  const subscribe = createSubscriber((update) => {
    shared!.updates.add(update)
  })

  // Create the reactive storage object
  const reactive: ReactiveStorage<T> = {
    get value(): T {
      subscribe()
      return shared!.currentValue
    },
    set value(newValue: T) {
      shared!.currentValue = newValue
      if (!shared!.storage) return
      if (newValue === null) {
        shared!.storage.removeItem(key)
      } else {
        shared!.storage.setItem(key, JSON.stringify(newValue))
      }
      // Notify all subscribers (all instances)
      shared!.updates.forEach((update) => update())
    },
    reset() {
      reactive.value = shared!.init
    }
  }

  return reactive
}

/**
 * Create a reactive localStorage instance with a shared state.
 *
 * Multiple calls with the same key will share the same underlying state,
 * meaning updates in one place automatically update all other instances.
 *
 * @param key - The localStorage key to use
 * @param init - The initial/default value
 * @returns A reactive storage instance with `.value` and `.reset()`
 *
 * @example
 * ```ts
 * // In any component
 * const darkMode = useLocalStorage('app:darkMode', false)
 *
 * // Read the value (reactive)
 * console.log(darkMode.value) // false
 *
 * // Update the value (triggers reactivity everywhere)
 * darkMode.value = true
 *
 * // Reset to initial value
 * darkMode.reset()
 * ```
 */
export function useLocalStorage<T>(key: string, init: T) {
  return createReactiveStorage({ key, init, storage: 'local' })
}
