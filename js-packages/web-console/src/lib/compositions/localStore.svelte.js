/**
 * @template T
 */
export class LocalStore {
  value = $state(/** @type {T} */ (/** @type {unknown} */ undefined))
  key = ''

  /**
   * @param {string} key
   * @param {T} value
   */
  constructor(key, value) {
    this.key = key
    this.value = value

    if ('window' in globalThis) {
      const item = localStorage.getItem(key)
      if (item) {
        this.value = JSON.parse(item)
      }
    }

    $effect(() => {
      localStorage.setItem(this.key, JSON.stringify(this.value))
    })
  }

  remove() {
    localStorage.removeItem(this.key)
    delete stores[this.key]
  }
}

/**
 * @type Record<string, any>
 */
const stores = {}

/**
 * @template T
 * @param {string} key
 * @param {T} value
 * @returns {LocalStore<T>}
 */
export function useLocalStorage(key, value) {
  stores[key] ??= new LocalStore(key, value)
  return stores[key]
}
