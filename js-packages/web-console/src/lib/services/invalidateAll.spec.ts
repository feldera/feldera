// The registration lives in a single line of `+layout.ts`. Losing it would
// otherwise be silent, so the unregistered call has to complain loudly.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

/** A cold module, as on a fresh app start: nothing registered yet. */
const startApp = async () => {
  vi.resetModules()
  return import('./invalidateAll')
}

let warn: ReturnType<typeof vi.spyOn>

beforeEach(() => {
  warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
})

afterEach(() => {
  warn.mockRestore()
})

describe('invalidateAll', () => {
  it('delegates to the registered function', async () => {
    const { invalidateAll, setInvalidateAll } = await startApp()
    const registered = vi.fn(async () => {})
    setInvalidateAll(registered)

    await invalidateAll()

    expect(registered).toHaveBeenCalledTimes(1)
    expect(warn).not.toHaveBeenCalled()
  })

  it('warns and resolves when no function is registered', async () => {
    const { invalidateAll } = await startApp()

    await expect(invalidateAll()).resolves.toBeUndefined()

    expect(warn).toHaveBeenCalledOnce()
    expect(warn.mock.calls[0][0]).toMatch(/registered/)
  })

  it('uses the function registered last', async () => {
    const { invalidateAll, setInvalidateAll } = await startApp()
    const first = vi.fn(async () => {})
    const second = vi.fn(async () => {})
    setInvalidateAll(first)
    setInvalidateAll(second)

    await invalidateAll()

    expect(first).not.toHaveBeenCalled()
    expect(second).toHaveBeenCalledTimes(1)
  })
})
