// The registration lives in a single line of `+layout.ts`. Losing it would
// otherwise be silent, so the unregistered call has to complain loudly.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

/** A cold module, as on a fresh app start: nothing registered yet. */
const startApp = async () => {
  vi.resetModules()
  return import('./refreshAll')
}

let warn: ReturnType<typeof vi.spyOn>

beforeEach(() => {
  warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
})

afterEach(() => {
  warn.mockRestore()
})

describe('refreshAll', () => {
  it('delegates to the registered function', async () => {
    const { refreshAll, setRefreshAll } = await startApp()
    const registered = vi.fn(async () => {})
    setRefreshAll(registered)

    await refreshAll()

    expect(registered).toHaveBeenCalledTimes(1)
    expect(warn).not.toHaveBeenCalled()
  })

  it('warns and resolves when no function is registered', async () => {
    const { refreshAll } = await startApp()

    await expect(refreshAll()).resolves.toBeUndefined()

    expect(warn).toHaveBeenCalledOnce()
    expect(warn.mock.calls[0][0]).toMatch(/registered/)
  })

  it('uses the function registered last', async () => {
    const { refreshAll, setRefreshAll } = await startApp()
    const first = vi.fn(async () => {})
    const second = vi.fn(async () => {})
    setRefreshAll(first)
    setRefreshAll(second)

    await refreshAll()

    expect(first).not.toHaveBeenCalled()
    expect(second).toHaveBeenCalledTimes(1)
  })
})
