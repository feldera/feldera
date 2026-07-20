import { describe, expect, it, vi } from 'vitest'
import { createLookupCoordinator, noMatches } from './lookup'

describe('createLookupCoordinator', () => {
  it('forwards query and direction to the active tab handler and returns its progress', () => {
    const coordinator = createLookupCoordinator()
    const logs = vi.fn().mockReturnValue({ current: 2, total: 3 })
    coordinator.register('Logs', logs)

    expect(coordinator.execute('Logs', 'error', 'next')).toEqual({ current: 2, total: 3 })
    expect(logs).toHaveBeenCalledWith('error', 'next')
  })

  it('routes only to the active tab, leaving other tabs untouched', () => {
    const coordinator = createLookupCoordinator()
    const logs = vi.fn().mockReturnValue({ current: 1, total: 1 })
    const metrics = vi.fn().mockReturnValue({ current: 1, total: 2 })
    coordinator.register('Logs', logs)
    coordinator.register('Metrics', metrics)

    expect(coordinator.execute('Metrics', 'q', 'prev')).toEqual({ current: 1, total: 2 })
    expect(metrics).toHaveBeenCalledWith('q', 'prev')
    expect(logs).not.toHaveBeenCalled()
  })

  it('defaults the direction to "next"', () => {
    const coordinator = createLookupCoordinator()
    const handler = vi.fn().mockReturnValue(noMatches)
    coordinator.register('Logs', handler)

    coordinator.execute('Logs', 'q')
    expect(handler).toHaveBeenCalledWith('q', 'next')
  })

  it('returns noMatches for an unknown tab without throwing', () => {
    const coordinator = createLookupCoordinator()
    expect(coordinator.execute('Nope', 'q', 'next')).toEqual(noMatches)
  })

  it('unregisters via the returned disposer', () => {
    const coordinator = createLookupCoordinator()
    const handler = vi.fn().mockReturnValue({ current: 1, total: 5 })
    const dispose = coordinator.register('Logs', handler)

    expect(coordinator.execute('Logs', 'q', 'next')).toEqual({ current: 1, total: 5 })
    dispose()
    expect(coordinator.execute('Logs', 'q', 'next')).toEqual(noMatches)
    expect(handler).toHaveBeenCalledTimes(1)
  })
})
