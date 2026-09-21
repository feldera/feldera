/**
 * Tests for the reactive view of the support bundle history.
 *
 * These run in the browser project, against the real IndexedDB the history lives in,
 * because what is under test is whether Dexie's `liveQuery` reaches Svelte's
 * reactivity at all.
 */

import { beforeEach, describe, expect, it, vi } from 'vitest'

const { observeBundleHistory } = vi.hoisted(() => ({
  observeBundleHistory: vi.fn<typeof actualHistory.observeBundleHistory>()
}))

// The history module is not replaced: these tests run against the real database.
// Only the observable is wrapped, so that one test can make a read fail.
vi.mock('$lib/services/supportBundleHistory', async (importOriginal) => ({
  ...(await importOriginal<typeof import('$lib/services/supportBundleHistory')>()),
  observeBundleHistory
}))

// This import follows the vi.mock call above, so that the mock is in place.
import { useSupportBundleHistory } from './useSupportBundleHistory.svelte'

const actualHistory = await vi.importActual<typeof import('$lib/services/supportBundleHistory')>(
  '$lib/services/supportBundleHistory'
)

/** Reads `current` in an effect, which is what starts the subscription. */
const readInAnEffect = () => {
  const history = useSupportBundleHistory()
  const stop = $effect.root(() => {
    $effect(() => void history.current)
  })
  return { history, stop }
}

const add = (name: string) => actualHistory.addToBundleHistory(new File(['archive'], name))

/**
 * Long enough for an unwanted update to have arrived, had one been coming, and for
 * `createSubscriber` to count a destroyed reader down. It does that in a microtask.
 */
const settle = () => new Promise((resolve) => setTimeout(resolve, 100))

describe('useSupportBundleHistory', () => {
  beforeEach(async () => {
    vi.restoreAllMocks()
    observeBundleHistory.mockImplementation(actualHistory.observeBundleHistory)
    await actualHistory.clearBundleHistory()
  })

  it('fills with the history once an effect reads it', async () => {
    await add('already-there.zip')

    const { history, stop } = readInAnEffect()

    try {
      await vi.waitFor(() =>
        expect(history.current.map((entry) => entry.name)).toEqual(['already-there.zip'])
      )
    } finally {
      stop()
    }
  })

  it('follows a bundle added later, with nothing telling it to re-read', async () => {
    const { history, stop } = readInAnEffect()

    try {
      await vi.waitFor(() => expect(history.current).toHaveLength(0))
      await add('added-later.zip')

      await vi.waitFor(() =>
        expect(history.current.map((entry) => entry.name)).toEqual(['added-later.zip'])
      )
    } finally {
      stop()
    }
  })

  it('stops following it once the last reader is destroyed', async () => {
    await add('before.zip')
    const { history, stop } = readInAnEffect()
    // On the name, not the length: `current` still holds the list the previous test
    // left behind, which is also one entry long.
    await vi.waitFor(() =>
      expect(history.current.map((entry) => entry.name)).toEqual(['before.zip'])
    )

    stop()
    await settle()
    await add('after.zip')
    await settle()

    expect(history.current.map((entry) => entry.name)).toEqual(['before.zip'])
  })

  it('holds an empty history when the database cannot be read', async () => {
    await add('unreachable.zip')
    observeBundleHistory.mockReturnValue({
      subscribe: (_next: unknown, error?: (e: unknown) => void) => {
        error?.(new Error('IndexedDB is unavailable'))
        return { unsubscribe: () => {}, closed: true }
      }
    } as unknown as ReturnType<typeof actualHistory.observeBundleHistory>)
    vi.spyOn(console, 'warn').mockImplementation(() => {})

    const { history, stop } = readInAnEffect()

    try {
      await vi.waitFor(() => expect(history.current).toEqual([]))
    } finally {
      stop()
    }
  })
})
