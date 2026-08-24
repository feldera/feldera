/**
 * Only the `(shell)` layout polls cluster health, so its verdict has to expire with the
 * poller: the profile menu's health dot renders on every page, the profile viewer and the
 * tenant picker included, and nothing outside the shell would ever correct a reading left
 * behind there.
 */
import { flushSync } from 'svelte'
import { afterEach, describe, expect, it, vi } from 'vitest'

// Hands the test the resolver of the poll the hook has in flight, so a request can be left
// unanswered across an unmount.
const poll = vi.hoisted(() => ({
  answer: undefined as ((event: unknown) => void) | undefined
}))

vi.mock('$lib/compositions/usePipelineManager.svelte', () => ({
  usePipelineManager: () => ({
    getClusterEvent: () =>
      new Promise((resolve) => {
        poll.answer = resolve
      })
  })
}))

// Imported AFTER vi.mock so the mock takes effect.
import { useClusterHealth, useRefreshClusterHealth } from './useClusterHealth.svelte'

const recordedAt = '2026-05-01T12:00:00Z'

const healthyEvent = {
  api_status: 'Healthy',
  compiler_status: 'Healthy',
  runner_status: 'Healthy',
  stale: false,
  recorded_at: recordedAt
}

const healthy = {
  api: 'healthy',
  compiler: 'healthy',
  runner: 'healthy',
  stale: false,
  recordedAt: new Date(recordedAt)
}

/**
 * Mounts the poller the way the `(shell)` layout does; the result unmounts it. The flush runs
 * the hook's effects, which a mounted shell has long since done by the time the user navigates
 * away.
 */
const mountPoller = () => {
  const unmount = $effect.root(() => useRefreshClusterHealth())
  flushSync()
  return unmount
}

let unmountPoller: (() => void) | undefined

const leaveTheShell = () => {
  unmountPoller!()
  unmountPoller = undefined
}

afterEach(() => {
  unmountPoller?.()
  unmountPoller = undefined
  poll.answer = undefined
})

describe('useRefreshClusterHealth', () => {
  it('publishes the verdict a poll answers with', async () => {
    unmountPoller = mountPoller()
    poll.answer!(healthyEvent)
    await vi.waitFor(() => expect(useClusterHealth().current).toEqual(healthy))
  })

  it('forgets the verdict it polled last when it unmounts', async () => {
    unmountPoller = mountPoller()
    poll.answer!(healthyEvent)
    await vi.waitFor(() => expect(useClusterHealth().current).toEqual(healthy))

    leaveTheShell()

    expect(useClusterHealth().current).toBeUndefined()
  })

  it('takes no verdict from a poll left in flight at unmount', async () => {
    // The request is issued before the navigation and answers after it, with nothing left
    // polling to age the answer out.
    unmountPoller = mountPoller()
    const answer = poll.answer!
    leaveTheShell()

    answer(healthyEvent)

    await new Promise((resolve) => setTimeout(resolve, 0))
    expect(useClusterHealth().current).toBeUndefined()
  })

  it('gives no verdict on returning to the shell until a poll answers', async () => {
    unmountPoller = mountPoller()
    poll.answer!(healthyEvent)
    await vi.waitFor(() => expect(useClusterHealth().current).toEqual(healthy))
    leaveTheShell()

    unmountPoller = mountPoller()
    expect(useClusterHealth().current).toBeUndefined()

    poll.answer!(healthyEvent)
    await vi.waitFor(() => expect(useClusterHealth().current).toEqual(healthy))
  })
})
