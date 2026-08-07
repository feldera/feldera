// The visitor ID has two sources: `ca.getDeviceId`, which exists only after the
// ConceptualHQ loader replaces its queue stub, and the localStorage key the
// loader writes. Both are read by `refreshConceptualHqDeviceId`, which the
// service calls when the loader lands, and by the module's initial read.

import { afterEach, describe, expect, it, vi } from 'vitest'
import { refreshConceptualHqDeviceId, useConceptualHq } from './useConceptualHq.svelte'

const DEVICE_ID_STORAGE_KEY = '_ca_device_id'

const loaderReports = (deviceId: string) => {
  window.ca = Object.assign(() => {}, { q: [], getDeviceId: () => deviceId })
}

afterEach(() => {
  window.ca = undefined
  window.localStorage.removeItem(DEVICE_ID_STORAGE_KEY)
  vi.restoreAllMocks()
  refreshConceptualHqDeviceId()
})

describe('useConceptualHq', () => {
  it('reads the ID from the loader API', () => {
    loaderReports('dev-123')
    window.localStorage.setItem(DEVICE_ID_STORAGE_KEY, 'dev-456')
    refreshConceptualHqDeviceId()

    expect(useConceptualHq().deviceId).toBe('dev-123')
  })

  it('falls back to the stored ID while only the queue stub exists', () => {
    window.localStorage.setItem(DEVICE_ID_STORAGE_KEY, 'dev-456')
    refreshConceptualHqDeviceId()

    expect(useConceptualHq().deviceId).toBe('dev-456')
  })

  it('is empty when neither source has an ID', () => {
    refreshConceptualHqDeviceId()

    expect(useConceptualHq().deviceId).toBe('')
  })

  it('is empty when reading storage throws', () => {
    vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
      throw new Error('site data blocked')
    })
    refreshConceptualHqDeviceId()

    expect(useConceptualHq().deviceId).toBe('')
  })
})

describe('refreshConceptualHqDeviceId', () => {
  it('reaches callers that read the ID before the refresh', () => {
    const earlyCaller = useConceptualHq()
    expect(earlyCaller.deviceId).toBe('')

    loaderReports('late-loader')
    refreshConceptualHqDeviceId()

    // One state serves every caller, so the early one sees the new ID too.
    expect(earlyCaller.deviceId).toBe('late-loader')
    expect(useConceptualHq().deviceId).toBe('late-loader')
  })

  it('notifies effects tracking the ID', async () => {
    const conceptualHq = useConceptualHq()
    const seen: string[] = []

    const stop = $effect.root(() => {
      $effect(() => {
        seen.push(conceptualHq.deviceId)
      })
    })
    await vi.waitFor(() => expect(seen).toEqual(['']))

    loaderReports('late-loader')
    refreshConceptualHqDeviceId()

    // A plain (non-state) variable would leave the effect asleep here.
    await vi.waitFor(() => expect(seen).toEqual(['', 'late-loader']))

    stop()
  })
})
