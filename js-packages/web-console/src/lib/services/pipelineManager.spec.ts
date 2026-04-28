/**
 * Unit tests verifying that pipelineManager wraps raw SDK rejections in proper
 * Error instances.
 *
 * Regression guard: plain API response objects must not be thrown directly —
 * the UI expects all API errors to be Error instances so that instanceof checks
 * and error.cause inspection work correctly.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { ErrorResponse } from '$lib/services/manager'

vi.mock('$lib/services/manager', async (importOriginal) => {
  const mod = await importOriginal<typeof import('$lib/services/manager')>()
  return {
    ...mod,
    getPipeline: vi.fn(),
    listPipelines: vi.fn(),
    patchPipeline: vi.fn(),
    putPipeline: vi.fn(),
  }
})

import * as sdkManager from '$lib/services/manager'
import {
  getExtendedPipeline,
  getPipelineThumb,
  getPipelines,
  patchPipeline,
  putPipeline,
} from '$lib/services/pipelineManager'

const API_ERROR: ErrorResponse = {
  message: 'The following pipeline edits are not allowed while storage is not cleared: `runtime_config.workers`',
  error_code: 'EditRestrictedToClearedStorage',
  details: { not_allowed: ['`runtime_config.workers`'] }
}

async function catchRejection(promise: Promise<unknown>): Promise<unknown> {
  return promise.then(
    () => { throw new Error('Expected promise to reject but it resolved') },
    (e) => e
  )
}

describe('pipelineManager API error propagation', () => {
  beforeEach(() => {
    vi.mocked(sdkManager.getPipeline).mockRejectedValue(API_ERROR)
    vi.mocked(sdkManager.listPipelines).mockRejectedValue(API_ERROR)
    vi.mocked(sdkManager.patchPipeline).mockRejectedValue(API_ERROR)
    vi.mocked(sdkManager.putPipeline).mockRejectedValue(API_ERROR)
  })

  // Functions that use mapResponse without an explicit g handler — these were
  // the broken ones before the fix.
  describe.each([
    { label: 'patchPipeline', call: () => patchPipeline('p', {}) },
    {
      label: 'putPipeline',
      call: () => putPipeline('p', { name: 'p', program_code: '' })
    },
    { label: 'getPipelines', call: () => getPipelines() },
  ])('$label (default mapResponse wrapping)', ({ call }) => {
    it('throws instanceof Error, not a plain object', async () => {
      const err = await catchRejection(call())
      expect(err).toBeInstanceOf(Error)
    })

    it('error.message matches the API error message', async () => {
      const err = (await catchRejection(call())) as Error
      expect(err.message).toBe(API_ERROR.message)
    })

    it('error.cause is the original ErrorResponse for programmatic branching', async () => {
      const err = (await catchRejection(call())) as Error
      expect(err.cause).toBe(API_ERROR)
      expect((err.cause as ErrorResponse).error_code).toBe('EditRestrictedToClearedStorage')
    })
  })

  // Functions with an explicit g handler — they already wrapped errors before
  // the fix; verify the contract holds.
  describe.each([
    { label: 'getExtendedPipeline', call: () => getExtendedPipeline('p') },
    { label: 'getPipelineThumb', call: () => getPipelineThumb('p') },
  ])('$label (explicit g handler)', ({ call }) => {
    it('throws instanceof Error, not a plain object', async () => {
      const err = await catchRejection(call())
      expect(err).toBeInstanceOf(Error)
    })

    it('error.message matches the API error message', async () => {
      const err = (await catchRejection(call())) as Error
      expect(err.message).toBe(API_ERROR.message)
    })

    it('error.cause carries error_code for programmatic branching', async () => {
      const err = (await catchRejection(call())) as Error
      expect((err.cause as ErrorResponse).error_code).toBe('EditRestrictedToClearedStorage')
    })
  })

  // The default handler must not re-wrap errors that are already Error
  // instances (TypeError for network failures, AbortError for cancellations).
  describe('Error instances pass through the default handler unchanged', () => {
    it.each([
      { label: 'patchPipeline', call: () => patchPipeline('p', {}) },
      { label: 'getPipelines', call: () => getPipelines() },
    ])('$label re-throws TypeError as the same reference', async ({ call }) => {
      const networkError = new TypeError('Failed to fetch')
      vi.mocked(sdkManager.patchPipeline).mockRejectedValue(networkError)
      vi.mocked(sdkManager.listPipelines).mockRejectedValue(networkError)
      const err = await catchRejection(call())
      expect(err).toBe(networkError)
    })
  })
})
