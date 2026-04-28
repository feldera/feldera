/**
 * Shared test helpers for pipeline lifecycle management.
 *
 * Used by both Playwright e2e tests and Vitest integration tests to avoid
 * duplicating pipeline create/compile/start/stop/delete boilerplate.
 */

import { client } from '$lib/services/manager/client.gen'
import {
  deletePipeline,
  type ExtendedPipeline,
  getPipelineThumb,
  type PipelineThumb,
  postPipelineAction,
  programStatusOf,
  putPipeline
} from '$lib/services/pipelineManager'

export { type ExtendedPipeline }

/**
 * Configure the API client base URL for tests.
 *
 * Reads from `FELDERA_TEST_API_ORIGIN` (shared) or `PLAYWRIGHT_API_ORIGIN`
 * (Playwright-specific), falling back to `http://localhost:8080`.
 */
export function configureTestClient() {
  // In browser tests (vitest with browser mode), `process` is undefined — fall
  // back to import.meta.env (Vite exposes env vars via `VITE_*`-prefixed vars
  // and also via import.meta.env at build time).
  const env: Record<string, string | undefined> =
    typeof process !== 'undefined' && process.env
      ? (process.env as Record<string, string | undefined>)
      : ((import.meta as unknown as { env?: Record<string, string | undefined> }).env ?? {})
  const origin = (
    env.FELDERA_TEST_API_ORIGIN ??
    env.PLAYWRIGHT_API_ORIGIN ??
    'http://localhost:8080'
  ).replace(/\/$/, '')
  client.setConfig({ baseUrl: origin })
  return origin
}

/**
 * Poll `getPipelineThumb` until `predicate` returns true or `timeoutMs` elapses.
 */
export async function waitForPipeline(
  pipelineName: string,
  predicate: (pipeline: PipelineThumb) => boolean | Promise<boolean>,
  timeoutMs = 120_000
) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const thumb = await getPipelineThumb(pipelineName)
    if (await predicate(thumb)) {
      return thumb
    }
    await new Promise((r) => setTimeout(r, 1000))
  }
  const { status } = await getPipelineThumb(pipelineName)
  throw new Error(
    `waitForPipelineStatus timed out for "${pipelineName}". status=${JSON.stringify(status)}`
  )
}

/**
 * Wait for a pipeline's SQL/Rust compilation to finish successfully.
 * Throws on compilation errors.
 */
export async function waitForCompilation(pipelineName: string, timeoutMs = 600_000) {
  await waitForPipeline(
    pipelineName,
    ({ status }) => {
      const programStatus = programStatusOf(status)
      if (
        programStatus === 'SqlError' ||
        programStatus === 'RustError' ||
        programStatus === 'SystemError'
      ) {
        throw new Error(`Compilation failed for "${pipelineName}": ${programStatus}`)
      }
      return programStatus === 'Success'
    },
    timeoutMs
  )
}

/**
 * Start a pipeline and wait until it reaches the Running state.
 * Automatically handles the AwaitingApproval → approve_changes transition.
 */
export async function startPipelineAndWaitForRunning(pipelineName: string, timeoutMs = 60_000) {
  await postPipelineAction(pipelineName, 'start')
  await waitForPipeline(
    pipelineName,
    async ({ status }) => {
      if (status === 'AwaitingApproval') {
        await postPipelineAction(pipelineName, 'approve_changes').catch(() => {})
      }
      return status === 'Running'
    },
    timeoutMs
  )
}

/**
 * Kill a pipeline and wait until it reaches the Stopped state.
 */
export async function killPipelineAndWaitForStopped(pipelineName: string) {
  try {
    await postPipelineAction(pipelineName, 'kill')
  } catch {
    // Ignore if already stopped
  }
  await waitForPipeline(pipelineName, ({ status }) => status === 'Stopped')
}

export const clearAndDeletePipeline = async (pipelineName: string) => {
  await postPipelineAction(pipelineName, 'clear')
  await waitForPipeline(pipelineName, (p) => p.storageStatus === 'Cleared', 30_000)
  await deletePipeline(pipelineName)
}

/**
 * Fully clean up a pipeline: kill → wait for stopped → clear → wait for cleared → delete.
 * Ignores errors at every step.
 */
export async function cleanupPipeline(pipelineName: string) {
  try {
    await killPipelineAndWaitForStopped(pipelineName)
  } catch (e) {
    if ((e as any).error_code !== 'UnknownPipelineName') {
      console.warn(`cleanupPipeline: kill failed for "${pipelineName}":`, e)
    }
  }
  try {
    await clearAndDeletePipeline(pipelineName)
  } catch (e) {
    if ((e as any).error_code !== 'UnknownPipelineName') {
      console.warn(`cleanupPipeline: clear/delete failed for "${pipelineName}":`, e)
    }
  }
}

const WARMUP_PIPELINE = '__test_warmup__'

/**
 * Warm the Rust compilation cache by creating, compiling, and deleting a
 * minimal pipeline. The first compilation in a fresh Feldera image compiles
 * many Rust crates from scratch, so this prevents individual tests from
 * timing out.
 */
export async function warmCompilationCache() {
  console.log('Warming Rust compilation cache…')

  // Clean up any leftover warmup pipeline
  try {
    await deletePipeline(WARMUP_PIPELINE)
  } catch {}

  try {
    await putPipeline(WARMUP_PIPELINE, {
      name: WARMUP_PIPELINE,
      program_code: 'CREATE TABLE _warmup (id INT);',
      program_config: { profile: 'unoptimized' }
    })
  } catch (e) {
    console.error('warmCompilationCache: putPipeline failed:', e)
    throw e
  }

  await waitForCompilation(WARMUP_PIPELINE)
  console.log('Compilation cache is warm.')

  await deletePipeline(WARMUP_PIPELINE)
}
