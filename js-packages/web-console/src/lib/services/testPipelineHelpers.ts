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
  getExtendedPipeline,
  getPipelineStatus,
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
  const origin = (
    process.env.FELDERA_TEST_API_ORIGIN ??
    process.env.PLAYWRIGHT_API_ORIGIN ??
    'http://localhost:8080'
  ).replace(/\/$/, '')
  client.setConfig({ baseUrl: origin })
  return origin
}

/**
 * Poll `getExtendedPipeline` until `predicate` returns true or `timeoutMs` elapses.
 */
export async function waitForExtendedPipeline(
  pipelineName: string,
  predicate: (p: ExtendedPipeline) => boolean,
  timeoutMs = 120_000
): Promise<ExtendedPipeline> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const pipeline = await getExtendedPipeline(pipelineName)
    if (predicate(pipeline)) return pipeline
    await new Promise((r) => setTimeout(r, 2000))
  }
  const pipeline = await getExtendedPipeline(pipelineName)
  throw new Error(
    `waitForExtendedPipeline timed out for "${pipelineName}". ` +
      `deploy=${pipeline.deploymentStatus} status=${JSON.stringify(pipeline.status)} storage=${pipeline.storageStatus}`
  )
}

/**
 * Poll `getPipelineStatus` until `predicate` returns true or `timeoutMs` elapses.
 */
export async function waitForPipelineStatus(
  pipelineName: string,
  predicate: (status: string) => boolean,
  timeoutMs = 120_000
) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const { status } = await getPipelineStatus(pipelineName)
    if (predicate(status as string)) return status
    await new Promise((r) => setTimeout(r, 1000))
  }
  const { status } = await getPipelineStatus(pipelineName)
  throw new Error(
    `waitForPipelineStatus timed out for "${pipelineName}". status=${JSON.stringify(status)}`
  )
}

/**
 * Wait for a pipeline's SQL/Rust compilation to finish successfully.
 * Throws on compilation errors.
 */
export async function waitForCompilation(pipelineName: string, timeoutMs = 600_000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const { status } = await getPipelineStatus(pipelineName)
    const programStatus = programStatusOf(status)
    if (programStatus === 'Success') return
    if (
      programStatus === 'SqlError' ||
      programStatus === 'RustError' ||
      programStatus === 'SystemError'
    ) {
      throw new Error(`Compilation failed for "${pipelineName}": ${programStatus}`)
    }
    await new Promise((r) => setTimeout(r, 2000))
  }
  throw new Error(`Compilation timed out for "${pipelineName}"`)
}

/**
 * Start a pipeline and wait until it reaches the Running state.
 * Automatically handles the AwaitingApproval → approve_changes transition.
 */
export async function startPipelineAndWaitForRunning(pipelineName: string, timeoutMs = 60_000) {
  await postPipelineAction(pipelineName, 'start')
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const { status } = await getPipelineStatus(pipelineName)
    if (status === 'Running') return
    if (status === 'AwaitingApproval') {
      await postPipelineAction(pipelineName, 'approve_changes')
    }
    await new Promise((r) => setTimeout(r, 1000))
  }
  throw new Error(`Pipeline "${pipelineName}" did not reach Running within ${timeoutMs}ms`)
}

/**
 * Stop a pipeline and wait until it reaches the Stopped state.
 */
export async function stopPipelineAndWaitForStopped(pipelineName: string, timeoutMs = 30_000) {
  try {
    await postPipelineAction(pipelineName, 'stop')
  } catch {
    // Ignore if already stopped
  }
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const { status } = await getPipelineStatus(pipelineName)
    if (status === 'Stopped') return
    await new Promise((r) => setTimeout(r, 1000))
  }
  throw new Error(`Pipeline "${pipelineName}" did not reach Stopped within ${timeoutMs}ms`)
}

/**
 * Fully clean up a pipeline: stop → wait → delete. Ignores errors at every step.
 */
export async function cleanupPipeline(pipelineName: string) {
  try {
    await stopPipelineAndWaitForStopped(pipelineName)
  } catch {}
  try {
    await deletePipeline(pipelineName)
  } catch {}
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
