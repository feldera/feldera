/**
 * Playwright global setup: warm the Rust compilation cache.
 *
 * The first SQL compilation in a fresh Feldera OSS image compiles many Rust
 * crates from scratch, which can take several minutes. Running this once before
 * the test suite keeps individual test timeouts tight.
 */

import { client } from '$lib/services/manager/client.gen'
import { deletePipeline, getExtendedPipeline, putPipeline } from '$lib/services/pipelineManager'

const API_ORIGIN = (process.env.PLAYWRIGHT_API_ORIGIN ?? 'http://localhost:8080').replace(/\/$/, '')
client.setConfig({ baseUrl: API_ORIGIN })

const WARMUP_PIPELINE = '__e2e_warmup__'

export default async function globalSetup() {
  console.log('Warming Rust compilation cache…')

  // Wait for the API to become reachable (service container may still be starting)
  const healthDeadline = Date.now() + 60_000
  while (Date.now() < healthDeadline) {
    try {
      const res = await fetch(`${API_ORIGIN}/healthz`)
      if (res.ok) {
        break
      }
    } catch {}
    await new Promise((r) => setTimeout(r, 2000))
  }

  // Clean up any leftover warmup pipeline
  try {
    await deletePipeline(WARMUP_PIPELINE)
  } catch {}

  // Create a minimal pipeline
  await putPipeline(WARMUP_PIPELINE, {
    name: WARMUP_PIPELINE,
    program_code: 'CREATE TABLE _warmup (id INT);',
    program_config: { profile: 'unoptimized' }
  })

  // Wait for compilation (up to 10 minutes for a cold cache)
  const deadline = Date.now() + 600_000
  while (Date.now() < deadline) {
    const pipeline = await getExtendedPipeline(WARMUP_PIPELINE)
    if (pipeline.status === 'Stopped') {
      console.log('Compilation cache is warm.')
      break
    }
    const status = pipeline.status
    if (status === 'SqlError' || status === 'RustError' || status === 'SystemError') {
      throw new Error(`Warmup compilation failed: ${status}`)
    }
    await new Promise((r) => setTimeout(r, 2000))
  }

  await deletePipeline(WARMUP_PIPELINE)
}
