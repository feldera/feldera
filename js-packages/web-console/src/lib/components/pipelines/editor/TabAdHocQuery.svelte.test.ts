/**
 * Integration UI test: ad-hoc query runtime-error propagation over WebSocket.
 *
 * Requires a running (open-source / no-auth) Feldera instance. Run with:
 *   bun run test-integration
 *
 * Creates a real pipeline with a single non-materialized table, starts it, then
 * drives the *real* `TabAdHocQuery` end to end — the query runs against the live
 * pipeline over the WebSocket transport (`adHocQuery`), and the test asserts
 * that an error raised at query *runtime* (not at planning) is rendered in the
 * result, rather than swallowed or surfaced as an internal "schema is undefined"
 * crash.
 *
 * Two runtime errors are exercised:
 *   - `select * from t` against a non-materialized source — an error raised when
 *     execution starts (before any schema), delivered as a WebSocket text frame.
 *   - a divide-by-zero over an inline `VALUES` relation — an error raised
 *     mid-stream, after the schema, while evaluating a row.
 */
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'

// `adHocQuery` derives its WebSocket URL from `felderaEndpoint`, which in a
// browser is computed from `window.location` — here the vitest dev-server
// origin, not the Feldera manager. Point it at the same origin the API client
// uses (configureTestClient) so the WebSocket reaches the running instance.
vi.mock('$lib/functions/configs/felderaEndpoint', () => {
  const env = (import.meta as unknown as { env?: Record<string, string | undefined> }).env ?? {}
  const felderaEndpoint = (
    env.FELDERA_TEST_API_ORIGIN ??
    env.PLAYWRIGHT_API_ORIGIN ??
    'http://localhost:8080'
  ).replace(/\/$/, '')
  return { felderaEndpoint }
})

// `TabAdHocQuery` pulls in editor components that read `$app/state`.
vi.mock('$app/state', () => ({
  page: {
    data: {
      feldera: { version: 'test-runtime', unstableFeatures: [], edition: 'Open Source' }
    },
    url: new URL('http://localhost/')
  }
}))

import { getExtendedPipeline, putPipeline } from '$lib/services/pipelineManager'
import {
  cleanupPipeline,
  configureTestClient,
  type ExtendedPipeline,
  startPipelineAndWaitForRunning,
  waitForCompilation
} from '$lib/services/testPipelineHelpers'
import TabAdHocQuery from './TabAdHocQuery.svelte'

const PIPELINE_NAME = 'test-adhoc-query-errors'

let pipeline: ExtendedPipeline

describe('TabAdHocQuery — runtime error propagation over WebSocket', () => {
  beforeAll(async () => {
    configureTestClient()
    await cleanupPipeline(PIPELINE_NAME)
    await putPipeline(PIPELINE_NAME, {
      name: PIPELINE_NAME,
      // `t` is non-materialized (no `materialized = 'true'`), so selecting from
      // it directly is a runtime error.
      program_code: 'create table t (x int);',
      runtime_config: {}
    })
    await waitForCompilation(PIPELINE_NAME, 120_000)
    await startPipelineAndWaitForRunning(PIPELINE_NAME, 60_000)
    pipeline = await getExtendedPipeline(PIPELINE_NAME)
  }, 180_000)

  afterAll(async () => {
    await cleanupPipeline(PIPELINE_NAME)
  }, 60_000)

  // Render the tab against the running pipeline, type `sql`, and run it.
  const runQuery = async (sql: string) => {
    const { unmount } = render(TabAdHocQuery, {
      pipeline: { current: pipeline },
      deleted: false
    })
    await page.getByRole('textbox').first().fill(sql)
    await page.getByRole('button', { name: 'Run query' }).first().click()
    return unmount
  }

  it('renders the execution error for a non-materialized SELECT', async () => {
    const unmount = await runQuery('select * from t')
    // e.g. "Execution error: Tried to SELECT from a non-materialized source. ..."
    await expect
      .element(page.getByText('Tried to SELECT from a non-materialized source', { exact: false }))
      .toBeInTheDocument()
    unmount()
  }, 60_000)

  it('renders the arrow runtime error for a divide-by-zero', async () => {
    const unmount = await runQuery('SELECT 100 / d AS v FROM (VALUES (5), (2), (0), (1)) AS t(d)')
    // e.g. "Arrow error: Divide by zero error"
    await expect.element(page.getByText('Divide by zero', { exact: false })).toBeInTheDocument()
    unmount()
  }, 60_000)
})
