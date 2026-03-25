/**
 * Integration tests for pipelineManager.ts that require a running Feldera instance.
 * Run via the 'integration' vitest project:
 *
 *   FELDERA_API_URL=http://localhost:8080 bun run test-integration
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'

const BASE_URL = process.env.FELDERA_API_URL ?? 'http://localhost:8080'

// Pipeline name is URL-safe by convention, but table/view/connector names are not.
const PIPELINE_NAME = 'test-special-chars-connector-status'

// Names with dots and URL-unsafe characters
const TABLE_NAME = 'my.input.table'
const VIEW_NAME = 'my.output.view'
const INPUT_CONNECTOR_NAME = 'http.input/special&name'
const OUTPUT_CONNECTOR_NAME = 'http.output/special&name'

const api = (path: string, init?: RequestInit) =>
  fetch(`${BASE_URL}${path}`, {
    ...init,
    headers: { 'Content-Type': 'application/json', ...init?.headers }
  })

describe('pipelineManager connector status with special characters', () => {
  beforeAll(async () => {
    // Clean up any leftover pipeline from a previous run
    const del = await api(`/v0/pipelines/${PIPELINE_NAME}`, { method: 'DELETE' })
    if (del.ok || del.status === 404) {
      // ok
    } else {
      throw new Error(`Cleanup failed: ${del.status} ${await del.text()}`)
    }

    // SQL program with quoted identifiers containing dots
    const programCode = `
      CREATE TABLE "${TABLE_NAME}" (id INT NOT NULL, val VARCHAR)
        WITH (
          'connectors' = '[{
            "name": "${INPUT_CONNECTOR_NAME}",
            "transport": { "name": "http_input" },
            "format": { "name": "json", "config": { "update_format": "raw" } }
          }]'
        );
      CREATE VIEW "${VIEW_NAME}" AS SELECT * FROM "${TABLE_NAME}"
        WITH (
          'connectors' = '[{
            "name": "${OUTPUT_CONNECTOR_NAME}",
            "transport": { "name": "http_output" },
            "format": { "name": "json", "config": { "update_format": "raw" } }
          }]'
        );
    `

    // Create the pipeline
    const put = await api(`/v0/pipelines/${PIPELINE_NAME}`, {
      method: 'PUT',
      body: JSON.stringify({
        name: PIPELINE_NAME,
        description: 'Integration test for special character handling',
        program_code: programCode,
        runtime_config: {}
      })
    })
    expect(put.ok, `PUT pipeline: ${put.status} ${await put.clone().text()}`).toBe(true)

    // Wait for compilation
    for (let i = 0; i < 120; i++) {
      const res = await api(`/v0/pipelines/${PIPELINE_NAME}`)
      const info = await res.json()
      if (info.program_status === 'Success') break
      if (info.program_status?.SqlError || info.program_status?.RustError || info.program_status?.SystemError) {
        throw new Error(`Compilation failed: ${JSON.stringify(info.program_status)}`)
      }
      await new Promise((r) => setTimeout(r, 1000))
    }

    // Start the pipeline
    const start = await api(`/v0/pipelines/${PIPELINE_NAME}/start`, { method: 'POST' })
    expect(start.ok, `POST start: ${start.status} ${await start.clone().text()}`).toBe(true)

    // Wait for pipeline to be running
    for (let i = 0; i < 60; i++) {
      const res = await api(`/v0/pipelines/${PIPELINE_NAME}`)
      const info = await res.json()
      if (info.deployment_status === 'Running') break
      if (info.deployment_error) {
        throw new Error(`Deployment failed: ${JSON.stringify(info.deployment_error)}`)
      }
      await new Promise((r) => setTimeout(r, 1000))
    }
  }, 180_000)

  afterAll(async () => {
    // Shutdown and delete
    await api(`/v0/pipelines/${PIPELINE_NAME}/shutdown`, { method: 'POST' })
    // Wait for shutdown
    for (let i = 0; i < 30; i++) {
      const res = await api(`/v0/pipelines/${PIPELINE_NAME}`)
      const info = await res.json()
      if (info.deployment_status === 'Shutdown') break
      await new Promise((r) => setTimeout(r, 1000))
    }
    await api(`/v0/pipelines/${PIPELINE_NAME}`, { method: 'DELETE' })
  }, 60_000)

  it('getInputConnectorStatus succeeds with dot and URL-unsafe characters in table and connector name', async () => {
    const url = `/v0/pipelines/${PIPELINE_NAME}/tables/${encodeURIComponent(TABLE_NAME)}/connectors/${encodeURIComponent(INPUT_CONNECTOR_NAME)}/status`
    const res = await api(url)
    expect(res.ok, `GET input connector status: ${res.status} ${await res.clone().text()}`).toBe(true)
    const body = await res.json()
    expect(body).toHaveProperty('num_parse_errors')
    expect(body).toHaveProperty('num_transport_errors')
  })

  it('getOutputConnectorStatus succeeds with dot and URL-unsafe characters in view and connector name', async () => {
    const url = `/v0/pipelines/${PIPELINE_NAME}/views/${encodeURIComponent(VIEW_NAME)}/connectors/${encodeURIComponent(OUTPUT_CONNECTOR_NAME)}/status`
    const res = await api(url)
    expect(res.ok, `GET output connector status: ${res.status} ${await res.clone().text()}`).toBe(true)
    const body = await res.json()
    expect(body).toHaveProperty('num_encode_errors')
    expect(body).toHaveProperty('num_transport_errors')
  })
})
