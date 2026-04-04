/**
 * Integration tests for pipelineManager.ts that require a running Feldera instance.
 * Run via the 'integration' vitest project:
 *
 *   bun run test-integration
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import {
  getInputConnectorStatus,
  getOutputConnectorStatus,
  putPipeline
} from '$lib/services/pipelineManager'
import {
  cleanupPipeline,
  configureTestClient,
  startPipelineAndWaitForRunning,
  waitForCompilation
} from '$lib/services/testPipelineHelpers'

// Pipeline name is URL-safe by convention, but table/view/connector names are not.
const PIPELINE_NAME = 'test-special-chars-connector-status'

// Names with dots and URL-unsafe characters
const TABLE_NAME = 'my.input.table'
const VIEW_NAME = 'my.output.view'
const INPUT_CONNECTOR_NAME = 'http-input_special-name'
const OUTPUT_CONNECTOR_NAME = 'http-output_special-name'

describe('pipelineManager connector status with special characters', () => {
  beforeAll(async () => {
    configureTestClient()

    // Clean up any leftover pipeline from a previous run
    await cleanupPipeline(PIPELINE_NAME)

    // SQL program with quoted identifiers containing dots
    const programCode = `
      CREATE TABLE "${TABLE_NAME}" (id INT NOT NULL, val VARCHAR)
        WITH (
          'connectors' = '[{
            "name": "${INPUT_CONNECTOR_NAME}",
            "transport": { "name": "url_input", "config": { "path": "https://feldera.com/test-data.json" } },
            "format": { "name": "json", "config": { "update_format": "raw" } }
          }]'
        );
      CREATE VIEW "${VIEW_NAME}"
        WITH (
          'connectors' = '[{
            "name": "${OUTPUT_CONNECTOR_NAME}",
            "transport": { "name": "file_output", "config": { "path": "/tmp/feldera-test-output.json" } },
            "format": { "name": "json" }
          }]'
        )
        AS SELECT * FROM "${TABLE_NAME}";
    `

    // Create the pipeline
    await putPipeline(PIPELINE_NAME, {
      name: PIPELINE_NAME,
      description: 'Integration test for special character handling',
      program_code: programCode,
      runtime_config: {}
    })

    await waitForCompilation(PIPELINE_NAME, 120_000)
    await startPipelineAndWaitForRunning(PIPELINE_NAME, 60_000)
  }, 180_000)

  afterAll(async () => {
    await cleanupPipeline(PIPELINE_NAME)
  }, 60_000)

  it('getInputConnectorStatus succeeds with dot and URL-unsafe characters in table and connector name', async () => {
    const body = await getInputConnectorStatus(PIPELINE_NAME, TABLE_NAME, INPUT_CONNECTOR_NAME)
    console.log('body', JSON.stringify(body))
    expect(body).toHaveProperty(['metrics', 'num_parse_errors'])
    expect(body).toHaveProperty(['metrics', 'num_transport_errors'])
  })

  it('getOutputConnectorStatus succeeds with dot and URL-unsafe characters in view and connector name', async () => {
    const body = await getOutputConnectorStatus(PIPELINE_NAME, VIEW_NAME, OUTPUT_CONNECTOR_NAME)
    expect(body).toHaveProperty(['metrics', 'num_encode_errors'])
    expect(body).toHaveProperty(['metrics', 'num_transport_errors'])
  })
})
