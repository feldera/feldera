import { expect, test } from '@playwright/test'
import { putPipeline } from '$lib/services/pipelineManager'
import {
  cleanupPipeline,
  configureTestClient,
  startPipelineAndWaitForRunning,
  waitForCompilation
} from '$lib/services/testPipelineHelpers'

configureTestClient()

const PIPELINE = `test-changestream-${Date.now()}`
// The relation name as keyed in the UI: unquoted SQL identifiers are
// case-independent and normalize to lowercase (see `normalizeCaseIndependentName`).
const VIEW = 'large_transactions'

// A self-contained pipeline modelled on the datagen-driven demos (e.g.
// `demos/sql/02-fraud-detection.sql`): a table fed by a continuous random data
// generator and a view derived from it. The Change Stream is a *live tail* of a
// relation's output changes, so the generator runs continuously (a `rate` with a
// large `limit`) — that way new rows keep arriving after the test subscribes,
// regardless of how much input was already consumed.
const PROGRAM_CODE = `
CREATE TABLE transactions (
    id BIGINT NOT NULL,
    amount DOUBLE
) WITH (
    'connectors' = '[{
      "transport": {
        "name": "datagen",
        "config": {
          "plan": [{
            "limit": 1000,
            "rate": 5,
            "fields": {
              "id": { "range": [0, 1000000] },
              "amount": { "strategy": "uniform", "range": [1, 1000] }
            }
          }]
        }
      }
    }]'
);

CREATE MATERIALIZED VIEW ${VIEW} AS
SELECT
    id,
    amount,
    CAST(id AS INTEGER UNSIGNED) AS id_unsigned,
    TIMESTAMP WITH TIME ZONE '2020-01-01 10:10:10 +00:00' AS event_tz
FROM transactions
WHERE amount > 0;
`

test.describe('Change Stream', () => {
  test.setTimeout(300_000)

  test.beforeAll(async ({}, testInfo) => {
    testInfo.setTimeout(600_000)
    await putPipeline(PIPELINE, {
      name: PIPELINE,
      description: 'E2E smoke test for the Change Stream tab',
      program_code: PROGRAM_CODE,
      program_config: { profile: 'unoptimized' }
    })
    await waitForCompilation(PIPELINE)
  })

  test.afterAll(async () => {
    await cleanupPipeline(PIPELINE)
  })

  test('shows rows for a followed view on a running pipeline', async ({ page }) => {
    await startPipelineAndWaitForRunning(PIPELINE)

    await page.goto(`/pipelines/${PIPELINE}`)

    // Open the Change Stream tab in the monitoring panel (shown by default).
    await page.getByRole('tab', { name: 'Change Stream' }).click()

    // Before any relation is followed, the panel prompts the user to pick one.
    await expect(page.getByText('Select tables and views to see the record updates')).toBeVisible()

    // Follow the view; this opens the egress stream for it.
    await page.getByTestId(`input-changestream-relation-${VIEW}`).check()

    // The generator keeps producing transactions, so change rows must appear.
    await expect(page.getByTestId('box-changestream-row').first()).toBeVisible({ timeout: 30_000 })

    // The section header renders the view name and its column types. This also
    // guards the `displaySQLColumnType` mapping for change-stream headers across
    // the type spellings that differ from the raw wire value:
    //   BIGINT       → "BIGINT"                    (the type that originally crashed)
    //   UINTEGER     → "INTEGER UNSIGNED"
    //   TIMESTAMP_TZ → "TIMESTAMP WITH TIME ZONE"
    await expect(page.getByText(VIEW).first()).toBeVisible()
    await expect(page.getByText('BIGINT', { exact: false }).first()).toBeVisible()
    await expect(page.getByText('INTEGER UNSIGNED', { exact: false }).first()).toBeVisible()
    await expect(page.getByText('TIMESTAMP WITH TIME ZONE', { exact: false }).first()).toBeVisible()
  })
})
