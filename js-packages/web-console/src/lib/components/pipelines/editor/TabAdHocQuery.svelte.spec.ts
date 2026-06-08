/**
 * UI integration test for the ad-hoc query tab.
 *
 * This drives the *real* `TabAdHocQuery.svelte` end to end: the only thing
 * mocked is `usePipelineManager().adHocQuery`, which returns a hard-coded Arrow
 * IPC byte stream — exactly the `format=arrow_ipc` response body the pipeline
 * manager sends for `/v0/pipelines/{name}/query`. Everything downstream is the
 * production code path: `RecordBatchReader` → `arrowIpcBatchToJS` /
 * `arrowSchemaToFelderaFields` → `Query.svelte` → `SQLValue.svelte` /
 * `SqlColumnHeader.svelte`.
 *
 * The fixture covers one row spanning every Feldera SQL type the web console
 * renders, using the exact Arrow types the server emits (e.g. `Time64(ns)`,
 * `Timestamp(µs)`, `Decimal128(38, 10)`, the full set of signed/unsigned
 * integer widths, `List`, `Struct`, `Map`). The test asserts the full result
 * header (column name + rendered SQL type) and every decoded cell value.
 *
 * To regenerate ARROW_IPC_BASE64, build an arrow Table with the same columns
 * and `Buffer.from(RecordBatchStreamWriter.writeAll(table).toUint8Array(true))
 * .toString('base64')`.
 */
import { describe, expect, it, vi } from 'vitest'
import { page } from 'vitest/browser'
import { render } from 'vitest-browser-svelte'
import type { ExtendedPipeline } from '$lib/services/pipelineManager'

// One row, every rendered type, in the exact Arrow encoding the server emits.
const ARROW_IPC_BASE64 =
  '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CAAAAAAAAABoAAAAAAAAAAAAAAAAAAAAaAAAAAAAAAAIAAAAAAAAAHAAAAAAAAAAAAAAAAAAAABwAAAAAAAAAAgAAAAAAAAAeAAAAAAAAAAAAAAAAAAAAHgAAAAAAAAACAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAQAAAAAAAAAJAAAAAAAAAAAAAAAAAAAACQAAAAAAAAAAgAAAAAAAAAmAAAAAAAAAAAAAAAAAAAAJgAAAAAAAAACAAAAAAAAACgAAAAAAAAAAAAAAAAAAAAoAAAAAAAAAAIAAAAAAAAAKgAAAAAAAAAEAAAAAAAAAC4AAAAAAAAAAAAAAAAAAAAuAAAAAAAAAAIAAAAAAAAAMAAAAAAAAAACAAAAAAAAADIAAAAAAAAAAAAAAAAAAAAyAAAAAAAAAAIAAAAAAAAANAAAAAAAAAAAAAAAAAAAADQAAAAAAAAAAgAAAAAAAAA2AAAAAAAAAAAAAAAAAAAANgAAAAAAAAACAAAAAAAAADgAAAAAAAAAAAAAAAAAAAA4AAAAAAAAAAIAAAAAAAAAOgAAAAAAAAAAAAAAAAAAADoAAAAAAAAABAAAAAAAAAA+AAAAAAAAAAAAAAAAAAAAPgAAAAAAAAAAAAAAAAAAAD4AAAAAAAAAAgAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAACAAAAAAAAAAIAQAAAAAAAAgAAAAAAAAAEAEAAAAAAAAAAAAAAAAAABABAAAAAAAACAAAAAAAAAAYAQAAAAAAAAAAAAAAAAAAGAEAAAAAAAAAAAAAAAAAABgBAAAAAAAACAAAAAAAAAAgAQAAAAAAAAgAAAAAAAAAKAEAAAAAAAAAAAAAAAAAACgBAAAAAAAACAAAAAAAAAAAAAAAGgAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAPQAAAAAAAAALvsAAAAAAADAHf7/AAAAAP////////9/+gAAAAAAAABg6gAAAAAAAAAoa+4AAAAA//////////9O8zi+kXp5bes1/QMAAAAAAABgQAAAAAAAAAAAAAAEQAAAAAAKAAAAaGVsbG8g8J+OiQAAAAAAAAAAAAAEAAAA3q2+7wAAAABAjwR7MikAABlNAAAAAAAACIbOPfsOBgAAAAAAAwAAAAEAAAACAAAAAwAAAAAAAAAHAAAAAAAAAAAAAAAFAAAAaW5uZXIAAAAAAAAAAQAAAAAAAAAFAAAAYWxwaGEAAAABAAAAAAAAAP////8AAAAA'

// Decode base64 to bytes (atob is available in the browser test environment).
const base64ToBytes = (b64: string): Uint8Array =>
  Uint8Array.from(atob(b64), (c) => c.charCodeAt(0))

const oneShotStream = (bytes: Uint8Array): ReadableStream<Uint8Array> =>
  new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(bytes)
      controller.close()
    }
  })

// --- Mock only the network: adHocQuery returns the fixture as a stream ---
const adHocQueryMock = vi.fn(async () => ({
  stream: oneShotStream(base64ToBytes(ARROW_IPC_BASE64)),
  cancel: () => {},
  error: (): Error | undefined => undefined
}))

vi.mock('$lib/compositions/usePipelineManager.svelte', () => ({
  usePipelineManager: () => ({ adHocQuery: adHocQueryMock })
}))

// Imported AFTER vi.mock so the mock takes effect.
import TabAdHocQuery from './TabAdHocQuery.svelte'

const pipelineProp = (name: string): { current: ExtendedPipeline } => ({
  // `Running` makes the tab interactive so the Run button is enabled.
  current: { name, status: 'Running' } as ExtendedPipeline
})

// Collapse template whitespace so header cells read "name TYPE".
const normalize = (s: string) => s.replace(/\s+/g, ' ').trim()

describe('TabAdHocQuery.svelte — arrow_ipc result rendering', () => {
  it('decodes a hard-coded arrow_ipc response and renders the full header and values', async () => {
    render(TabAdHocQuery, { pipeline: pipelineProp('p-adhoc-arrow') })

    // Type a query and run it; the mock ignores the SQL and returns the fixture.
    await page.getByRole('textbox').first().fill('SELECT * FROM test_types')
    await page.getByRole('button', { name: 'Run query' }).first().click()

    // Wait for the decoded row to land (the BIGINT cell is unique).
    await expect.element(page.getByText('9223372036854775807')).toBeInTheDocument()
    expect(adHocQueryMock).toHaveBeenCalled()

    // --- Result header: column name + rendered SQL type (incl. the "#" index column) ---
    const headerTexts = Array.from(document.querySelectorAll('thead th')).map((th) =>
      normalize(th.textContent ?? '')
    )
    expect(headerTexts).toEqual([
      '#',
      'c_boolean BOOLEAN',
      'c_tinyint TINYINT',
      'c_smallint SMALLINT',
      'c_integer INTEGER',
      'c_bigint BIGINT',
      'c_utinyint TINYINT UNSIGNED',
      'c_usmallint SMALLINT UNSIGNED',
      'c_uint INTEGER UNSIGNED',
      'c_ubigint BIGINT UNSIGNED',
      'c_decimal DECIMAL(38, 10)',
      'c_real REAL',
      'c_double DOUBLE PRECISION',
      'c_varchar VARCHAR',
      'c_binary VARBINARY',
      'c_time TIME',
      'c_date DATE',
      'c_timestamp TIMESTAMP',
      'c_int_array INTEGER ARRAY',
      'c_row ROW',
      'c_map MAP'
    ])

    // --- Cell values, in column order (the leading "0" is the row-index column) ---
    const cellTexts = Array.from(document.querySelectorAll('tbody tr td')).map(
      (td) => td.textContent ?? ''
    )
    expect(cellTexts).toEqual([
      '0',
      'true',
      '-12',
      '-1234',
      '-123456',
      '9223372036854775807',
      '250',
      '60000',
      '4000000000',
      '18446744073709551615',
      // Decimal is scaled and previewed to 3 fractional digits by SQLValue.
      '123456789012345678.901',
      '3.5',
      '2.5',
      'hello 🎉',
      // Binary renders as a lowercase hex string (0xde 0xad 0xbe 0xef).
      'deadbeef',
      // Time64(ns) → time-of-day as a Dayjs ISO string (epoch day).
      '"1970-01-01T12:34:56.789Z"',
      '"2024-01-15T00:00:00.000Z"',
      '"2024-01-15T12:34:56.789Z"',
      '[\n 1,\n 2,\n 3\n]',
      '{\n "a": 7,\n "b": "inner"\n}',
      '{\n "alpha": 1\n}'
    ])
  })

  it('renders a query error reported out of band instead of crashing on a missing schema', async () => {
    // A query that fails before producing any rows (e.g. selecting from a
    // non-materialized source) closes the stream cleanly with no arrow schema;
    // the message arrives via `error()`. Regression test: the UI must show the
    // message, not "can't access property fields, schema is undefined".
    const message = 'Execution error: Tried to SELECT from a non-materialized source'
    adHocQueryMock.mockResolvedValueOnce({
      stream: new ReadableStream<Uint8Array>({
        start(controller) {
          controller.close()
        }
      }),
      cancel: () => {},
      error: () => new Error(message)
    })

    render(TabAdHocQuery, { pipeline: pipelineProp('p-adhoc-arrow-error') })
    await page.getByRole('textbox').first().fill('SELECT * FROM not_materialized')
    await page.getByRole('button', { name: 'Run query' }).first().click()

    await expect.element(page.getByText(message)).toBeInTheDocument()
  })
})
