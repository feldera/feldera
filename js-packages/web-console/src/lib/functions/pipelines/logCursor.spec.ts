import { describe, expect, it } from 'vitest'
import { formatLogCursor, isExactResume, type LogResume, parseLogResume } from './logCursor'

const EPOCH = '0199c3f1-2d0a-7e84-b711-6f2c9a1d4e08'
const resumed = (over: Partial<LogResume> = {}): LogResume => ({
  epoch: EPOCH,
  seq: 41272,
  gap: 0,
  ...over
})

const headers = (over: Record<string, string | undefined> = {}) =>
  new Headers(
    Object.entries({
      'feldera-logs-epoch': EPOCH,
      'feldera-logs-seq': '41272',
      'feldera-logs-gap': '0',
      ...over
    }).filter((entry): entry is [string, string] => entry[1] !== undefined)
  )

describe('formatLogCursor', () => {
  it('renders a cursor the server can resume from', () => {
    expect(formatLogCursor({ epoch: EPOCH, seq: 41272 })).toBe(`${EPOCH}:41272`)
  })

  it('renders no cursor as an empty string, which still asks where we are', () => {
    expect(formatLogCursor(null)).toBe('')
  })
})

describe('parseLogResume', () => {
  it('reads the epoch, line count and gap', () => {
    expect(parseLogResume(headers())).toEqual(resumed())
  })

  it('reads a response reporting discarded lines', () => {
    expect(
      parseLogResume(headers({ 'feldera-logs-seq': '900', 'feldera-logs-gap': '42' }))
    ).toEqual(resumed({ seq: 900, gap: 42 }))
  })

  // An older server ignores the parameter and starts sending logs straight away. It is
  // also what we see when the headers are not exposed to us across origins.
  it('returns null when the response says nothing about a position', () => {
    expect(parseLogResume(new Headers({ 'content-type': 'text/plain' }))).toBeNull()
  })

  it.each([
    ['a missing epoch', { 'feldera-logs-epoch': undefined }],
    ['an empty epoch', { 'feldera-logs-epoch': '' }],
    ['a missing line count', { 'feldera-logs-seq': undefined }],
    ['an empty line count', { 'feldera-logs-seq': '' }],
    ['a line count that is not a number', { 'feldera-logs-seq': 'soon' }],
    ['a fractional line count', { 'feldera-logs-seq': '1.5' }],
    ['a negative line count', { 'feldera-logs-seq': '-1' }],
    ['a missing gap', { 'feldera-logs-gap': undefined }],
    ['a negative gap', { 'feldera-logs-gap': '-1' }]
  ])('returns null for %s', (_, over) => {
    expect(parseLogResume(headers(over))).toBeNull()
  })
})

describe('isExactResume', () => {
  const requested = { epoch: EPOCH, seq: 41272 }

  it('continues when the server picked up exactly where we asked', () => {
    expect(isExactResume(requested, resumed())).toBe(true)
  })

  // What we have on screen stops where we asked, and the lines that would have joined
  // it to the next one have been thrown away.
  it('starts over when lines were discarded before the resume point', () => {
    expect(isExactResume(requested, resumed({ seq: 41300, gap: 28 }))).toBe(false)
  })

  // A new epoch means the server started a fresh buffer, so our count refers to lines
  // it never had.
  it('starts over when the server restarted its log buffer', () => {
    expect(
      isExactResume(requested, resumed({ epoch: '0199d000-0000-7000-8000-000000000000' }))
    ).toBe(false)
  })

  it('starts over on a first connection, when there is nothing to continue from', () => {
    expect(isExactResume(null, resumed({ seq: 0 }))).toBe(false)
  })

  it('starts over when the server reported no position', () => {
    expect(isExactResume(requested, null)).toBe(false)
  })
})
