/**
 * Lets the log viewer reconnect without downloading the whole log again.
 *
 * The server tells us how far we have read, and we hand that back when we reconnect so it
 * can carry on from there. Without it, a flaky connection can spend all its bandwidth
 * re-downloading old logs and never catch up to what the pipeline is doing now.
 *
 * The server side is `LogsBuffer::resume_from` in
 * `crates/pipeline-manager/src/runner/pipeline_logs.rs`.
 */

/**
 * Response headers naming where the server picked us up. They ride beside the log body
 * rather than inside it, so every line we receive is a log line and we never have to work
 * out which is which. Keep in sync with `LOGS_EPOCH_HEADER` and its neighbours in
 * `crates/pipeline-manager/src/runner/pipeline_logs.rs`.
 */
const epochHeader = 'feldera-logs-epoch'
const seqHeader = 'feldera-logs-seq'
const gapHeader = 'feldera-logs-gap'

/** How far we have read in the server's log stream. */
export type LogCursor = {
  /**
   * Which run of the server's log buffer `seq` counts within. That buffer only lives in
   * memory, so when the server restarts it starts numbering from one again. We compare
   * the epoch so an old count is never applied to a fresh buffer, where it would point at
   * completely different lines.
   */
  epoch: string
  /** How many lines we have received from this epoch. Zero if none yet. */
  seq: number
}

/** Where the server started us from, as reported by a response. */
export type LogResume = LogCursor & {
  /**
   * How many lines the server had already thrown away before the point we asked for.
   * They are gone for good. Zero means we carry on exactly where we left off.
   */
  gap: number
}

/** Turns a cursor into the `cursor` query parameter. Empty string if we have none yet. */
export const formatLogCursor = (cursor: LogCursor | null) =>
  cursor ? `${cursor.epoch}:${cursor.seq}` : ''

/** Reads a header holding a count, or null if it is absent or not one. */
const readCount = (value: string | null) => {
  if (!value) {
    return null
  }
  const count = Number(value)
  return Number.isInteger(count) && count >= 0 ? count : null
}

/**
 * Reads where the server started us from, or returns null if the response does not say.
 *
 * A server that does not know about cursors sends none of these headers and starts sending
 * logs straight away. All three are required: two of them cannot be turned into a cursor,
 * and counting lines from a position we only half know would put us somewhere the server
 * never offered.
 */
export const parseLogResume = (headers: Headers): LogResume | null => {
  const epoch = headers.get(epochHeader)
  const seq = readCount(headers.get(seqHeader))
  const gap = readCount(headers.get(gapHeader))
  return epoch && seq !== null && gap !== null ? { epoch, seq, gap } : null
}

/**
 * Whether the lines we already have run straight into the ones about to arrive.
 *
 * This is only true when we picked up exactly where we left off. If lines went missing,
 * or the server restarted its buffer, or it told us nothing, or we had no cursor to begin
 * with, then there may be a hole between what we have and what comes next. The viewer
 * clears itself in that case, rather than showing two unrelated stretches of log as though
 * they ran together.
 */
export const isExactResume = (requested: LogCursor | null, resumed: LogResume | null) =>
  !!resumed && !!requested && resumed.epoch === requested.epoch && resumed.gap === 0
