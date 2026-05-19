import { type JSONParserOptions, Tokenizer, TokenParser } from '@streamparser/json'
import { BigNumber } from 'bignumber.js'
import invariant from 'tiny-invariant'
import type { ChangeStreamData, Row } from '$lib/components/pipelines/editor/ChangeStream.svelte'
import { findIndex } from '$lib/functions/common/array'
import { tuple } from '$lib/functions/common/tuple'
import type { XgressEntry } from '$lib/services/pipelineManager'

class BigNumberTokenizer extends Tokenizer {
  parseNumber = BigNumber as any
}

export interface StreamingJsonParser<T> {
  /** Feed text into the parser. Returns the values it produced from this input. */
  write(input: string): T[]
  /** Drop internal state — call after `write` throws so the next call starts clean. */
  reset(): void
}

/**
 * Long-lived JSON parser that preserves number precision via `BigNumber` (so SQL
 * DECIMAL / large-integer columns aren't coerced through JS `Number`). Designed to be
 * allocated **once per stream** and fed multi-line blocks: the underlying
 * `@streamparser/json` tokenizer + token parser handle a series of concatenated
 * documents in a single `write()` call (`separator: ''` mode).
 */
export const createBigNumberStreamParser = <T>(
  opts?: JSONParserOptions
): StreamingJsonParser<T> => {
  let buffer: T[] = []
  let tokenizer!: Tokenizer
  let tokenParser!: TokenParser
  const init = () => {
    tokenizer = new BigNumberTokenizer()
    tokenParser = new TokenParser(opts)
    tokenizer.onToken = tokenParser.write.bind(tokenParser)
    tokenParser.onValue = (v) => {
      buffer.push(v.value as T)
    }
  }
  init()
  return {
    write(input: string): T[] {
      tokenizer.write(input)
      const out = buffer
      buffer = []
      return out
    },
    reset() {
      buffer = []
      init()
    }
  }
}

/**
 * Soft cap on how many bytes a single not-yet-newline-terminated record may accumulate.
 * Anything beyond this is reported as skipped — accepting unbounded leftovers would let
 * a runaway record exhaust browser memory.
 */
const MAX_LINE_SIZE = 16 * 1024 * 1024

/**
 * Soft target for the size of each `parser.write` call. The decoder slices the
 * accumulated buffer at the nearest newline below this size and yields between
 * parser chunks, so a single big network chunk can't monopolize the main thread.
 */
const PARSER_CHUNK_TARGET_BYTES = 16 * 1024

/**
 * Yields control back to the event loop so the UI can render and respond to input.
 *
 * Uses `scheduler.yield()` when available (Chrome 129+) to keep the task's priority,
 * otherwise a `MessageChannel` round-trip — both yield in the same macrotask boundary
 * that `setTimeout(0)` would, but without the 4ms minimum browsers clamp setTimeout to
 * after a few nested calls. That clamp caps throughput far below what overflow
 * shedding requires.
 */
const yieldToEventLoop = (): Promise<void> => {
  const sched = (globalThis as { scheduler?: { yield?: () => Promise<void> } }).scheduler
  if (sched && typeof sched.yield === 'function') {
    return sched.yield()
  }
  if (typeof MessageChannel !== 'undefined') {
    return new Promise<void>((resolve) => {
      const channel = new MessageChannel()
      channel.port1.onmessage = () => {
        channel.port1.close()
        resolve()
      }
      channel.port2.postMessage(0)
    })
  }
  return new Promise((resolve) => setTimeout(resolve))
}

/**
 * Pluggable byte-stream decoder. The orchestrator (`parseStream`) owns the
 * lifecycle (reader scheduling, flush cadence, cancel, end-of-stream); the
 * decoder is responsible only for turning bytes into typed items.
 *
 * Decoders are stateful and **single-use**: each call to `decode` allocates a
 * fresh reader, accumulator, and (if shedding) per-window budget. To enforce
 * that statically, decoders are obtained via a `StreamFormatDecoderFactory`
 * rather than passed directly — the orchestrator calls the factory exactly
 * once. Each decoder also owns its own shedding policy and skip-event
 * reporting: NDJSON keeps a per-flush-window byte budget and reports
 * `onBytesSkipped`; arrow IPC counts admitted rows and reports `onRowsSkipped`.
 */
export interface StreamFormatDecoder<T> {
  /**
   * Drive the read loop. Must call `ctx.admit(item)` for each parsed item and
   * `ctx.nextTick()` at safe yield points so the flush timer keeps firing.
   * Must return when the source is exhausted or `ctx.isCancelled()` becomes
   * true. Thrown errors are routed through `cbs.onDecodeError`.
   */
  decode(ctx: DecodeContext<T>): Promise<void>
  /**
   * Release any reader-side state. Called on consumer cancel before the
   * pipeline aborts the underlying source. Async errors are swallowed.
   */
  cancel?(): void | Promise<void>
}

/** Single-use factory that produces a fresh, configured decoder per call. */
export type StreamFormatDecoderFactory<T> = () => StreamFormatDecoder<T>

export interface DecodeContext<T> {
  /** Raw source — the decoder may call `getReader()` or hand it to a library. */
  source: ReadableStream<Uint8Array<ArrayBufferLike>>
  /** Queue an item for the orchestrator to flush on its own UI cadence. */
  admit: (value: T) => void
  /** Cooperative cancel flag — decoders should poll this and return promptly. */
  isCancelled: () => boolean
  /** Yield to a macrotask so timers (and the rest of the page) keep ticking. */
  nextTick: () => Promise<void>
}

/**
 * Format-agnostic stream orchestrator. Drives one sequential reader loop and
 * one `setInterval` flush — the abstraction is in the decoder boundary, not
 * the runtime topology.
 *
 * Two cadences are at play, and they are intentionally independent:
 *
 *   - **Parse cadence** — set by the decoder. The decoder yields between
 *     parser chunks (so a single big read can't monopolize the main thread)
 *     and sheds load on its own window when the input arrives faster than
 *     the parser can keep up. None of this is visible at this layer.
 *   - **UI flush cadence** — `flushIntervalMs`. How often `pushChanges` fires
 *     for the consumer. Tuned for human perception (rendering is cheap
 *     relative to parsing, so the flush rate can be set purely for how
 *     "smooth" updates should feel), not for keeping parsing alive.
 *
 * Why not a `TransformStream` pipe? Web-streams transforms run back-to-back
 * via microtasks; without explicit macrotask hops between chunks, the flush
 * timer (and every other `setTimeout`/`setInterval` on the page) starves
 * behind the microtask queue and the UI appears to update only every few
 * seconds on busy streams. Owning the loop ourselves keeps the cadence
 * predictable.
 *
 * @param decoderFactory Called once per `parseStream` invocation to obtain a
 *   fresh, single-use decoder. Reusing a configured factory across multiple
 *   `parseStream` calls is safe — each call gets its own decoder.
 * @param options.flushIntervalMs UI flush cadence: how often `pushChanges`
 *   fires for the consumer. Lower → smoother UI updates, more frequent DOM
 *   work. Default 100ms. Does **not** affect parsing throughput or
 *   shedding — those are configured per decoder.
 */
export const parseStream = <T>(
  source: {
    stream: ReadableStream<Uint8Array<ArrayBufferLike>>
    cancel: () => void
  },
  decoderFactory: StreamFormatDecoderFactory<T>,
  cbs: {
    pushChanges: (changes: T[]) => void
    onParseEnded?: (reason: 'ended' | 'cancelled') => void
    onDecodeError?: (e: Error) => void
  },
  options?: { flushIntervalMs?: number }
): { cancel: () => void } => {
  invariant(
    source.stream instanceof ReadableStream,
    `parseStream(): stream is ${JSON.stringify(source.stream)}`
  )
  const flushIntervalMs = options?.flushIntervalMs ?? 100
  const decoder = decoderFactory()

  let cancelled = false
  let closedReason: null | 'ended' | 'cancelled' = null
  let endedReported = false

  const outBuffer: T[] = []
  const admit = (v: T) => {
    outBuffer.push(v)
  }

  const ctx: DecodeContext<T> = {
    source: source.stream,
    admit,
    isCancelled: () => cancelled,
    nextTick: yieldToEventLoop
  }

  setTimeout(async () => {
    try {
      await decoder.decode(ctx)
    } catch (e) {
      cbs.onDecodeError?.(e instanceof Error ? e : new Error(String(e)))
    }
    closedReason ??= 'ended'
  })

  const flushHandle = setInterval(() => {
    if (outBuffer.length) {
      const batch = outBuffer.slice()
      outBuffer.length = 0
      cbs.pushChanges(batch)
    }
    if (closedReason && !endedReported) {
      endedReported = true
      clearInterval(flushHandle)
      cbs.onParseEnded?.(closedReason)
    }
  }, flushIntervalMs)

  return {
    cancel: () => {
      cancelled = true
      closedReason = 'cancelled'
      Promise.resolve(decoder.cancel?.())
        .catch(() => {})
        .finally(() => {
          try {
            source.cancel()
          } catch {
            /* source.cancel may be a no-op or already cancelled */
          }
        })
    }
  }
}

/**
 * Decoder for newline-delimited JSON. Two granularities are at play here:
 *
 *   - **Network chunk:** whatever bytes one `reader.read()` call returns.
 *     Sized by the network, unpredictable, can be very large.
 *   - **Parser chunk:** a line-aligned slice the decoder hands to
 *     `parser.write` — bounded by `PARSER_CHUNK_TARGET_BYTES` so a single
 *     synchronous parse can't monopolize the main thread.
 *
 * After each read, the accumulated buffer (prior leftover + the new network
 * chunk) is sliced into parser chunks; whatever sits past the last newline
 * used becomes the leftover for the next read. The parser is fed each parser
 * chunk via `parser.write` (multi-document mode); on a throw, `parser.reset()`
 * clears its partial state and the parser chunk is reported as skipped — the
 * next parser chunk starts on a clean parser. Because we never feed the
 * parser anything that isn't a complete document, structurally invalid JSON
 * corruption cannot occur.
 *
 * **Load shedding.** Independent of UI flush cadence. The decoder caps the
 * bytes it admits to the orchestrator to `bufferSize` per `bufferWindowMs`;
 * anything past the cap is dropped (and reported via `onBytesSkipped`)
 * instead of parsed. The budget resets on the decoder's own `setInterval`
 * so even parser chunks that produced no admitted items still release their
 * reservation. Failed parses do **not** consume the budget. In steady state,
 * worst-case parser-to-admit latency is roughly `bufferSize / parse_rate`.
 *
 *  - **Too many skipped records?** Raise `bufferSize` (or lengthen
 *    `bufferWindowMs`). More bytes are admitted before dropping kicks in,
 *    but visible data lags further behind real time.
 *  - **Parse spikes feel heavy?** Lower `bufferSize`. The window's CPU work
 *    is capped sooner and the excess is shed.
 *
 * @param parser Streaming JSON parser; the factory uses it as-is, so caller
 *   must ensure that any single parser instance is not shared across parallel
 *   parseStream invocations (its tokenizer state cannot be safely interleaved).
 * @param opts.bufferSize Bytes admitted per `bufferWindowMs`; default 1 MB.
 * @param opts.bufferWindowMs Budget reset period; default 100ms. Independent
 *   of the orchestrator's UI flush cadence.
 * @param opts.onBytesSkipped Called when a parser chunk is dropped.
 */
export const newlineJsonDecoder = <T>(
  parser: StreamingJsonParser<T>,
  opts?: {
    bufferSize?: number
    bufferWindowMs?: number
    onBytesSkipped?: (bytes: number) => void
  }
): StreamFormatDecoderFactory<T> => {
  const maxPendingBytes = opts?.bufferSize ?? 1_000_000
  const bufferWindowMs = opts?.bufferWindowMs ?? 100
  const onBytesSkipped = opts?.onBytesSkipped ?? (() => {})

  return () => {
    let pendingBytes = 0
    let reader: ReadableStreamDefaultReader<Uint8Array<ArrayBufferLike>> | null = null
    let budgetTimer: ReturnType<typeof setInterval> | null = null

    return {
      async cancel() {
        try {
          await reader?.cancel()
        } catch {
          /* reader may already be closed */
        }
      },
      async decode(ctx) {
        // `pendingBytes` represents "CPU work admitted since the last window",
        // not "bytes currently in outBuffer", so it must be released on a
        // fixed cadence — even when no items were produced (heartbeat lines
        // with no `json_data`, path-filtered lines, etc.). Resetting only
        // alongside successful admits would let a stream of no-output parser
        // chunks silently exhaust the cap and stall the reader.
        budgetTimer = setInterval(() => {
          pendingBytes = 0
        }, bufferWindowMs)
        try {
          reader = ctx.source.getReader()
          const textDecoder = new TextDecoder('utf-8')
          let leftover = ''
          while (!ctx.isCancelled()) {
            const networkChunk = await reader.read()
            if (networkChunk.done) {
              return
            }
            const text = leftover + textDecoder.decode(networkChunk.value, { stream: true })

            // One newline search per parser chunk, none per network chunk. For
            // each parser chunk, snap the cut back to the last newline below
            // the target; if none exists (a single record longer than
            // `PARSER_CHUNK_TARGET_BYTES`), search forward to the next newline
            // so the parser chunk is still a complete document; if no newline
            // is found at all, stop and hold the rest as leftover for the next
            // read. The pending-bytes check is per parser chunk: dropping at
            // network-chunk granularity would shed a single network chunk
            // larger than `bufferSize` wholesale even though the flush timer
            // resets `pendingBytes` between yields and most parser chunks
            // would fit individually.
            let cursor = 0
            while (cursor < text.length) {
              const target = Math.min(cursor + PARSER_CHUNK_TARGET_BYTES, text.length)
              let nl = text.lastIndexOf('\n', target - 1)
              if (nl < cursor) {
                nl = text.indexOf('\n', target)
                if (nl === -1) {
                  break
                }
              }
              const end = nl + 1
              const parserChunkLen = end - cursor
              if (pendingBytes + parserChunkLen > maxPendingBytes) {
                onBytesSkipped(parserChunkLen)
              } else {
                const parserChunk = text.slice(cursor, end)
                try {
                  const items = parser.write(parserChunk)
                  for (const item of items) {
                    ctx.admit(item)
                  }
                  pendingBytes += parserChunkLen
                } catch {
                  parser.reset()
                  onBytesSkipped(parserChunkLen)
                }
              }
              cursor = end
              await ctx.nextTick()
              if (ctx.isCancelled()) {
                return
              }
            }
            leftover = text.slice(cursor)
            // Cap the leftover: accepting unbounded leftovers would let a
            // runaway record without any terminator exhaust browser memory.
            if (leftover.length > MAX_LINE_SIZE) {
              onBytesSkipped(leftover.length)
              leftover = ''
            }
          }
        } finally {
          if (budgetTimer !== null) {
            clearInterval(budgetTimer)
            budgetTimer = null
          }
        }
      }
    }
  }
}

/**
 * Decoder for newline-delimited UTF-8 text. Same line-aligned chunking and
 * per-window byte budget as `newlineJsonDecoder`, but each parser chunk is
 * split into its constituent lines and admitted one string at a time (with
 * the trailing `\n` or `\r\n` preserved on each line). Suitable for log
 * viewers and other "byte stream → string[]" consumers.
 *
 * Shedding granularity is the parser chunk, not the individual line: when the
 * budget is exhausted, the whole parser chunk's worth of lines is dropped and
 * its byte count is reported via `onBytesSkipped`. Dropping at line
 * granularity would be possible but produces visually scattered gaps; the
 * parser-chunk strategy keeps dropped regions contiguous.
 *
 * @param opts.bufferSize Bytes admitted per `bufferWindowMs`; default 1 MB.
 * @param opts.bufferWindowMs Budget reset period; default 100ms. Independent
 *   of the orchestrator's UI flush cadence.
 * @param opts.onBytesSkipped Called when a parser chunk is dropped, or when
 *   a runaway record (no newline within `MAX_LINE_SIZE` bytes) is purged.
 */
export const newlineTextDecoder = (opts?: {
  bufferSize?: number
  bufferWindowMs?: number
  onBytesSkipped?: (bytes: number) => void
}): StreamFormatDecoderFactory<string> => {
  const maxPendingBytes = opts?.bufferSize ?? 1_000_000
  const bufferWindowMs = opts?.bufferWindowMs ?? 100
  const onBytesSkipped = opts?.onBytesSkipped ?? (() => {})

  return () => {
    let pendingBytes = 0
    let reader: ReadableStreamDefaultReader<Uint8Array<ArrayBufferLike>> | null = null
    let budgetTimer: ReturnType<typeof setInterval> | null = null

    return {
      async cancel() {
        try {
          await reader?.cancel()
        } catch {
          /* reader may already be closed */
        }
      },
      async decode(ctx) {
        budgetTimer = setInterval(() => {
          pendingBytes = 0
        }, bufferWindowMs)
        try {
          reader = ctx.source.getReader()
          const textDecoder = new TextDecoder('utf-8')
          let leftover = ''
          while (!ctx.isCancelled()) {
            const networkChunk = await reader.read()
            if (networkChunk.done) {
              return
            }
            const text = leftover + textDecoder.decode(networkChunk.value, { stream: true })

            // Forward-scan once, collecting line endings, until we've covered
            // ~PARSER_CHUNK_TARGET_BYTES; that span is one parser chunk. Each
            // byte is examined exactly once: the same scan that finds line
            // boundaries for admit also finds the parser-chunk boundary used
            // by the budget check.
            let cursor = 0
            while (cursor < text.length) {
              const target = cursor + PARSER_CHUNK_TARGET_BYTES
              const lineEnds: number[] = []
              let scan = cursor
              while (scan < text.length) {
                const nl = text.indexOf('\n', scan)
                if (nl === -1) {
                  break
                }
                lineEnds.push(nl + 1)
                scan = nl + 1
                if (scan >= target) {
                  break
                }
              }
              if (lineEnds.length === 0) {
                // No complete line in the remainder — stop and hold the rest
                // as leftover for the next read.
                break
              }
              const parserChunkEnd = lineEnds[lineEnds.length - 1]
              const parserChunkLen = parserChunkEnd - cursor
              if (pendingBytes + parserChunkLen > maxPendingBytes) {
                onBytesSkipped(parserChunkLen)
              } else {
                let lineStart = cursor
                for (const lineEnd of lineEnds) {
                  ctx.admit(text.slice(lineStart, lineEnd))
                  lineStart = lineEnd
                }
                pendingBytes += parserChunkLen
              }
              cursor = parserChunkEnd
              await ctx.nextTick()
              if (ctx.isCancelled()) {
                return
              }
            }
            leftover = text.slice(cursor)
            if (leftover.length > MAX_LINE_SIZE) {
              onBytesSkipped(leftover.length)
              leftover = ''
            }
          }
        } finally {
          if (budgetTimer !== null) {
            clearInterval(budgetTimer)
            budgetTimer = null
          }
        }
      }
    }
  }
}

/**
 *
 * @returns Index offset of items in the original list after push (positive number)
 */
export const pushAsCircularBuffer =
  <T, R = T>(
    arr: () => R[],
    bufferSize: number,
    mapValue: (v: T) => R,
    getLength?: (value: R) => number
  ) =>
  (values: T[]) => {
    if (!getLength) {
      const offset = arr().length + values.length - bufferSize
      arr().splice(0, offset)
      arr().push(...values.slice(-bufferSize).map(mapValue))
      return Math.max(offset, 0)
    }
    const vs = values.map(mapValue)
    let numNewItems = findIndex(
      vs,
      (acc: number, item: R) => {
        const sum = acc + getLength(item)
        return tuple(sum > 0, sum)
      },
      -bufferSize
    )
    numNewItems = numNewItems === -1 ? vs.length : numNewItems
    const numItemsToDrop = findIndex(
      arr(),
      (acc: number, item: R) => {
        const sum = acc + getLength(item)
        return tuple(sum >= 0, sum)
      },
      bufferSize - numNewItems
    )
    arr().splice(0, numItemsToDrop)
    arr().push(...vs.slice(0, numNewItems))
    return numItemsToDrop
  }

/**
 * Header-aware circular append for a Change Stream view. Appends `newRows` for
 * `relationName` to `cs.rows`, maintaining `cs.headers` so that every section of rows
 * is preceded by a header row (`{ relationName, columns }`) and orphan rows never
 * appear.
 *
 * Handles two cases where a plain circular buffer push would leave rows without a section header:
 *
 *  - **Batch overflow:** when a single push exceeds `bufferSize`, the trailing tail
 *    is kept but the prepended header at the front of the input is dropped along
 *    with the displaced overflow.
 *  - **Front-shift drift:** every same-relation push shifts existing rows left; once
 *    enough items accumulate, the only header (at index 0) is dropped, and the rest
 *    of the section becomes orphaned.
 *
 * On overflow it always builds a header at the front of the kept tail; on front-shift
 * it captures the most-recent header that's about to be dropped and prepends it back
 * so the surviving rows still render under their section.
 *
 * @param buildHeader Callback that returns a header row for `relationName`. Receives
 *   an optional sample data row so the caller can derive column order from the actual
 *   data shape (with a schema-based fallback for the skipped-bytes path).
 */
export const appendRowsForRelation = (
  cs: ChangeStreamData,
  relationName: string,
  newRows: Row[],
  buildHeader: (sample?: XgressEntry) => Row,
  bufferSize: number,
  sample?: XgressEntry
) => {
  const lastHeaderIdx = cs.headers.at(-1)
  const lastHeader = lastHeaderIdx !== undefined ? cs.rows[lastHeaderIdx] : undefined
  const lastRelationName = lastHeader && 'columns' in lastHeader ? lastHeader.relationName : null
  const sectionHeader: Row | null = relationName !== lastRelationName ? buildHeader(sample) : null

  const itemsToAdd = (sectionHeader ? 1 : 0) + newRows.length

  // Batch alone fills (or overflows) the buffer. Discard everything else and start
  // the kept tail with a fresh header.
  if (itemsToAdd >= bufferSize) {
    const header = sectionHeader ?? buildHeader(sample)
    cs.rows = [header, ...newRows.slice(-(bufferSize - 1))]
    cs.headers = [0]
    return
  }

  const initialLen = cs.rows.length
  const dropCount = Math.max(0, initialLen + itemsToAdd - bufferSize)

  // Capture the most recent header that's about to be shifted off the front, so we
  // can restore section context for any rows that survive its drop.
  let lastDroppedHeader: Row | undefined
  for (const hIdx of cs.headers) {
    if (hIdx >= dropCount) {
      break
    }
    lastDroppedHeader = cs.rows[hIdx]
  }

  cs.rows.splice(0, dropCount)
  let newHeaders = cs.headers.map((i) => i - dropCount).filter((i) => i >= 0)

  // If the kept rows now begin with orphan data rows (no header at index 0),
  // prepend the captured dropped header so the section is preserved.
  if (cs.rows.length > 0 && (newHeaders.length === 0 || newHeaders[0] > 0) && lastDroppedHeader) {
    cs.rows.unshift(lastDroppedHeader)
    newHeaders = [0, ...newHeaders.map((i) => i + 1)]
  }

  if (sectionHeader) {
    newHeaders.push(cs.rows.length)
    cs.rows.push(sectionHeader)
  }
  for (const r of newRows) {
    cs.rows.push(r)
  }
  cs.headers = newHeaders
}
