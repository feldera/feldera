import { type JSONParser, type JSONParserOptions, Tokenizer, TokenParser } from '@streamparser/json'
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

const NEWLINE = 0x0a

/**
 * Soft cap on how many bytes a single not-yet-newline-terminated record may accumulate.
 * Anything beyond this is reported as skipped — accepting unbounded leftovers would let
 * a runaway record exhaust browser memory.
 */
const MAX_LINE_SIZE = 16 * 1024 * 1024

/**
 * Soft target for the size of each `parser.write` call. The reader splits each
 * line-aligned block at the nearest newline below this size and yields between pieces,
 * so a single big network chunk can't monopolize the main thread.
 */
const PARSE_PIECE_TARGET_BYTES = 16 * 1024

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
 * Streams newline-delimited JSON from `source` and dispatches parsed values via
 * `pushChanges` on a fixed cadence.
 *
 * Single sequential reader loop (no `TransformStream` pipe). One `setInterval` drives UI
 * updates. Overflow shedding uses a single counter: when the bytes of unflushed values
 * exceed `bufferSize`, the next line-aligned block is dropped and reported via
 * `onBytesSkipped`. Between network chunks the reader yields to a macrotask so the flush
 * timer (and the rest of the page) keep ticking.
 *
 * The parser is `write`/`reset`-shaped on purpose: each network chunk is split into one
 * line-aligned block (everything up to the last `\n`) and passed straight to
 * `parser.write(block)`. The parser handles the multi-line block as a series of
 * concatenated documents in a single call, which is much cheaper than tokenizing one
 * line at a time. If `write` throws, the parser is `reset()` — its internal state is
 * discarded, the block is reported as skipped, and the next block starts on a clean
 * parser. Because we never feed the parser anything that isn't a complete document,
 * structurally-invalid-JSON corruption cannot occur.
 *
 * @param parser Long-lived parser, typically allocated once per stream.
 * @param options.bufferSize **Main tuning knob.** Per-flush-window byte budget for
 *   pieces admitted into the parser. When the running total of admitted bytes in the
 *   current window exceeds this, the next piece (up to `PARSE_PIECE_TARGET_BYTES`) is dropped (and
 *   reported via `onBytesSkipped`) instead of being parsed. The budget resets on every
 *   `flushIntervalMs` tick, so in steady state the worst-case stream-to-UI latency is
 *   approximately `bufferSize / parse_rate`.
 *
 *   - **Too many skipped records?** Raise `bufferSize`. More bytes are admitted before
 *     dropping kicks in, but the visible data lags further behind real time.
 *   - **Updates feel too slow / stale?** Lower `bufferSize`. The window's worth of
 *     CPU/UI work is capped sooner, dropping the excess; perceived latency falls but
 *     the skipped-bytes counter climbs.
 * @param options.flushIntervalMs How often `pushChanges` fires and `bufferSize` is
 *   released. Lower → smoother UI cadence and a tighter latency cap, but more frequent
 *   Svelte/DOM updates per second. Default 100ms.
 */
export const parseStream = <T>(
  source: {
    stream: ReadableStream<Uint8Array<ArrayBufferLike>>
    cancel: () => void
  },
  parser: StreamingJsonParser<T>,
  cbs: {
    pushChanges: (changes: T[]) => void
    onBytesSkipped?: (bytes: number) => void
    onParseEnded?: (reason: 'ended' | 'cancelled') => void
    onNetworkError?: (e: TypeError, injectValue: (value: T) => void) => void
  },
  options?: { bufferSize?: number; flushIntervalMs?: number }
) => {
  invariant(
    source.stream instanceof ReadableStream,
    `parseStream(): stream is ${JSON.stringify(source.stream)}`
  )
  const maxPendingBytes = options?.bufferSize ?? 1_000_000
  const flushIntervalMs = options?.flushIntervalMs ?? 100

  let cancelled = false
  let closedReason: null | 'ended' | 'cancelled' = null
  let endedReported = false

  const outBuffer: T[] = []
  let pendingBytes = 0

  const reader = source.stream.getReader()

  const readLoop = async () => {
    const decoder = new TextDecoder('utf-8')
    let leftover = ''
    while (!cancelled) {
      let chunk
      try {
        chunk = await reader.read()
      } catch (e) {
        cbs.onNetworkError?.(e as TypeError, (v) => outBuffer.push(v))
        return
      }
      if (chunk.done) {
        return
      }
      const text = leftover + decoder.decode(chunk.value, { stream: true })
      const lastNewline = text.lastIndexOf('\n')
      if (lastNewline === -1) {
        if (text.length > MAX_LINE_SIZE) {
          cbs.onBytesSkipped?.(text.length)
          leftover = ''
        } else {
          leftover = text
        }
        continue
      }
      const block = text.slice(0, lastNewline + 1)
      leftover = text.slice(lastNewline + 1)

      // Feed the parser line-aligned pieces of bounded size and yield between them.
      // The parser is the same instance across all pieces (zero per-piece allocation),
      // but bounding the size of each `parser.write` call caps how long the main
      // thread is blocked at a stretch. The pending-bytes check is per-piece, not
      // per-block: if we admitted/dropped at block granularity, a single network
      // chunk larger than `bufferSize` would be dropped even though the
      // flush timer resets `pendingBytes` to 0 between yields and most pieces would
      // fit individually. Dropping per-piece (each piece is itself line-aligned)
      // keeps as much data flowing as the flush rate allows.
      let cursor = 0
      while (cursor < block.length) {
        let end = Math.min(cursor + PARSE_PIECE_TARGET_BYTES, block.length)
        if (end < block.length) {
          const nl = block.lastIndexOf('\n', end - 1)
          end = nl > cursor ? nl + 1 : block.length
        }
        const pieceLen = end - cursor
        if (pendingBytes + pieceLen > maxPendingBytes) {
          cbs.onBytesSkipped?.(pieceLen)
        } else {
          const piece = block.slice(cursor, end)
          try {
            const items = parser.write(piece)
            for (const item of items) {
              outBuffer.push(item)
            }
            pendingBytes += pieceLen
          } catch {
            parser.reset()
            cbs.onBytesSkipped?.(pieceLen)
          }
        }
        cursor = end
        await yieldToEventLoop()
        if (cancelled) {
          return
        }
      }
    }
  }

  setTimeout(async () => {
    await readLoop()
    closedReason ??= 'ended'
  })

  const flushHandle = setInterval(() => {
    // Reset the per-window byte budget unconditionally. `pendingBytes` represents
    // "CPU work admitted since the last window", not "bytes currently in outBuffer",
    // so even pieces that parsed successfully but produced no values (heartbeat
    // lines with no `json_data`, lines that don't match the parser's path filter,
    // etc.) need to release their budget on each tick. If we only reset alongside
    // pushChanges, a stream of no-output pieces would silently exhaust the cap
    // and the read loop would drop everything from then on.
    pendingBytes = 0
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
      reader
        .cancel()
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
 *
 * @param stream
 * @param pushChanges
 * @param options.bufferSize Soft byte budget for the line queue between the network reader
 *   and the JSON parser. When the queue exceeds this, incoming complete lines are dropped
 *   instead of enqueued — see `splitByNewlineWithOverflowShedding` for why this is sound.
 * @returns
 */
export const parseCancellable = <T, Transformer extends TransformStream<Uint8Array, T>>(
  source: {
    stream: ReadableStream<Uint8Array<ArrayBufferLike>>
    cancel: () => void
  },
  cbs: {
    pushChanges: (changes: T[]) => void
    onBytesSkipped?: (bytes: number) => void
    onParseEnded?: (reason: 'ended' | 'cancelled') => void
    onNetworkError?: (e: TypeError, injectValue: (value: T) => void) => void
  },
  transformer: Transformer,
  options?: { bufferSize?: number }
) => {
  invariant(
    source.stream instanceof ReadableStream,
    `parseCancellable(): stream is ${JSON.stringify(source.stream)}`
  )
  const reader = source.stream
    .pipeThrough(
      splitByNewlineWithOverflowShedding(options?.bufferSize ?? 1_000_000, cbs.onBytesSkipped)
    )
    .pipeThrough(transformer)
    .getReader()
  const resultBuffer = [] as T[]
  setTimeout(async () => {
    while (true) {
      try {
        const { done, value } = await reader.read()
        if (done || value === undefined) {
          break
        }
        resultBuffer.push(value)
      } catch (e) {
        cbs.onNetworkError?.(e as TypeError, (value: T) => resultBuffer.push(value))
        cbs.onParseEnded?.('ended')
        break
      }
    }
  })
  const flush = () => {
    if (resultBuffer.length) {
      cbs.pushChanges?.(resultBuffer)
      resultBuffer.length = 0
    }
  }
  let closedReason: null | 'ended' | 'cancelled' = null
  setTimeout(async () => {
    reader.closed.then(
      () => {
        closedReason ??= 'ended'
      },
      (e) => {
        closedReason = 'ended'
        cbs.onNetworkError?.(e, (value: T) => resultBuffer.push(value))
      }
    )
    while (true) {
      flush()
      if (closedReason) {
        break
      }
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
    cbs.onParseEnded?.(closedReason)
  })
  return {
    cancel: () => {
      flush()
      closedReason = 'cancelled'
      reader.cancel().then(() => {
        source.cancel()
      })
    }
  }
}

/**
 * Trims each network chunk so what reaches the JSON parser starts and ends on a line
 * boundary, without emitting individual lines or scanning the interior. The trailing
 * partial line is held over to be prepended to the next chunk; everything before it —
 * including any prior leftover — is a contiguous run of complete top-level JSON
 * documents that the parser handles in one `write()` call. All current consumers
 * (pipeline egress, ad-hoc query results, time-series) emit newline-delimited JSON.
 *
 * The line boundary is the overflow-drop granularity, not a parser requirement. The
 * parser accepts both multi-record blocks and blocks that span only part of what was
 * originally one network chunk — as long as every emit and every drop snaps to a
 * newline, the parser never receives a partial record.
 *
 * Overflow shedding: when the readable side's queued bytes reach `maxBufferedBytes`
 * (`controller.desiredSize <= 0`), the line-aligned block is dropped wholesale instead
 * of enqueued, and its byte count is reported via `onBytesSkipped` so the UI can render
 * a gap marker.
 *
 * Line-aligned drops guarantee the parser's input is always a clean sequence of complete
 * documents — with gaps but never corruption — because drops and emits never split a
 * record mid-way.
 *
 * Records longer than `MAX_LINE_SIZE` while still being assembled are reported as
 * skipped and discarded, since accepting them would let a runaway record exhaust memory.
 */
function splitByNewlineWithOverflowShedding(
  maxBufferedBytes: number,
  onBytesSkipped?: (bytes: number) => void
): TransformStream<Uint8Array, Uint8Array> {
  let leftover: Uint8Array | null = null
  return new TransformStream<Uint8Array, Uint8Array>(
    {
      transform(chunk, controller) {
        // Scan only backwards from the end of the chunk to find the last newline.
        // The bytes before it (plus any prior leftover) form a contiguous run of complete
        // lines; the bytes after it are the head of an incomplete record. We never need
        // to find the newlines in between — the parser will walk them itself.
        let lastNewline = -1
        for (let i = chunk.length - 1; i >= 0; --i) {
          if (chunk[i] === NEWLINE) {
            lastNewline = i
            break
          }
        }

        if (lastNewline === -1) {
          // No record terminator yet — extend the leftover.
          const accumulated = (leftover?.length ?? 0) + chunk.length
          if (accumulated > MAX_LINE_SIZE) {
            onBytesSkipped?.(accumulated)
            leftover = null
            return
          }
          if (leftover) {
            const merged = new Uint8Array(accumulated)
            merged.set(leftover, 0)
            merged.set(chunk, leftover.length)
            leftover = merged
          } else {
            // Copy so we don't pin the network chunk's full backing buffer for one record.
            leftover = chunk.slice()
          }
          return
        }

        const emitFromChunk = lastNewline + 1
        const leftoverLen = leftover?.length ?? 0
        const emitLen = leftoverLen + emitFromChunk

        if (hasBackpressure(controller)) {
          onBytesSkipped?.(emitLen)
        } else if (leftoverLen === 0) {
          // No prior leftover — enqueue a view into the chunk; saves an allocation.
          // The view keeps the chunk's backing buffer alive only until the parser
          // consumes it, which happens on the next transform tick.
          controller.enqueue(chunk.subarray(0, emitFromChunk))
        } else {
          const merged = new Uint8Array(emitLen)
          merged.set(leftover!, 0)
          merged.set(chunk.subarray(0, emitFromChunk), leftoverLen)
          controller.enqueue(merged)
        }
        leftover = null

        const remainderLen = chunk.length - emitFromChunk
        if (remainderLen === 0) {
          return
        }
        if (remainderLen > MAX_LINE_SIZE) {
          onBytesSkipped?.(remainderLen)
        } else {
          // Copy so leftover doesn't pin the (much larger) network chunk buffer.
          leftover = chunk.slice(emitFromChunk)
        }
      },
      flush(controller) {
        if (leftover && leftover.length > 0) {
          if (hasBackpressure(controller)) {
            onBytesSkipped?.(leftover.length)
          } else {
            controller.enqueue(leftover)
          }
          leftover = null
        }
      }
    },
    {},
    { highWaterMark: maxBufferedBytes, size: (c) => c?.length ?? 0 }
  )
}

const hasBackpressure = <T>(controller: TransformStreamDefaultController<T>) => {
  return controller.desiredSize !== null && controller.desiredSize <= 0
}

const mkTransformerParser = <T>(
  controller: TransformStreamDefaultController<T>,
  opts?: JSONParserOptions
) => {
  const tokenizer = new BigNumberTokenizer()
  const tokenParser = new TokenParser(opts)
  tokenizer.onToken = tokenParser.write.bind(tokenParser)
  tokenParser.onValue = (value) => {
    controller.enqueue(value.value as T)
  }

  const parser = {
    onToken: tokenizer.onToken.bind(tokenizer),
    get isEnded() {
      return tokenizer.isEnded
    },
    write: tokenizer.write.bind(tokenizer),
    end: tokenizer.end.bind(tokenizer),
    onError: tokenizer.onError.bind(tokenizer)
  } as JSONParser
  return parser
}

class JSONParserTransformer<T> implements Transformer<Uint8Array | string, T> {
  // @ts-expect-error Controller always defined during start
  private controller: TransformStreamDefaultController<T>
  // @ts-expect-error Controller always defined during start
  private parser: JSONParser
  private opts?: JSONParserOptions

  constructor(opts?: JSONParserOptions) {
    this.opts = opts
  }

  start(controller: TransformStreamDefaultController<T>) {
    this.controller = controller
    this.parser = mkTransformerParser(this.controller, this.opts)
  }

  async transform(chunk: Uint8Array | string) {
    try {
      this.parser.write(chunk)
    } catch {
      // Every chunk arrives line-aligned from the upstream splitter, so a parse
      // error means the source itself produced a malformed document. Reset the
      // parser to drop its partial state and continue with the next chunk.
      this.parser = mkTransformerParser(this.controller, this.opts)
    }
    // Yield to a macrotask between every chunk — not because the parser needs a
    // break, but because the WebStreams pipe runs transforms back-to-back via
    // microtasks. Without this hop, the 100ms flush timer (and any other
    // setTimeout/setInterval) starves behind the microtask queue, which makes
    // the UI appear to update only every few seconds on busy streams.
    await yieldToEventLoop()
  }

  flush() {}
}

export class CustomJSONParserTransformStream<T> extends TransformStream<Uint8Array | string, T> {
  constructor(
    opts?: JSONParserOptions,
    writableStrategy?: QueuingStrategy<Uint8Array | string>,
    readableStrategy?: QueuingStrategy<T>
  ) {
    const transformer = new JSONParserTransformer(opts)
    super(transformer, writableStrategy, readableStrategy)
  }
}

export class SplitNewlineTransformStream extends TransformStream<Uint8Array, string> {
  private decoder: TextDecoder
  private buffer: string
  private newlineRegex: RegExp

  constructor(
    writableStrategy?: QueuingStrategy<Uint8Array>,
    readableStrategy?: QueuingStrategy<string>
  ) {
    super(
      {
        transform: (chunk, controller) => this.transform(chunk, controller),
        flush: (controller) => this.flush(controller)
      },
      writableStrategy,
      readableStrategy
    )

    this.decoder = new TextDecoder('utf-8')
    this.buffer = ''
    this.newlineRegex = /\r?\n/g // Matches both \n and \r\n
  }

  private async transform(chunk: Uint8Array, controller: TransformStreamDefaultController<string>) {
    // Decode the chunk as a string and append it to the buffer
    this.buffer += this.decoder.decode(chunk, { stream: true })
    // Use RegExp.exec to find each newline
    let match
    while ((match = this.newlineRegex.exec(this.buffer)) !== null) {
      // Extract the line from the start of the buffer up to the matched newline
      const line = this.buffer.slice(0, match.index + match[0].length) // Include the newline character at the end of the line with `match[0].length`: '\n' => 1, '\r\n' => 2
      controller.enqueue(line)

      // Update buffer by removing the processed line and newline
      this.buffer = this.buffer.slice(this.newlineRegex.lastIndex) // this.buffer.slice(match.index + match[0].length);
      // Reset lastIndex for the regex to handle the modified buffer
      this.newlineRegex.lastIndex = 0
    }
  }

  private flush(controller: TransformStreamDefaultController<string>) {
    // Enqueue any remaining buffered data
    if (this.buffer) {
      controller.enqueue(this.buffer)
      this.buffer = ''
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
