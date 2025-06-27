import { BigNumber } from 'bignumber.js/bignumber.js'
import { JSONParser, Tokenizer, TokenParser, type JSONParserOptions } from '@streamparser/json'
import { findIndex } from '$lib/functions/common/array'
import { tuple } from '$lib/functions/common/tuple'
import invariant from 'tiny-invariant'

class BigNumberTokenizer extends Tokenizer {
  parseNumber = BigNumber as any
}

interface BatchingCallbacks<T> {
  pushChanges: (changes: T[]) => void
  onParseEnded?: (reason: 'ended' | 'cancelled') => void
  onNetworkError?: (e: TypeError, injectValue: (value: T) => void) => void
}

interface BatchingOptions {
  batchSize?: number
  batchTimeoutMs?: number
}

export class BatchingWritableStream<T> extends WritableStream<T> {
  private batch: T[] = []
  private batchTimer: number | null = null
  private bytesProcessed = 0
  // private bytesSkipped = 0
  // private retryCount = 0
  private isEnded = false

  constructor(
    private cbs: BatchingCallbacks<T>,
    private options: BatchingOptions = {}
  ) {
    const { batchSize = 5000, batchTimeoutMs = 100 } = options

    super(
      {
        write: async (chunk: T, controller: WritableStreamDefaultController) => {
          // Add chunk to current batch
          this.batch.push(chunk)
          this.bytesProcessed++

          // Check if we should flush the batch
          if (this.batch.length >= batchSize) {
            await this.flushBatch()
            await new Promise((resolve) => setTimeout(resolve))
          } else {
            // Set/reset batch timeout
            this.resetBatchTimer(batchTimeoutMs)
          }
        },

        close: async () => {
          try {
            // Flush any remaining items in batch
            if (this.batch.length > 0) {
              await this.flushBatch()
            }

            this.clearBatchTimer()
            this.isEnded = true
            this.cbs.onParseEnded?.('ended')
          } catch (error) {
            // If flush fails on close, we still need to clean up
            this.clearBatchTimer()
            this.isEnded = true
            this.cbs.onParseEnded?.('ended')
            throw error
          }
        }
      },
      { highWaterMark: batchSize, size: () => 1 }
    )
  }

  async abort(reason?: any) {
    this.clearBatchTimer()
    this.isEnded = true
    this.cbs.onParseEnded?.('cancelled')
  }

  private async flushBatch(): Promise<void> {
    if (this.batch.length === 0) {
      return
    }

    this.clearBatchTimer()

    if (this.isEnded) {
      return
    }

    const currentBatch = this.batch
    this.batch = []

    try {
      await Promise.resolve(this.cbs.pushChanges(currentBatch))
    } catch (e) {
      // Failed to push stream items
    }
  }

  private resetBatchTimer(timeoutMs: number): void {
    this.clearBatchTimer()

    this.batchTimer = window.setTimeout(async () => {
      try {
        await this.flushBatch()
      } catch (error) {
        // Timer-triggered flush errors need to be handled carefully
        // since there's no controller context
      }
    }, timeoutMs)
  }

  private clearBatchTimer(): void {
    if (this.batchTimer !== null) {
      clearTimeout(this.batchTimer)
      this.batchTimer = null
    }
  }

  // Utility method to get current statistics
  getStats() {
    return {
      bytesProcessed: this.bytesProcessed,
      // bytesSkipped: this.bytesSkipped,
      currentBatchSize: this.batch.length,
      isEnded: this.isEnded
    }
  }
}

/**
 * Splits transform stream in chunks of size maxChunkBytes or less
 * Includes basic backpressure relief: incoming chunks are dropped if backpressure is detected
 * @param maxChunkBytes Maximum size of an output chunk
 * @param maxChunkBufferSize When output buffer size grows close to this value backpressure occurs
 * @returns
 */
export function splitStreamByMaxChunk(
  maxChunkBytes: number
): TransformStream<Uint8Array, Uint8Array> {
  return new TransformStream<Uint8Array, Uint8Array>({
    async transform(chunk, controller) {
      let start = 0
      while (start < chunk.length) {
        const end = Math.min(chunk.length, start + maxChunkBytes)
        controller.enqueue(chunk.subarray(start, end))
        start = end
      }
    }
  })
}

const hasBackpressure = <T>(controller: TransformStreamDefaultController<T>, offset: number) => {
  return controller.desiredSize !== null && controller.desiredSize - offset < 0
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

type ExtraOpts = {
  onBytesSkipped?: ((bytes: number) => void) | undefined
  skipThresholdMs?: number
}

const recordsNumberWatermark = 5000

class JSONParserTransformer<T> implements Transformer<Uint8Array | string, T> {
  // @ts-ignore Controller always defined during start
  private controller: TransformStreamDefaultController<T>
  // @ts-ignore Controller always defined during start
  private parser: JSONParser
  private lastSkipTimestamp = 0
  private opts?: JSONParserOptions & ExtraOpts
  private totalSkippedBytes = 0

  constructor(opts?: JSONParserOptions & ExtraOpts) {
    this.opts = opts
  }

  start(controller: TransformStreamDefaultController<T>) {
    this.controller = controller
    this.parser = mkTransformerParser(this.controller, this.opts)
  }

  async transform(chunk: Uint8Array | string, controller: TransformStreamDefaultController<T>) {
    const now = Date.now()
    const skipThresholdMs = this.opts?.skipThresholdMs ?? 100

    // Check if we're currently in a skip period
    const isCurrentlySkipping = now < this.lastSkipTimestamp + skipThresholdMs

    // Check for new backpressure
    const hasCurrentBackpressure = hasBackpressure(controller, recordsNumberWatermark)

    if (isCurrentlySkipping) {
      this.lastSkipTimestamp = now
      this.totalSkippedBytes += chunk.length
      return
    }

    if (hasCurrentBackpressure) {
      // Backpressure detected - clearing the upstream chunks
      if (!isCurrentlySkipping) {
        try {
          this.parser.end()
        } catch {}
      }
      this.lastSkipTimestamp = now
      this.totalSkippedBytes += chunk.length
      return
    }

    // Just exited skip period
    if (this.lastSkipTimestamp > 0) {
      this.opts?.onBytesSkipped?.(this.totalSkippedBytes)
      this.lastSkipTimestamp = 0
      this.totalSkippedBytes = 0

      this.parser = mkTransformerParser(this.controller, this.opts)
    }

    // Process chunk normally
    try {
      this.parser.write(chunk)
    } catch (e) {
      this.opts?.onBytesSkipped?.(this.totalSkippedBytes)
      this.parser = mkTransformerParser(this.controller, this.opts)
      // We can ignore the parse error and skipt to the next chunk
    }
  }

  flush() {}
}

export class CustomJSONParserTransformStream<T> extends TransformStream<Uint8Array | string, T> {
  constructor(
    opts?: JSONParserOptions & ExtraOpts,
    writableStrategy?: QueuingStrategy<Uint8Array | string>,
    readableStrategy?: QueuingStrategy<T>
  ) {
    const transformer = new JSONParserTransformer(opts)
    super(
      transformer,
      writableStrategy,
      // readableStrategy
      // { highWaterMark: maxChunkBufferSize, size: (c) => c?.length ?? 0 },
      { highWaterMark: recordsNumberWatermark, size: () => 1 }
    )
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
      let f = function (replacementItems: R[] = []) {
        arr().splice(0, offset, ...replacementItems)
        arr().push(...values.slice(-bufferSize).map(mapValue))
      }
      return Object.assign(
        function (items?: R[]) {
          f?.(items)
          f = undefined!
        },
        {
          offset: Math.max(offset, 0)
        }
      )
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
    let f = function (replacementItems: R[] = []) {
      arr().splice(0, numItemsToDrop, ...replacementItems)
      arr().push(...vs.slice(0, numNewItems))
    }
    return Object.assign(
      function (items?: R[]) {
        f?.(items)
        f = undefined!
      },
      { offset: numItemsToDrop }
    )
  }
