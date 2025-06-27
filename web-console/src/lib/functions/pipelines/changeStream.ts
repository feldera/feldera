import { BigNumber } from 'bignumber.js/bignumber.js'
import { JSONParser, Tokenizer, TokenParser, type JSONParserOptions } from '@streamparser/json'
import { findIndex } from '$lib/functions/common/array'
import { tuple } from '$lib/functions/common/tuple'
import { RecordBatchReader, RecordBatch, Schema, Field, type TypeMap } from 'apache-arrow'

class BigNumberTokenizer extends Tokenizer {
  parseNumber = BigNumber as any
}

// Batching transform stream that collects items and emits batches based on timer
export function createBatchingTransform<T>(flushIntervalMs: number = 100): TransformStream<T, T[]> {
  let buffer: T[] = []
  let flushTimer: number | null = null
  let controller: TransformStreamDefaultController<T[]> | null = null

  const flush = () => {
    if (buffer.length > 0 && controller) {
      controller.enqueue([...buffer])
      buffer.length = 0
    }
    flushTimer = null
  }

  const scheduleFlush = () => {
    if (flushTimer === null) {
      flushTimer = setTimeout(flush, flushIntervalMs) as any
    }
  }

  return new TransformStream<T, T[]>({
    start(ctrl) {
      controller = ctrl
    },

    transform(chunk) {
      buffer.push(chunk)
      scheduleFlush()
    },

    flush() {
      if (flushTimer !== null) {
        clearTimeout(flushTimer)
        flushTimer = null
      }
      flush()
    }
  })
}

export class BatchProcessor<T> extends WritableStream<T[]> {
  private closedReason: 'ended' | 'cancelled' | null = null

  constructor(
    private readonly source: {
      cancel: () => void
    },
    private readonly cbs: {
      pushChanges: (changes: T[]) => void
      onParseEnded?: (reason: 'ended' | 'cancelled') => void
      onNetworkError?: (e: TypeError, injectValue: (value: T) => void) => void
    }
  ) {
    super({
      write: async (batch: T[]) => {
        if (batch && batch.length > 0 && this.closedReason !== 'cancelled') {
          cbs.pushChanges(batch)
        }
      },

      close: () => {
        this.closedReason ??= 'ended'
        this.cbs.onParseEnded?.(this.closedReason)
      }
    })
  }
  async abort(e?: any) {
    this.closedReason = 'ended'
    this.handleNetworkError(e)
    this.cbs.onParseEnded?.(this.closedReason)
  }

  cancel() {
    this.closedReason = 'cancelled'
    this.source.cancel()
    this.cbs.onParseEnded?.(this.closedReason)
  }

  private handleNetworkError(e: unknown) {
    const error = e instanceof TypeError ? e : new TypeError('Unknown stream error')
    const errorBatch: T[] = []
    this.cbs.onNetworkError?.(error, (value: T) => errorBatch.push(value))
    if (errorBatch.length > 0) {
      this.cbs.pushChanges(errorBatch)
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
export function splitStreamByMaxChunk({
  maxChunkBytes,
  maxChunkBufferSize,
  onBytesSkipped
}: {
  maxChunkBytes: number
  maxChunkBufferSize: number
  onBytesSkipped?: (bytes: number) => void
}): TransformStream<Uint8Array, Uint8Array> {
  return new TransformStream<Uint8Array, Uint8Array>(
    {
      async transform(chunk, controller) {
        let start = 0
        while (start < chunk.length) {
          const end = Math.min(chunk.length, start + maxChunkBytes)
          if (hasBackpressure(controller, maxChunkBytes)) {
            break
          }
          controller.enqueue(chunk.subarray(start, end))

          start = end
        }
        if (start < chunk.length) {
          onBytesSkipped?.(chunk.length - start)
        }
      }
    },
    {},
    { highWaterMark: maxChunkBufferSize, size: (c) => c?.length ?? 0 }
  )
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

class JSONParserTransformer<T> implements Transformer<Uint8Array | string, T> {
  // @ts-ignore Controller always defined during start
  private controller: TransformStreamDefaultController<T>
  // @ts-ignore Controller always defined during start
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
    } catch (e) {
      this.parser = mkTransformerParser(this.controller, this.opts)
    }
    await new Promise((resolve) => setTimeout(resolve))
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

interface HeaderChunk {
  header: {
    schema: Schema
    fields: Field[]
    metadata?: Map<string, string>
  }
}

interface RowChunk {
  row: any[]
}

export type ArrowIpcChunk = HeaderChunk | RowChunk

export class SimpleArrowIPCTransformStream<T extends TypeMap = any> {
  private transform: {
    writable: WritableStream<Uint8Array>
    readable: ReadableStream<RecordBatch<T>>
  }

  constructor(
    writableStrategy?: ByteLengthQueuingStrategy,
    readableStrategy?: { autoDestroy: boolean }
  ) {
    this.transform = RecordBatchReader.throughDOM<T>(writableStrategy, readableStrategy)
  }

  // Get the writable side for input
  get writable(): WritableStream<Uint8Array> {
    return this.transform.writable
  }

  // Transform the readable side to emit header + rows
  get readable(): ReadableStream<ArrowIpcChunk[]> {
    let headerEmitted = false

    return new ReadableStream<ArrowIpcChunk[]>({
      start: async (controller) => {
        try {
          const reader = this.transform.readable.getReader()

          while (true) {
            const { done, value: batch } = await reader.read()

            if (done) {
              controller.close()
              break
            }

            // Emit header once
            if (!headerEmitted && batch.schema) {
              const headerChunk: HeaderChunk = {
                header: {
                  schema: batch.schema,
                  fields: batch.schema.fields,
                  metadata: batch.schema.metadata
                }
              }
              controller.enqueue([headerChunk])
              console.log('emit header')
              headerEmitted = true
            }

            controller.enqueue(this.batchToArray(batch))
          }
        } catch (error) {
          controller.error(error)
        }
      }
    })
  }

  private batchToArray(batch: RecordBatch<T>) {
    const res: ArrowIpcChunk[] = new Array(batch.numRows)

    for (let i = 0; i < batch.numRows; i++) {
      const row: any[] = []

      for (let j = 0; j < batch.schema.fields.length; j++) {
        const field = batch.schema.fields[j]
        const column = batch.getChildAt(j)

        if (column && column.isValid(i)) {
          const value = column.get(i)
          row[j] = this.convertValue(value)
        }
      }

      res[i] = { row }
    }
    return res
  }

  private convertValue(value: any): any {
    if (value === null || value === undefined) return null
    if (typeof value === 'bigint') {
      return value <= Number.MAX_SAFE_INTEGER && value >= Number.MIN_SAFE_INTEGER
        ? Number(value)
        : value.toString()
    }
    if (value instanceof Date) return value.toISOString()
    return value
  }
}
