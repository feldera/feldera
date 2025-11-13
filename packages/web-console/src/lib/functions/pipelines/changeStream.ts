import { BigNumber } from 'bignumber.js'
import { JSONParser, Tokenizer, TokenParser, type JSONParserOptions } from '@streamparser/json'
import { findIndex } from '$lib/functions/common/array'
import { tuple } from '$lib/functions/common/tuple'
import invariant from 'tiny-invariant'

class BigNumberTokenizer extends Tokenizer {
  parseNumber = BigNumber as any
}

/**
 *
 * @param stream
 * @param pushChanges
 * @param options.bufferSize Threshold size of the buffer that holds unprocessed JSON chunks.
 * If the buffer size exceeds this value - when the new JSON batch arrives previous JSON batches are dropped
 * until the buffer size is under the threshold, or only one batch remains.
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
  const maxChunkSize = 100000
  const reader = source.stream
    .pipeThrough(
      splitStreamByMaxChunk(maxChunkSize, options?.bufferSize ?? 1000000, cbs.onBytesSkipped)
    )
    .pipeThrough(transformer)
    .getReader()
  let resultBuffer = [] as T[]
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
 * Splits transform stream in chunks of size maxChunkBytes or less
 * Includes basic backpressure relief: incoming chunks are dropped if backpressure is detected
 * @param maxChunkBytes Maximum size of an output chunk
 * @param maxChunkBufferSize When output buffer size grows close to this value backpressure occurs
 * @returns
 */
function splitStreamByMaxChunk(
  maxChunkBytes: number,
  maxChunkBufferSize: number,
  onBytesSkipped?: (bytes: number) => void
): TransformStream<Uint8Array, Uint8Array> {
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
