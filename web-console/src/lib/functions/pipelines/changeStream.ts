import BigNumber from 'bignumber.js'
import {
  JSONParser,
  Tokenizer,
  TokenParser,
  type JSONParserOptions,
  type TokenParserOptions
} from '@streamparser/json'
import type { ParsedElementInfo } from '@streamparser/json/utils/types/parsedElementInfo.js'
import { discreteDerivative } from '$lib/functions/common/math'
import { tuple } from '$lib/functions/common/tuple'
import { chunkIndices } from '$lib/functions/common/array'
import { humanSize } from '$lib/functions/common/string'
import { JSONParser as JSONParserTransformStream } from '@streamparser/json-whatwg'
import { cloneParsedElementInfo } from '@streamparser/json-whatwg/utils.js'

class BigNumberTokenizer extends Tokenizer {
  parseNumber = BigNumber as any
}

const mkParser = (
  onValue: (parsedElementInfo: ParsedElementInfo) => void,
  options?: TokenParserOptions
): { write: JSONParser['write']; end: JSONParser['end']; isParserEnded: () => boolean } => {
  const tokenizer = new BigNumberTokenizer()
  const tokenParser = new TokenParser(options)
  tokenizer.onToken = tokenParser.write.bind(tokenParser)
  tokenParser.onValue = onValue
  return Object.assign(tokenizer, {
    isParserEnded() {
      return tokenParser.isEnded
    }
  })
}

/**
 *
 * @param chunksToParse a list of batches (complete JSON objects), each split into a list of bytestring chunks.
 * It contains no empty bytestrings
 */
const parseStreamOfUTF8JSON =
  <T>(
    pushChanges: (changes: T[]) => void,
    onBytesSkipped?: (bytes: number) => void,
    onParseEnded?: (reason: any) => void,
    options?: TokenParserOptions & { bufferSize?: number }
  ) =>
  (chunksToParse: Uint8Array[][]) => {
    let count = 0
    let resultBuffer = [] as any[]

    const onValue = ({ value }: ParsedElementInfo) => {
      resultBuffer[count] = value
      ++count
    }

    let parser = mkParser(onValue, options)
    let isEnd = false
    let done = true
    let dataSourceEnded = false

    const interrupt = async () => {
      try {
        parser.end()
      } catch {
        // We ignore the error because we just want to interrupt parsing and production of values
      }
      parser = mkParser(onValue, options)
      chunksToParse = [[]]
      while (!done) {
        // Release thread to parse JSON
        await new Promise((resolve) => setTimeout(resolve))
      }
    }

    const maxJsonChunkBytes = 200000
    const maxSynchronousParseBytes = 10000
    return {
      start: async () => {
        let synchronousParseBytesCount = 0
        while (!isEnd) {
          const value = chunksToParse[0]?.shift()
          if (!value) {
            if (chunksToParse.length > 1) {
              // Keep atleast a single empty batch in chunksToParse
              chunksToParse.shift()
              parser = mkParser(onValue, options)
            }
            await new Promise((resolve) => setTimeout(resolve))
            if (chunksToParse.length < 2 && dataSourceEnded) {
              break
            } else {
              continue
            }
          }

          // Parse JSON in subchunks of up to maxJsonChunkBytes bytes to keep UI freezes to a minimum
          // because during parsing JavaScript cannot handle UI updates
          const positions = chunkIndices(0, value.length, maxJsonChunkBytes)
          const pairs = discreteDerivative(positions, tuple)
          for (const [n1, n0] of pairs) {
            if (isEnd) {
              break
            }

            {
              // In each parse iteration we check if buffer is not too large
              // If so, we drop oldest batches until buffer fits the threshold, or a single batch remains
              const batchLengths = chunksToParse.map((batch) =>
                batch.reduce((acc, cur) => acc + cur.length, 0)
              )
              const previousBufferSize = batchLengths.reduce((acc, cur) => acc + cur, 0)
              let bufferSize = previousBufferSize
              const tooLarge = () =>
                chunksToParse.length > 1 && bufferSize > (options?.bufferSize ?? 0)
              const restart = tooLarge()
              while (tooLarge()) {
                bufferSize -= batchLengths.shift()!
                chunksToParse.shift()
              }
              if (restart) {
                console.log(
                  `Skipped ${humanSize(previousBufferSize - bufferSize)} of change stream. New buffer size is ${humanSize(bufferSize)}`
                )
                onBytesSkipped?.(previousBufferSize - bufferSize)
                parser = mkParser(onValue, options)
                break
              }
            }

            done = false
            try {
              parser.write(value.slice(n0, n1))
            } catch (e) {
              console.log('JSON parse error', e)
              done = true
              break
            }
            synchronousParseBytesCount += n1 - n0

            if (parser.isParserEnded()) {
              parser = mkParser(onValue, options)
            }
            done = true
            if (synchronousParseBytesCount >= maxSynchronousParseBytes) {
              synchronousParseBytesCount = 0
              pushChanges(resultBuffer.slice(0, count))
              count = 0
              await new Promise((resolve) => setTimeout(resolve))
            }
          }
        }
        if (synchronousParseBytesCount !== 0) {
          pushChanges(resultBuffer.slice(0, count))
        }
        onParseEnded?.(undefined)
      },
      stop() {
        // Stop data stream and parsing immediately
        isEnd = true
        interrupt()
      },
      end() {
        // End reading data stream and continue parsing buffered data
        dataSourceEnded = true
      }
    }
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
export const parseUTF8JSON = <T>(
  stream: ReadableStream<Uint8Array>,
  cbs: {
    pushChanges: (changes: T[]) => void
    onBytesSkipped?: (bytes: number) => void
    onParseEnded?: () => void
  },
  options?: TokenParserOptions & { bufferSize?: number }
) => {
  // let cancel = false
  // queueMicrotask(async () => {
  //   const parser = new JSONParser({
  //     separator: '\n',
  //     ...options
  //   })
  //   parser.onEnd = onParseEnded
  //   const x = ({ value }: ParsedElementInfo) => {
  //     let count = 0
  //     let max = 10
  //   }
  //   parser.onValue = x
  //   let buffer: Uint8Array[] = []
  //   while(true) {
  //     const reader = stream.getReader()
  //     const { done, value } = await reader.read()
  //     if (done || cancel) {
  //       break
  //     }
  //     parser.write(value);
  //   }
  // })
  // return {
  //   cancel: () => { cancel = true }
  // }
  // return processUTF8StreamByLine(
  //   stream,
  //   parseStreamOfUTF8JSON(pushChanges, onBytesSkipped, onParseEnded, options)
  // )

  // let cancel = false
  const reader = stream
    .pipeThrough(splitStreamByMaxChunk(options?.bufferSize ?? 100000))
    .pipeThrough(
      new BackpressureTransformStream({
        maxChunkBufferSize: 1000000,
        onBytesSkipped: cbs.onBytesSkipped
      })
    )
    .pipeThrough(
      new CustomJSONParserTransformStream({
        ...options
      })
    )
    .getReader()
  let resultBuffer = [] as T[]
  setTimeout(async () => {
    while (true) {
      const { done, value } = await reader.read()
      if (done || !value /*|| cancel*/) {
        break
      }
      resultBuffer.push(value.value as T)
    }
  })
  setTimeout(async () => {
    let closed = false
    reader.closed.then(() => (closed = true))
    while (true) {
      if (closed) {
        break
      }
      if (resultBuffer.length) {
        console.log(`Pushing ${resultBuffer.length} changes`)
        cbs.pushChanges?.(resultBuffer)
        resultBuffer.length = 0
      }
      await new Promise((resolve) => setTimeout(resolve))
    }
  })
  return {
    cancel: () => {
      // cancel = true
      reader.cancel()
    }
  }
}

const processUTF8StreamInChunks = (
  stream: ReadableStream<Uint8Array>,
  parser: (chunks: Uint8Array[]) => {
    write: (chunk: Uint8Array) => Promise<void>
    end: () => void
  }
) => {
  const reader = stream.pipeThrough(splitStreamByMaxChunk(2000)).getReader()
  let chunksToParse = [] as Uint8Array[]
  const { write, end } = parser(chunksToParse)

  setTimeout(async () => {
    while (true) {
      const { done, value } = await reader.read()
      if (done || !value) {
        end()
        break
      }
      write(value)
    }
  })
  return {
    stop: () => {
      // Stop producing parse results immediately
      reader.cancel()
    }
  }
}

/**
 * Splits transform stream in chunks of size maxChunkBytes or less
 * Each chunk is enqueued in a separate scope of synchronous execution
 * @param maxChunkBytes
 * @returns
 */
function splitStreamByMaxChunk(maxChunkBytes: number): TransformStream<Uint8Array, Uint8Array> {
  return new TransformStream<Uint8Array, Uint8Array>({
    async transform(chunk, controller) {
      let start = 0
      while (start < chunk.length) {
        // const newlineIndex = chunk.indexOf(10, start)
        const end = Math.min(chunk.length, start + maxChunkBytes)

        controller.enqueue(chunk.subarray(start, end))
        controller.desiredSize, controller.enqueue, controller.error, controller.terminate

        start = end
        // if (end !== chunk.length) {
        //   await new Promise(resolve => setTimeout(resolve))
        // }
      }
    }
  })
}

// function skipOldestWhenBackpressure(maxBufferSize: number): TransformStream<Uint8Array, Uint8Array> {

// }

const hasBackpressure = <T>(controller: TransformStreamDefaultController<T>) => {
  return controller.desiredSize !== null && controller.desiredSize < 0
}

class BackpressureTransformStream extends TransformStream<Uint8Array, Uint8Array> {
  private chunkBuffer: Array<Uint8Array> = []
  private bufferSize = 0
  private maxChunkBufferSize: number

  constructor({
    maxChunkBufferSize,
    onBytesSkipped
  }: {
    maxChunkBufferSize: number
    onBytesSkipped?: (byteCount: number) => void
  }) {
    super({
      transform: async (chunk: Uint8Array, controller) => {
        // const chunkSize = chunk.length; // Get size of the incoming chunk
        // console.log(`Received chunk: "${chunk}" with size ${chunk.length} bytes`);
        this.bufferSize += chunk.length

        let skippedByteCount = 0
        console.log('BackpressureTransformStream a', chunk.length)
        // Drop oldest chunks if buffer size exceeds the max limit
        while (this.bufferSize > this.maxChunkBufferSize) {
          const oldestChunk = this.chunkBuffer.shift() // Remove oldest chunk
          if (oldestChunk) {
            this.bufferSize -= oldestChunk.length
            skippedByteCount += oldestChunk.length
            // console.log(`Dropped chunk: "${oldestChunk.length}" to reduce buffer size`);
          }
          console.log('BackpressureTransformStream b', this.bufferSize)
        }
        if (skippedByteCount !== 0) {
          onBytesSkipped?.(skippedByteCount)
        }

        // Add the chunk to the buffer
        this.chunkBuffer.push(chunk)

        if (hasBackpressure(controller)) {
          // await new Promise(resolve => setTimeout(resolve));
          // this.chunkBuffer.shift()
          console.log('BackpressureTransformStream c', controller.desiredSize)
          return
        }
        console.log('BackpressureTransformStream d', chunk.length)

        // Enqueue the current chunk to downstream
        controller.enqueue(this.chunkBuffer.shift())
      },
      flush: async (controller) => {
        while (true) {
          const oldestChunk = this.chunkBuffer.shift()
          if (!oldestChunk) {
            break
          }
          controller.enqueue(oldestChunk)
          // await new Promise(resolve => setTimeout(resolve));
        }
      }
    })

    this.maxChunkBufferSize = maxChunkBufferSize // Initialize the maximum buffer size
  }
}

export const processUTF8StreamByLine = (
  stream: ReadableStream<Uint8Array>,
  parser: (chunks: Uint8Array[][]) => {
    start: () => Promise<void>
    stop: () => void
    end?: () => void
  },
  onStreamClosed?: (reason: any) => void
) => {
  const reader = stream.getReader()
  let chunksToParse = [[]] as Uint8Array[][]
  const { start, stop, end } = parser(chunksToParse)

  setTimeout(async () => {
    start()
    while (true) {
      const { done, value } = await reader.read()
      if (done || !value) {
        // stop()
        end?.()
        break
      }

      splitByNewline(
        (chunk) => chunksToParse.at(-1)!.push(chunk),
        () => chunksToParse.push([]),
        value
      )

      // Release thread to process UI
      await new Promise((resolve) => setTimeout(resolve))
    }
  })
  return {
    cancel: () => {
      reader.cancel()
      stop()
    }
  }
}

export const parseUTF8AsTextLines = (
  stream: ReadableStream<Uint8Array>,
  onValue: (value: string) => void,
  onDone?: () => void
) => {
  queueMicrotask(async () => {
    for await (let line of makeUTF8LineIterator(stream, onDone)) {
      onValue(line)
    }
  })
  return {
    cancel: () => stream.cancel()
  }
}

async function* makeUTF8LineIterator(stream: ReadableStream<Uint8Array>, onDone?: () => void) {
  const utf8Decoder = new TextDecoder('utf-8')
  let reader = stream.getReader()
  let readerDone = false
  let res = await reader.read()
  readerDone = res.done
  let chunk = res.value ? utf8Decoder.decode(res.value, { stream: true }) : ''

  let re = /\r\n|\n|\r/gm
  let startIndex = 0

  for (;;) {
    let result = re.exec(chunk)
    if (!result) {
      if (readerDone) {
        break
      }
      let remainder = chunk.slice(startIndex)
      let res2 = await reader.read()
      chunk = remainder + (res2.value ? utf8Decoder.decode(res2.value, { stream: true }) : '')
      readerDone = res2.done
      startIndex = re.lastIndex = 0
      continue
    }
    yield chunk.substring(startIndex, result.index)
    startIndex = re.lastIndex
  }
  if (startIndex < chunk.length) {
    // last line didn't end in a newline char
    yield chunk.slice(startIndex)
  }
  onDone?.()
}

/**
 * Split stream by newline character (LF, 0x0A), sending an empty chunk on each occurrence
 * Empty chunk is also sent when the upstream has ended
 */
function splitByNewline(
  onChunk: (chunk: Uint8Array) => void,
  onBatch: () => void,
  chunk: Uint8Array
) {
  let start = 0
  while (start < chunk.length) {
    const newlineIndex = chunk.indexOf(10, start)
    const end = newlineIndex === -1 ? chunk.length : newlineIndex

    onChunk(chunk.subarray(start, end))
    if (end !== chunk.length) {
      onBatch()
    }

    // Move start to after the newline character
    start = end + 1
  }
}

/**
 * Split stream by newline character (LF, 0x0A), sending an empty chunk on each occurrence
 * Empty chunk is also sent when the upstream has ended
 */
function splitStreamByNewline(): TransformStream<Uint8Array, Uint8Array> {
  return new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      splitByNewline(
        controller.enqueue.bind(controller),
        () => controller.enqueue(new Uint8Array()),
        chunk
      )
    },
    flush(controller) {
      controller.enqueue(new Uint8Array())
    }
  })
}

// ===================================

const mkTransformerParser = <T>(
  controller: TransformStreamDefaultController<T>,
  opts?: JSONParserOptions
) => {
  console.log('mkparser')
  const parser = new JSONParser(opts)
  parser.onValue = (value) => {
    console.log('JSONParserTransformer a', value)
    controller.enqueue(value.value as T)
  }
  parser.onError = (err) => controller.error(err)
  parser.onEnd = () => controller.terminate()
  return parser
}

class JSONParserTransformer<T> implements Transformer<Iterable<number> | string, T> {
  // @ts-ignore Controller always defined during start
  private controller: TransformStreamDefaultController<T>
  private parser: JSONParser
  private opts?: JSONParserOptions

  constructor(opts?: JSONParserOptions) {
    // @ts-ignore Property 'controller' is used before being assigned.
    this.parser = mkTransformerParser(this.controller, opts)
    this.opts = opts
  }

  start(controller: TransformStreamDefaultController<T>) {
    this.controller = controller
  }

  transform(chunk: Iterable<number> | string) {
    try {
      this.parser.write(chunk)
    } catch (e) {
      console.log('parse err', e)
      this.parser = mkTransformerParser(this.controller, this.opts)
    }
  }

  flush() {
    this.parser.end()
  }
}

class CustomJSONParserTransformStream extends TransformStream<
  Iterable<number> | string,
  ParsedElementInfo
> {
  constructor(
    opts?: JSONParserOptions,
    writableStrategy?: QueuingStrategy,
    readableStrategy?: QueuingStrategy
  ) {
    const transformer = new JSONParserTransformer(opts)
    super(transformer, writableStrategy, readableStrategy)
  }
}
