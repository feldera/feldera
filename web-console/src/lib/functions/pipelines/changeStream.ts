import BigNumber from 'bignumber.js'
import {
  JSONParser,
  Tokenizer,
  TokenParser,
  type JSONParserOptions,
  type TokenParserOptions
} from '@streamparser/json'

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
  const maxChunkSize = 100000
  const reader = stream
    .pipeThrough(
      splitStreamByMaxChunk(maxChunkSize, options?.bufferSize ?? 1000000, cbs.onBytesSkipped)
    )
    .pipeThrough(
      new CustomJSONParserTransformStream<T>(
        {
          ...options
        },
        {},
        {}
      )
    )
    .getReader()
  let resultBuffer = [] as T[]
  setTimeout(async () => {
    while (true) {
      const { done, value } = await reader.read()
      if (done || !value /*|| cancel*/) {
        break
      }
      resultBuffer.push(value)
    }
  })
  setTimeout(async () => {
    let closed = false
    reader.closed.then(() => {
      closed = true
    })
    while (true) {
      if (resultBuffer.length) {
        cbs.pushChanges?.(resultBuffer)
        resultBuffer.length = 0
      }
      if (closed) {
        break
      }
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
    cbs.onParseEnded?.()
  })
  return {
    cancel: () => {
      // cancel = true
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
    { highWaterMark: maxChunkBufferSize, size: (c) => c.length }
  )
}

const hasBackpressure = <T>(controller: TransformStreamDefaultController<T>, offset: number) => {
  return controller.desiredSize !== null && controller.desiredSize - offset < 0
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
      console.log('JSON parse error', e)
      this.parser = mkTransformerParser(this.controller, this.opts)
    }
  }

  flush() {
    this.parser.end()
  }
}

class CustomJSONParserTransformStream<T> extends TransformStream<Uint8Array | string, T> {
  constructor(
    opts?: JSONParserOptions,
    writableStrategy?: QueuingStrategy<Uint8Array | string>,
    readableStrategy?: QueuingStrategy<T>
  ) {
    const transformer = new JSONParserTransformer(opts)
    super(transformer, writableStrategy, readableStrategy)
  }
}
