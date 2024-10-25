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
  const flush = () => {
    if (resultBuffer.length) {
      cbs.pushChanges?.(resultBuffer)
      resultBuffer.length = 0
    }
  }
  setTimeout(async () => {
    let closed = false
    reader.closed.then(() => {
      closed = true
    })
    while (true) {
      flush()
      if (closed) {
        break
      }
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
    cbs.onParseEnded?.()
  })
  return {
    cancel: () => {
      flush()
      reader.cancel()
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
    { highWaterMark: maxChunkBufferSize, size: (c) => c.length }
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

export const parseUTF8AsTextLines = (
  stream: ReadableStream<Uint8Array>,
  cbs: {
    pushChanges: (changes: string[]) => void
    onBytesSkipped?: (bytes: number) => void
    onParseEnded?: () => void
  },
) => {
  const reader = stream
    .pipeThrough(
      new SplitNewlineTransformStream(undefined,
        undefined,//{ highWaterMark: 256 * 1024, size: (c) => c.length },
        { highWaterMark: 256 * 1024, size: (c) => c.length })
    )
    .getReader()
  let resultBuffer = [] as string[]
  setTimeout(async () => {
    while (true) {
      const { done, value } = await reader.read()
      if (done || !value /*|| cancel*/) {
        // console.log('done')
        break
      }
      // console.log('value', value)
      resultBuffer.push(value)
    }
  })
  const flush = () => {
    // console.log('flush', resultBuffer.length)
    if (resultBuffer.length) {
      cbs.pushChanges?.(resultBuffer)
      resultBuffer.length = 0
    }
  }
  setTimeout(async () => {
    let closed = false
    reader.closed.then(() => {
      // console.log('closed1')
      closed = true
    })
    while (true) {
      flush()
      if (closed) {
        // console.log('closed2')
        break
      }
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
    cbs.onParseEnded?.()
  })
  return {
    cancel: () => {
      // console.log('cancel')
      flush()
      reader.cancel()
    }
  }
}

class SplitNewlineTransformStream extends TransformStream<Uint8Array, string> {
  private decoder: TextDecoder;
  private buffer: string;
  private newlineRegex: RegExp;
  private onBytesSkipped: ((bytes: number) => void) | undefined;

  constructor(
      onBytesSkipped?: (bytes: number) => void,
      writableStrategy?: QueuingStrategy<Uint8Array>,
      readableStrategy?: QueuingStrategy<string>
    ) {
    super(
    {
      transform: (chunk, controller) => this.transform(chunk, controller),
      flush: (controller) => this.flush(controller),
    }
    , writableStrategy, readableStrategy);

    this.decoder = new TextDecoder('utf-8');
    this.buffer = '';
    this.newlineRegex = /\r?\n/g; // Matches both \n and \r\n
    this.onBytesSkipped = onBytesSkipped
  }

  private async transform(chunk: Uint8Array, controller: TransformStreamDefaultController<string>) {
    // Decode the chunk as a string and append it to the buffer
    this.buffer += this.decoder.decode(chunk, { stream: true });

    // Use RegExp.exec to find each newline
    let match;
    while ((match = this.newlineRegex.exec(this.buffer)) !== null) {
      if (hasBackpressure(controller, match.index)) {
        // console.log('backpressure', this.buffer.length - this.newlineRegex.lastIndex)
        this.onBytesSkipped?.(this.buffer.length - this.newlineRegex.lastIndex)
        this.buffer = this.buffer.slice(this.newlineRegex.lastIndex);
        break
      }
      // Extract the line from the start of the buffer up to the matched newline
      const line = this.buffer.slice(0, match.index);
      // console.log('match', this.buffer)
      controller.enqueue(line);

      // Update buffer by removing the processed line and newline
      this.buffer = this.buffer.slice(this.newlineRegex.lastIndex); // this.buffer.slice(match.index + match[0].length);
      // Reset lastIndex for the regex to handle the modified buffer
      this.newlineRegex.lastIndex = 0;
      await new Promise((resolve) => setTimeout(resolve))
    }
  }

  private flush(controller: TransformStreamDefaultController<string>) {
    // Enqueue any remaining buffered data
    if (this.buffer) {
      controller.enqueue(this.buffer);
      this.buffer = '';
    }
  }
}
