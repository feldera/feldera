import BigNumber from 'bignumber.js'
import { JSONParser, Tokenizer, TokenParser, type TokenParserOptions } from '@streamparser/json'
import type { ParsedElementInfo } from '@streamparser/json/utils/types/parsedElementInfo.js'
import { discreteDerivative } from '$lib/functions/common/math'
import { tuple } from '$lib/functions/common/tuple'
import { chunkIndices } from '$lib/functions/common/array'
import { humanSize } from '$lib/functions/common/string'

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
 * @param stream
 * @param pushChanges
 * @param options.bufferSize Threshold size of the buffer that holds unprocessed JSON chunks.
 * If the buffer size exceeds this value - when the new JSON batch arrives previous JSON batches are dropped
 * until the buffer size is under the threshold, or only one batch remains.
 * @returns
 */
export const parseJSONInStream = <T>(
  stream: ReadableStream<Uint8Array>,
  pushChanges: (changes: T[]) => void,
  onBytesSkipped?: (bytes: number) => void,
  options?: TokenParserOptions & { bufferSize?: number }
) => {
  const reader = stream.getReader()
  let count = 0
  let resultBuffer = [] as any[]

  const onValue = ({ value }: ParsedElementInfo) => {
    resultBuffer[count] = value
    ++count
  }

  // chunksToParse is a list of batches (complete JSON objects), each split into a list of bytestring chunks
  // chunksToParse contains no empty bytestrings
  let chunksToParse = [[]] as Uint8Array[][]

  const startInterruptableParse = () => {
    let parser = mkParser(onValue, options)
    let isEnd = false
    let done = true

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

    return {
      start: async () => {
        while (!isEnd) {
          const value = chunksToParse[0]?.shift()
          if (!value) {
            if (chunksToParse.length > 1) {
              // Keep atleast a single empty batch in chunksToParse
              chunksToParse.shift()
              parser = mkParser(onValue, options)
            }
            await new Promise((resolve) => setTimeout(resolve))
            continue
          }

          // Parse JSON in subchunks of up to 200000 bytes to keep UI freezes to a minimum
          // because during parsing JavaScript cannot handle UI updates
          const positions = chunkIndices(0, value.length, 200000)
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
                console.log(`Skipped ${humanSize(previousBufferSize - bufferSize)} of change stream. New buffer size is ${humanSize(bufferSize)}`)
                onBytesSkipped?.(previousBufferSize - bufferSize)
                parser = mkParser(onValue, options)
                break
              }
            }

            count = 0
            done = false
            try {
              parser.write(value.slice(n0, n1))
            } catch (e) {
              console.log('JSON parse error', e)
              done = true
              break
            }

            if (parser.isParserEnded()) {
              parser = mkParser(onValue, options)
            }

            pushChanges(resultBuffer.slice(0, count))
            done = true
            await new Promise((resolve) => setTimeout(resolve))
          }
        }
      },
      stop() {
        isEnd = true
        interrupt()
      }
    }
  }

  const { start, stop } = startInterruptableParse()

  setTimeout(async () => {
    start()
    while (true) {
      const { done, value } = await reader.read()
      if (done || !value) {
        stop()
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
  return () => {
    reader.cancel()
    stop()
  }
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
