import BigNumber from 'bignumber.js'
import { JSONParser, Tokenizer, TokenParser, type TokenParserOptions } from '@streamparser/json'
import type { ParsedElementInfo } from '@streamparser/json/utils/types/parsedElementInfo.js'
import { discreteDerivative } from '$lib/functions/common/math'
import { tuple } from '$lib/functions/common/tuple'
import { chunkIndices } from '$lib/functions/common/array'

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

export const parseJSONInStream = <T>(
  stream: ReadableStream<Uint8Array>,
  pushChanges: (changes: T[]) => void,
  options?: TokenParserOptions
) => {
  const reader = stream.getReader()
  let count = 0
  let resultBuffer = new Array()

  const onValue = ({ value }: ParsedElementInfo) => {
    resultBuffer[count] = value
    ++count
  }

  const chunksToParse = [] as Uint8Array[]

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
      chunksToParse.length = 0
      while (!done) {
        // Release thread to parse JSON
        await new Promise((resolve) => setTimeout(resolve))
      }
    }

    return {
      start: async () => {
        while (!isEnd) {
          const value = chunksToParse.shift()
          if (!value || value.length === 0) {
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
              // In each iteration we check if an empty array has been added to the parse queue
              // If so, we drop all chunks before the latest empty array
              const emptyChunkIdx = chunksToParse.findLastIndex((chunk) => chunk.length === 0)
              if (emptyChunkIdx !== -1) {
                chunksToParse.splice(0, emptyChunkIdx + 1)
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
      splitByNewline(chunksToParse.push.bind(chunksToParse), value)
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
function splitByNewline(onChunk: (chunk: Uint8Array) => void, chunk: Uint8Array) {
  let start = 0

  while (start < chunk.length) {
    const newlineIndex = chunk.indexOf(10, start)
    const end = newlineIndex === -1 ? chunk.length : newlineIndex

    onChunk(chunk.subarray(start, end))
    if (end !== chunk.length) {
      onChunk(new Uint8Array())
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
      splitByNewline(controller.enqueue.bind(controller), chunk)
    },
    flush(controller) {
      controller.enqueue(new Uint8Array())
    }
  })
}
